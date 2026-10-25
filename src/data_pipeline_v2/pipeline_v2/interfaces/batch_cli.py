"""Finite commands for NiFi and a separately launched worker; reports are explicitly requested."""
import argparse
import hashlib
import json
from pathlib import Path
import re
from ..batch_bootstrap import configuration, runtime
from ..adapters.files import read, write
from ..adapters.timing import Timing
from ..domain.contracts import Invalid

COMMANDS = {'batch-prepare','batch-relay','batch-submit','batch-work','batch-status','batch-recover','batch-audit',
            'batch-backlog','batch-retract','batch-repair','batch-retention','batch-quarantine-read','batch-serve','batch-abandon',
            'metrics','spark-metrics'}
READ_ONLY = {'metrics','spark-metrics','batch-status','batch-backlog'}


def items(path, byte_limit, expected_hash):
    path = Path(path)
    if path.stat().st_size > byte_limit:
        raise Invalid('CRAWL_FILE_BYTE_LIMIT')
    checksum = hashlib.sha256()
    if path.suffix == '.jsonl':
        with path.open('rb') as stream:
            for line in stream:
                checksum.update(line)
                if not line.strip():
                    raise Invalid('CRAWL_EMPTY_JSONL_RECORD')
                yield json.loads(line)
    else:
        body = path.read_bytes()
        checksum.update(body)
        value = json.loads(body)
        if not isinstance(value,list):
            raise Invalid('CRAWL_INPUT_ARRAY_REQUIRED')
        yield from value
    if checksum.hexdigest() != expected_hash:
        raise Invalid('CRAWL_FILE_CHANGED_DURING_READ')


def dispatch(args, runner, outbox):
    if args.command == 'batch-prepare':
        crawl = read(args.manifest)
        checksum = hashlib.sha256()
        with args.input.open('rb') as stream:
            while chunk := stream.read(1024*1024):
                checksum.update(chunk)
        if checksum.hexdigest() != crawl['input_sha256']:
            raise Invalid('CRAWL_FILE_HASH_CONFLICT')
        source = outbox.prepare(crawl,items(args.input,runner.limits.max_input_bytes,crawl['input_sha256']))
        result = {k:source[k] for k in ('crawl_batch_id','status','content_hash','event_count','event_set_hash','source_bytes')}
    elif args.command == 'batch-relay':
        delivery = outbox.relay(args.batch,args.generation)
        result = {k:delivery[k] for k in ('crawl_batch_id','status','generation','content_hash','event_count','event_set_hash','ranges')}
    elif args.command == 'batch-submit':
        result = runner.submit(args.batch,args.generation)
    elif args.command == 'batch-status':
        result = runner.status(args.job)
    elif args.command == 'batch-work':
        result = runner.work(args.job,args.phase)
    elif args.command == 'batch-recover':
        result = runner.recover(args.job,args.stop_evidence,args.generation)
    elif args.command == 'batch-abandon':
        result = runner.abandon(args.job,args.stop_evidence,args.reason)
    elif args.command == 'batch-backlog':
        result = runner.backlog(rebuild=args.rebuild)
    elif args.command == 'batch-retract':
        result = runner.retract(dict(product_key=args.product_key,raw_document_id=args.raw_document_id,reason=args.reason))
    elif args.command == 'batch-repair':
        result = runner.repair([v.strip() for v in args.products.split(',') if v.strip()])
    elif args.command == 'batch-retention':
        result = runner.retention_purge(args.as_of)
    elif args.command == 'batch-quarantine-read':
        if args.output.exists():
            raise Invalid('QUARANTINE_OUTPUT_EXISTS')
        record = runner.quarantine_read(args.event_id,args.purpose,args.operator)
        # Restricted payload goes only to the operator's file, never to stdout or logs.
        write(args.output,record)
        result = dict(status='QUARANTINE_RECORD_WRITTEN',event_id=args.event_id,reason=record['reason'])
    else:
        result = runner.audit(args.job)
    return result


def error_code(exc):
    code = str(exc) if isinstance(exc,Invalid) else type(exc).__name__
    return code if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,127}',code) else type(exc).__name__


def serve(args, parser, runner, outbox):
    """Resident worker: one Spark session, ledger, Kafka producer and writer lease for many requests.

    Each separate command paid Python start, Mongo connect and (for Spark work) session and
    executor start, 10-23 s, before doing anything. A request is `<spool>/inbox/<id>.json` =
    {"argv": [command, args...]} with this CLI's arguments minus --config. Requests run one at a
    time in name order; the answer goes to `<spool>/outbox/<id>.json`, then the request is
    removed. `<spool>/stop` ends the loop after the current request.
    """
    import time
    inbox, answers = args.spool/'inbox', args.spool/'outbox'
    inbox.mkdir(parents=True,exist_ok=True)
    answers.mkdir(parents=True,exist_ok=True)
    # Open Spark (session, executors, writer lease) before the first batch arrives, not inside it.
    warm = getattr(runner.processor,'warm',None) or getattr(runner.processor,'_open',None)
    if warm:
        warm()
    # History compaction runs in the background only while the queue is idle (at start and after
    # processed batches). A request that arrives meanwhile cancels it, so no batch waits for it.
    import threading
    compact = getattr(runner.processor,'compact',None)
    cancel = getattr(runner.processor,'cancel_maintenance',None)
    state = {}

    def maintain():
        try:
            compact()
            state['outcome'] = 'DONE'
        except Exception as exc:
            state['outcome'] = 'CANCELLED' if state.get('cancelled') else 'FAILED'
            if not state.get('cancelled'):
                write(args.spool/'compaction-failed.json',dict(error_code=error_code(exc)))
        finally:
            runner.timing.flush()
    dirty, last, served, worker = compact is not None, time.monotonic() - args.compact_idle_seconds, 0, None
    while not (args.spool/'stop').exists():
        pending = sorted(inbox.glob('*.json'))
        if worker is not None and not worker.is_alive():
            # A cancelled pass is tried again on the next idle period; a failed one waits for new work.
            dirty, worker = dirty or state.get('outcome') == 'CANCELLED', None
        if not pending:
            if worker is None and dirty and time.monotonic() - last >= args.compact_idle_seconds:
                dirty, state = False, {}
                worker = threading.Thread(target=maintain,daemon=True)
                worker.start()
            time.sleep(args.poll_seconds)
            continue
        if worker is not None:
            state['cancelled'] = True
            cancel()
            worker.join()
            dirty, worker = dirty or state.get('outcome') != 'DONE', None
        request, started = pending[0], time.monotonic()
        try:
            argv = read(request)['argv']
            try:
                inner = parser.parse_args(['--config',str(args.config)] + [str(v) for v in argv])
            except SystemExit:
                # A malformed request fails alone; it must not stop the worker or block the queue.
                raise Invalid('SERVE_REQUEST_INVALID') from None
            if inner.command in {'batch-serve','metrics','spark-metrics'}:
                raise Invalid('SERVE_COMMAND_UNSUPPORTED')
            result = dispatch(inner,runner,outbox)
            dirty = dirty or (compact is not None and inner.command in {'batch-work','batch-repair'})
        except Exception as exc:
            result = dict(status='FAILED',error_code=error_code(exc))
        finally:
            runner.timing.flush()
            last = time.monotonic()
        write(answers/request.name,dict(request=request.stem,wall_ms=round((time.monotonic()-started)*1000,1),result=result))
        request.unlink()
        served += 1
    if worker is not None:
        state['cancelled'] = True
        cancel()
        worker.join()
    return dict(status='STOPPED',served=served)


def build_parser():
    parser = argparse.ArgumentParser(description='NiFi 완료 크롤 → Kafka 고정 범위 → Spark 유한 배치')
    parser.add_argument('--config',type=Path,required=True)
    commands = parser.add_subparsers(dest='command',required=True)
    prepare = commands.add_parser('batch-prepare')
    prepare.add_argument('--manifest',type=Path,required=True)
    prepare.add_argument('--input',type=Path,required=True)
    for name in ('batch-relay','batch-submit'):
        command = commands.add_parser(name)
        command.add_argument('--batch',required=True)
        command.add_argument('--generation',type=int,default=0)
    for name in ('batch-work','batch-status','batch-recover','batch-audit','batch-abandon'):
        command = commands.add_parser(name)
        command.add_argument('--job',required=True)
        if name == 'batch-abandon':
            command.add_argument('--stop-evidence',required=True)
            command.add_argument('--reason',required=True)
        if name == 'batch-recover':
            command.add_argument('--stop-evidence',required=True)
            command.add_argument('--generation',type=int)
        if name == 'batch-work':
            # process and publish may run as separate NiFi-launched processes to overlap batches.
            command.add_argument('--phase',choices=['all','process','publish'],default='all')
    backlog = commands.add_parser('batch-backlog')
    backlog.add_argument('--rebuild',action='store_true')
    retract = commands.add_parser('batch-retract')
    retract.add_argument('--product-key',required=True)
    retract.add_argument('--raw-document-id',required=True)
    retract.add_argument('--reason',required=True)
    repair = commands.add_parser('batch-repair')
    repair.add_argument('--products',required=True,help='쉼표로 구분한 product_key 목록')
    retention = commands.add_parser('batch-retention')
    retention.add_argument('--as-of')
    quarantine = commands.add_parser('batch-quarantine-read')
    quarantine.add_argument('--event-id',required=True)
    quarantine.add_argument('--purpose',required=True)
    quarantine.add_argument('--operator',required=True)
    quarantine.add_argument('--output',type=Path,required=True)
    metrics = commands.add_parser('metrics')
    metrics.add_argument('--scope',required=True)
    spark_metrics = commands.add_parser('spark-metrics')
    spark_metrics.add_argument('--event-log',type=Path,required=True)
    spark_metrics.add_argument('--scope',required=True)
    serve = commands.add_parser('batch-serve',help='상주 처리기: Spark 세션·원장·Kafka 연결을 유지하고 spool 요청을 차례로 실행')
    serve.add_argument('--spool',type=Path,required=True)
    serve.add_argument('--poll-seconds',type=float,default=0.2)
    serve.add_argument('--compact-idle-seconds',type=float,default=10.0)
    for name,command in commands.choices.items():
        if name not in READ_ONLY:
            command.add_argument('--apply',action='store_true')
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = configuration(args.config)
        if args.command == 'metrics':
            result = Timing(config['state_root']).summary(args.scope,persist=False)
        elif args.command == 'spark-metrics':
            from ..adapters.spark_metrics import report
            result = report(args.event_log,args.scope)
        else:
            writable = args.command in {'batch-work','batch-audit','batch-retention','batch-serve','batch-abandon'}
            if config['mode'] == 'server' and args.command not in READ_ONLY and not args.apply:
                raise Invalid('SERVER_EXECUTION_REQUIRES_APPLY')
            event_scope = getattr(args,'batch',getattr(args,'job',args.command))
            with runtime(config,batch=event_scope,writable=writable,phase=getattr(args,'phase','all')) as (runner,outbox):
                if args.command == 'batch-serve':
                    result = serve(args,parser,runner,outbox)
                else:
                    result = dispatch(args,runner,outbox)
        print(json.dumps(result,ensure_ascii=False),flush=True)
        return 0
    except KeyboardInterrupt:
        print(json.dumps(dict(status='INTERRUPTED')),flush=True)
        return 130
    except Exception as exc:
        print(json.dumps(dict(status='FAILED',error_code=error_code(exc))),flush=True)
        return 2
