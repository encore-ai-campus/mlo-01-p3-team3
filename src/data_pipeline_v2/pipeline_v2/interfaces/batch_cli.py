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
            'batch-backlog','batch-retract','batch-repair','batch-retention','batch-quarantine-read','metrics','spark-metrics'}
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


def main(argv=None):
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
    for name in ('batch-work','batch-status','batch-recover','batch-audit'):
        command = commands.add_parser(name)
        command.add_argument('--job',required=True)
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
    for name,command in commands.choices.items():
        if name not in READ_ONLY:
            command.add_argument('--apply',action='store_true')
    args = parser.parse_args(argv)
    try:
        config = configuration(args.config)
        if args.command == 'metrics':
            result = Timing(config['state_root']).summary(args.scope,persist=False)
        elif args.command == 'spark-metrics':
            from ..adapters.spark_metrics import report
            result = report(args.event_log,args.scope)
        else:
            writable = args.command in {'batch-work','batch-audit','batch-retention'}
            if config['mode'] == 'server' and args.command not in READ_ONLY and not args.apply:
                raise Invalid('SERVER_EXECUTION_REQUIRES_APPLY')
            event_scope = getattr(args,'batch',getattr(args,'job',args.command))
            with runtime(config,batch=event_scope,writable=writable,phase=getattr(args,'phase','all')) as (runner,outbox):
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
        print(json.dumps(result,ensure_ascii=False),flush=True)
        return 0
    except KeyboardInterrupt:
        print(json.dumps(dict(status='INTERRUPTED')),flush=True)
        return 130
    except Exception as exc:
        code = str(exc) if isinstance(exc,Invalid) else type(exc).__name__
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,127}',code):
            code = type(exc).__name__
        print(json.dumps(dict(status='FAILED',error_code=code)),flush=True)
        return 2
