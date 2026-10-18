"""Bounded operator commands suitable for NiFi ExecuteProcess and manual recovery."""
import argparse
from datetime import datetime,timezone
import os
from pathlib import Path
import time

from ..cli import emit
from ..contracts import digest,now
from ..errors import PipelineError
from ..files import atomic_json,read_json,sha_file
from .queue import Queue


def configuration(path):
    c=read_json(path)
    if (c.get('contract_version')!=1 or c.get('mode') not in {'live','rehearse'} or not c.get('logical_target') or
        c['logical_target'].startswith('replace-with-') or not c.get('queue_root') or not c.get('audit_root')):
        raise PipelineError('OPERATIONS_CONFIG_REQUIRED')
    if c['mode']=='live' and c.get('single_owner_confirmed') is not True:raise PipelineError('OPERATIONS_SINGLE_OWNER_REQUIRED')
    if c.get('discover_remote') and (c['mode']!='live' or not c.get('pipeline_config') or
        not c.get('receive_root') or not c.get('ingest_state') or type(c.get('max_receive_bytes')) is not int or c['max_receive_bytes']<1):
        raise PipelineError('REMOTE_DISCOVERY_CONFIG_REQUIRED')
    return c


def tick(config_path):
    from .dispatch import handlers,discover_remote,discover_local_plans
    c=configuration(config_path);q=Queue(c['queue_root'],c['logical_target'])
    storage=None
    if c.get('storage_policy'):
        from .storage import report as storage_report
        policy=c['storage_policy'];storage=storage_report(policy['root'],policy['max_bytes'],policy['min_free_bytes'],policy.get('log_retention_days',30))
        if set(storage['alerts']) & {'STATE_SIZE_LIMIT','FREE_SPACE_LOW'}:raise PipelineError('OPERATIONS_STORAGE_BACKPRESSURE')
    for root in c.get('flow_directories',[]):
        from .flow import load
        plan=load(root)
        if plan['target']!=c['logical_target']:raise PipelineError('FLOW_QUEUE_TARGET_MISMATCH')
        flow_file=Path(root)/'flow.json'
        q.enqueue('FLOW',dict(state=str(Path(root).resolve()),flow=str(flow_file.resolve()),
            flow_sha256=sha_file(flow_file),flow_id=plan['content_hash']))
    discovered=discover_remote(c,q) if c.get('discover_remote') else 0
    for path in c.get('publication_ready_directories',[]):
        if (Path(path)/'ready.json').exists():discovered+=discover_local_plans(path,q,c['mysql_config'])
    if c.get('reconciliation_snapshots'):
        period=datetime.now(timezone.utc).strftime('%Y-%m-%d')
        for path in c['reconciliation_snapshots']:
            q.enqueue('RECONCILE',dict(snapshot=path,period=period,config=c['pipeline_config'],config_sha256=sha_file(c['pipeline_config'])))
    report=q.tick(handlers(c,q),limit=c.get('max_tasks_per_tick',10));report['discovered']=discovered
    report['queue_counts']=q.summary()
    if storage is not None:report['storage']=storage
    report['status']='ATTENTION_REQUIRED' if any(report['queue_counts'].get(k,0) for k in
        ('RUNNING','QUARANTINED','RETRY_EXHAUSTED')) else 'TICK_COMPLETED'
    atomic_json(Path(c['audit_root'])/'scheduler'/(now().replace(':','-')+'.json'),report)
    if report['recovered_succeeded'] and not report['quarantined'] and not report['retry_exhausted']:
        atomic_json(Path(c['audit_root'])/'manual-smoke.json',dict(status='MANUAL_TICK_PASSED',
                    config_sha256=sha_file(config_path),logical_target=c['logical_target'],recorded_at=now(),recovery_verified=True))
    return report


def main(argv=None):
    p=argparse.ArgumentParser(description='파이프라인 예약 실행·복구·이관')
    sub=p.add_subparsers(dest='action',required=True)
    for name in ('tick','enqueue','recover','retry'):
        cmd=sub.add_parser(name);cmd.add_argument('--config',type=Path,required=True)
        if name=='enqueue':
            cmd.add_argument('--kind',required=True,choices=['INGEST','PUBLISH','RECEIVE','RECONCILE','FLOW']);cmd.add_argument('--spec',type=Path,required=True)
            cmd.add_argument('--max-attempts',type=int,default=5)
        if name=='recover':
            cmd.add_argument('--task-id',required=True);cmd.add_argument('--expected-token',required=True);cmd.add_argument('--stop-evidence',required=True)
        if name=='retry':
            cmd.add_argument('--task-id',required=True);cmd.add_argument('--evidence',required=True);cmd.add_argument('--additional-attempts',type=int,default=1)
    flow=sub.add_parser('flow-prepare');flow.add_argument('--config',type=Path,required=True);flow.add_argument('--output',type=Path,required=True)
    flow=sub.add_parser('flow-run');flow.add_argument('--state',type=Path,required=True)
    flow=sub.add_parser('flow-audit');flow.add_argument('--state',type=Path,required=True)
    flow=sub.add_parser('flow-recover');flow.add_argument('--state',type=Path,required=True)
    flow.add_argument('--stage',required=True);flow.add_argument('--expected-token',required=True);flow.add_argument('--stop-evidence',required=True)
    storage=sub.add_parser('storage-report');storage.add_argument('--root',type=Path,required=True)
    storage.add_argument('--max-bytes',type=int,required=True);storage.add_argument('--min-free-bytes',type=int,required=True)
    storage.add_argument('--log-retention-days',type=int,default=30)
    for name in ('migration-pack','migration-restore','migration-verify'):
        cmd=sub.add_parser(name)
        if name=='migration-pack':cmd.add_argument('--roots',type=Path,required=True)
        else:cmd.add_argument('--bundle',type=Path,required=True)
        if name!='migration-verify':
            cmd.add_argument('--attestation',type=Path,required=True);cmd.add_argument('--output',type=Path,required=True)
    release=sub.add_parser('release');release.add_argument('--source',type=Path,required=True);release.add_argument('--output',type=Path,required=True)
    release_verify=sub.add_parser('release-verify');release_verify.add_argument('--bundle',type=Path,required=True)
    n=sub.add_parser('nifi')
    for name in ('url','group','version','python','working-directory','config'):n.add_argument('--'+name,required=True)
    n.add_argument('--ca-env',default='NIFI_CA_FILE');n.add_argument('--token-env',default='NIFI_TOKEN');n.add_argument('--apply',action='store_true')
    n.add_argument('--processor-id');n.add_argument('--start',action='store_true');n.add_argument('--stop',action='store_true')
    args=p.parse_args(argv);started=time.monotonic()
    try:
        if args.action=='tick':result=tick(args.config)
        elif args.action.startswith('flow-'):
            from .flow import prepare,run,recover,audit
            if args.action=='flow-prepare':result=prepare(args.config,args.output)
            elif args.action=='flow-run':result=run(args.state)
            elif args.action=='flow-audit':result=audit(args.state)
            else:result=recover(args.state,args.stage,args.expected_token,args.stop_evidence)
            result={k:v for k,v in result.items() if k!='outputs'}
        elif args.action=='storage-report':
            from .storage import report
            result=report(args.root,args.max_bytes,args.min_free_bytes,args.log_retention_days)
        elif args.action in {'enqueue','recover','retry'}:
            c=configuration(args.config);q=Queue(c['queue_root'],c['logical_target'])
            if args.action=='enqueue':row=q.enqueue(args.kind,read_json(args.spec),max_attempts=args.max_attempts)
            elif args.action=='recover':row=q.recover(args.task_id,args.expected_token,args.stop_evidence)
            else:row=q.retry_exhausted(args.task_id,args.evidence,args.additional_attempts)
            result=dict(status=row['status'],task_id=row['task_id'])
        elif args.action in {'release','release-verify'}:
            from .release import build,verify
            result=build(args.source,args.output) if args.action=='release' else verify(args.bundle)
        elif args.action.startswith('migration-'):
            from .migration import pack,restore,verify
            if args.action=='migration-pack':result=pack(read_json(args.roots),args.output,read_json(args.attestation))
            elif args.action=='migration-restore':result=restore(args.bundle,args.output,read_json(args.attestation))
            else:result=verify(args.bundle)
            result={k:v for k,v in result.items() if k!='files'}
        else:
            from .nifi import Client,processor_plan
            c=configuration(args.config)
            if args.apply and not args.stop:
                receipt_path=Path(c['audit_root'])/'manual-smoke.json'
                if not receipt_path.exists():raise PipelineError('NIFI_MANUAL_RECOVERY_SMOKE_REQUIRED')
                receipt=read_json(receipt_path)
                if (receipt.get('status')!='MANUAL_TICK_PASSED' or receipt.get('recovery_verified') is not True or
                    receipt.get('config_sha256')!=sha_file(args.config)):
                    raise PipelineError('NIFI_MANUAL_RECOVERY_SMOKE_REQUIRED')
            client=Client(args.url,ca_file=os.environ.get(args.ca_env),token=os.environ.get(args.token_env))
            spec=processor_plan(args.python,args.config,args.working_directory)
            if args.start or args.stop:
                if args.start==args.stop or not args.apply or not args.processor_id:raise PipelineError('NIFI_EXPLICIT_PROCESSOR_ACTION_REQUIRED')
                result=client.set_running(args.processor_id,spec,expected_version=args.version,running=args.start)
            else:
                result=client.prepare(args.group,spec,expected_version=args.version,apply=args.apply)
        emit({**result,'duration_ms':round((time.monotonic()-started)*1000)})
        return 2 if result.get('status')=='ATTENTION_REQUIRED' else 0
    except KeyboardInterrupt as exc:
        from .flow import WorkerUnconfirmed
        emit(dict(status='STOP_VERIFICATION_REQUIRED' if isinstance(exc,WorkerUnconfirmed) else 'INTERRUPTED',
                  duration_ms=round((time.monotonic()-started)*1000)))
        return 130
    except Exception as exc:
        failure=dict(status='FAILED',error_code=exc.code if isinstance(exc,PipelineError) else type(exc).__name__,
                     duration_ms=round((time.monotonic()-started)*1000))
        # Discovery/preflight may fail before a task exists. Persist those failures
        # too: NiFi may auto-terminate the command's stdout FlowFile.
        if hasattr(args,'config'):
            try:
                from uuid import uuid4
                failed_config=configuration(args.config)
                atomic_json(Path(failed_config['audit_root'])/'command_failures'/(str(uuid4())+'.json'),failure)
            except Exception:pass
        emit(failure);return 2


if __name__=='__main__':raise SystemExit(main())
