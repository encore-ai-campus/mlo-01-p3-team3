"""Operational tick composition shared by CLI and the server coordinator."""
from datetime import datetime,timezone
from pathlib import Path
from ..contracts import now
from ..errors import PipelineError
from ..files import atomic_json,read_json,sha_file
from .queue import Queue


def configuration(path):
    c=read_json(path)
    if (c.get('contract_version')!=1 or c.get('mode') not in {'live','rehearse'} or not c.get('logical_target') or
        c['logical_target'].startswith('replace-with-') or not c.get('queue_root') or not c.get('audit_root')):
        raise PipelineError('OPERATIONS_CONFIG_REQUIRED')
    if c['mode']=='live' and c.get('single_owner_confirmed') is not True:raise PipelineError('OPERATIONS_SINGLE_OWNER_REQUIRED')
    if c.get('incremental'):
        value=c['incremental']
        if (not isinstance(value,dict) or any(not isinstance(value.get(k),str) or not value[k] or value[k].startswith('replace-with-')
            for k in ('production_root','production_sha256','mongo_database','mongo_replica_set','state_root'))
                or len(value['production_sha256'])!=64):raise PipelineError('INCREMENTAL_CONFIG_REQUIRED')
    if c.get('discover_remote') and (c['mode']!='live' or not c.get('pipeline_config') or
        not c.get('receive_root') or not c.get('ingest_state') or type(c.get('max_receive_bytes')) is not int or c['max_receive_bytes']<1):
        raise PipelineError('REMOTE_DISCOVERY_CONFIG_REQUIRED')
    return c


def tick(config_path):
    from contextlib import ExitStack
    from ..files import lock
    c=configuration(config_path)
    if not c.get('incremental'):return _tick(config_path)
    from ..bootstrap import open_incremental
    if c['mode']!='live':raise PipelineError('INCREMENTAL_LIVE_CONFIG_REQUIRED')
    with lock(Path(c['incremental']['state_root'])/'discovery.lock'),ExitStack() as stack:
        return _tick(config_path,open_incremental(c,stack))


def _tick(config_path,incremental_runtime=None):
    from .dispatch import handlers,discover_remote,discover_local_plans
    c=configuration(config_path);q=Queue(c['queue_root'],c['logical_target'])
    incremental=None
    if incremental_runtime is not None:
        from ..incremental.service import discover
        state,source,backend=incremental_runtime
        incremental=discover(source,state,q)
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
    report=q.tick(handlers(c,q,incremental=incremental_runtime),limit=c.get('max_tasks_per_tick',10));report['discovered']=discovered
    if incremental is not None:report['incremental']=incremental
    report['queue_counts']=q.summary()
    if storage is not None:report['storage']=storage
    report['status']='ATTENTION_REQUIRED' if any(report['queue_counts'].get(k,0) for k in
        ('RUNNING','QUARANTINED','RETRY_EXHAUSTED')) else 'TICK_COMPLETED'
    atomic_json(Path(c['audit_root'])/'scheduler'/(now().replace(':','-')+'.json'),report)
    if report['recovered_succeeded'] and not report['quarantined'] and not report['retry_exhausted']:
        atomic_json(Path(c['audit_root'])/'manual-smoke.json',dict(status='MANUAL_TICK_PASSED',
                    config_sha256=sha_file(config_path),logical_target=c['logical_target'],recorded_at=now(),recovery_verified=True))
    return report

