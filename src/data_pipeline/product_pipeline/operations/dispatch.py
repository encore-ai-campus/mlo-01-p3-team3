"""Allowlisted task handlers. No task-supplied shell or executable is accepted."""
from contextlib import ExitStack
from pathlib import Path
import tempfile

from ..config import Settings
from ..contracts import digest
from ..errors import Conflict,PipelineError
from ..files import atomic_json,read_json,rows,sha_file
from ..snapshot import verify
from .queue import RetryLater


def resolve_path(value,bindings):
    text=str(value).replace('\\','/')
    for old,new in sorted(bindings.items(),key=lambda r:len(r[0]),reverse=True):
        old=old.replace('\\','/').rstrip('/')
        if text==old or text.startswith(old+'/'):return Path(new)/text[len(old):].lstrip('/')
    return Path(value)


def reconcile(snapshot,ledger):
    """A fixed FULL/LIVE manifest versus majority Raw and ledger, no mutation."""
    from ..mongo import wire_document
    from bson import ObjectId
    manifest=verify(snapshot);result=dict(snapshot_id=manifest['snapshot_id'],observations=0,missing_registration=0,
        missing_raw=0,unconfirmed_events=0,conflicts=0)
    for row in rows(Path(snapshot)/'observations.jsonl'):
        result['observations']+=1
        try:saved=ledger.get(row['observation_key'])
        except PipelineError as exc:
            if exc.code!='REGISTRATION_UNCONFIRMED':raise
            result['missing_registration']+=1;continue
        raw=ledger.raw.find_one({'_id':ObjectId(saved['document']['_id'])})
        if raw is None:result['missing_raw']+=1
        elif wire_document(raw)!=saved['document']:result['conflicts']+=1
        if not saved.get('event_confirmed'):result['unconfirmed_events']+=1
    result['status']='CONFLICT' if result['conflicts'] else 'REPAIR_REQUIRED' if any(result[k] for k in
        ('missing_registration','missing_raw','unconfirmed_events')) else 'CONSISTENT'
    return result


def handlers(config,queue):
    bindings=config.get('path_bindings',{})
    def path(value):return resolve_path(value,bindings)
    def checked(spec,key):
        value=path(spec[key]);expected=spec.get(key+'_sha256')
        if expected is None or sha_file(value)!=expected:raise Conflict('SCHEDULED_INPUT_CHANGED')
        return value

    def ingest(spec):
        from ..runner import ingest as run
        snapshot=path(spec['snapshot']);manifest=verify(snapshot)
        if manifest['snapshot_id']!=spec['snapshot_id']:raise Conflict('SCHEDULED_SNAPSHOT_CHANGED')
        settings=Settings.load(checked(spec,'config')) if config['mode']=='live' else Settings('rehearsal','rehearsal','rehearsal-images')
        if settings.fingerprint!=spec['target_fingerprint']:raise Conflict('SCHEDULED_TARGET_CHANGED')
        state=path(spec['state']);marker=state/'scheduled-target.json'
        with ExitStack() as stack:
            if config['mode']=='rehearse':
                from ..local import FileLedger,FileObjects,FilePublisher
                ledger=FileLedger(state/'ledger',settings);objects=FileObjects(state/'objects');publisher=FilePublisher(state/'events')
            else:
                from ..bootstrap import open_live as _live
                ledger,objects,publisher=_live(settings,stack);objects.preflight();publisher.preflight()
            report=run(snapshot,settings,ledger,objects,publisher,state/'audit',repair_raw=spec.get('repair_raw',False))
        if report['failed'] or report['errors'] or report['raw_confirmed']!=manifest['product_count']:
            if any(e['error_code'] in {'RAW_DOCUMENT_CONFLICT','REGISTRATION_EVENT_CONFLICT','REGISTRATION_DOCUMENT_CONFLICT'} for e in report['errors']):
                raise Conflict('INGEST_INPUT_CONFLICT')
            raise RetryLater('INGEST_RETRY_REQUIRED')
        return dict(snapshot_id=manifest['snapshot_id'],ingestion_run_id=report['ingestion_run_id'],
                    status='INGEST_DRAINED',image_relations_unavailable=report['image_relations_unavailable'])

    def publish(spec):
        if config['mode']!='live':raise PipelineError('MYSQL_PUBLISH_REQUIRES_EXPLICIT_LIVE_MODE')
        from ..publication.mysql import MySQL
        from ..publication.plan import load_plan
        from ..publication.runner import publish as run
        plan=load_plan(checked(spec,'plan'))
        if plan['plan_id']!=spec['plan_id']:raise Conflict('SCHEDULED_PLAN_CHANGED')
        db=MySQL(read_json(checked(spec,'config')))
        try:return run(plan,db)
        finally:db.close()

    def receive(spec):
        from ..objects import S3Objects
        from ..bootstrap import required_env as _env
        from ..transfer import receive_snapshot,CHUNK_BYTES,MAX_READY_BYTES
        conf=checked(spec,'config');settings=Settings.load(conf)
        if not settings.handoff_bucket:raise PipelineError('HANDOFF_BUCKET_REQUIRED')
        endpoint=_env(settings.s3_endpoint_env)
        if endpoint!='aws' and not endpoint.startswith('https://'):raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
        objects=S3Objects.from_settings(settings,endpoint,bucket=settings.handoff_bucket)
        def fetch(key,limit=CHUNK_BYTES):
            obj=objects.client.get_object(Bucket=settings.handoff_bucket,Key=key)
            try:data=obj['Body'].read(limit+1)
            finally:obj['Body'].close()
            if len(data)>limit:raise PipelineError('TRANSPORT_OBJECT_TOO_LARGE')
            return data
        with tempfile.TemporaryDirectory(prefix='scheduled-ready-') as tmp:
            ready=Path(tmp)/'ready.json';ready.write_bytes(fetch(spec['ready_key'],MAX_READY_BYTES))
            if spec['ready_key']!='handoff/ready/'+read_json(ready).get('transport_id','')+'.ready.json':
                raise Conflict('SCHEDULED_READY_KEY_CHANGED')
            report=receive_snapshot(ready,fetch,path(spec['output']),max_total_bytes=config['max_receive_bytes'])
        snapshot=path(spec['output']);target=settings.fingerprint
        queue.enqueue('INGEST',dict(snapshot=str(snapshot),snapshot_id=verify(snapshot)['snapshot_id'],
            state=str(path(config['ingest_state'])),config=str(conf),config_sha256=sha_file(conf),target_fingerprint=target))
        return report

    def audit(spec):
        if config['mode']!='live':raise PipelineError('RECONCILE_LIVE_MONGO_REQUIRED')
        from ..mongo import MongoLedger
        from ..bootstrap import required_env as _env
        conf=checked(spec,'config');settings=Settings.load(conf)
        ledger=MongoLedger(settings,_env(settings.mongo_uri_env))
        try:report=reconcile(path(spec['snapshot']),ledger)
        finally:ledger.close()
        from datetime import date
        if date.fromisoformat(spec['period']).isoformat()!=spec['period']:raise PipelineError('RECONCILIATION_PERIOD_INVALID')
        atomic_json(path(config['audit_root'])/'reconciliation'/(queue.target+'-'+spec['period']+'-'+report['snapshot_id']+'.json'),report)
        if report['status']=='CONFLICT':raise Conflict('RECONCILIATION_RAW_CONFLICT')
        if report['status']=='REPAIR_REQUIRED':
            queue.enqueue('INGEST',dict(snapshot=spec['snapshot'],snapshot_id=report['snapshot_id'],
                state=config['ingest_state'],config=spec['config'],config_sha256=spec['config_sha256'],
                target_fingerprint=settings.fingerprint,repair_raw=True,reconciliation_period=spec['period']))
        return report
    def flow(spec):
        from .flow import load,run
        root=path(spec['state']);checked(spec,'flow')
        if root/'flow.json' != path(spec['flow']):raise Conflict('SCHEDULED_FLOW_PATH_CHANGED')
        plan=load(root)
        if plan['content_hash']!=spec['flow_id'] or plan['target']!=config['logical_target']:
            raise Conflict('SCHEDULED_FLOW_TARGET_CHANGED')
        if config['mode']!='live':raise PipelineError('FLOW_LIVE_CONFIG_REQUIRED')
        return run(root)
    return dict(INGEST=ingest,PUBLISH=publish,RECEIVE=receive,RECONCILE=audit,FLOW=flow)


def discover_local_plans(directory,queue,config_file):
    """Ready manifest is the authority; individual temp plans are ignored."""
    from ..publication.plan import load_plan
    ready=read_json(Path(directory)/'ready.json')
    if ready.get('stage')!='PUBLICATION_PLANS_READY' or ready.get('content_hash')!=digest({k:v for k,v in ready.items() if k!='content_hash'}):
        raise Conflict('PUBLICATION_READY_CHANGED')
    count=0
    from ..files import confined
    for name,checksum in ready['plans'].items():
        file=confined(directory,name);plan=load_plan(file)
        if digest(plan)!=checksum or plan['target']!=ready['target']:raise Conflict('PUBLICATION_READY_PLAN_CHANGED')
        queue.enqueue('PUBLISH',dict(plan=str(file),plan_sha256=sha_file(file),plan_id=plan['plan_id'],
                                    config=str(config_file),config_sha256=sha_file(config_file)))
        count+=1
    return count


def discover_remote(config,queue):
    from ..config import Settings
    from ..objects import S3Objects
    from ..bootstrap import required_env as _env
    from ..files import lock
    import re
    conf=Path(config['pipeline_config']);settings=Settings.load(conf)
    if not settings.handoff_bucket:raise PipelineError('HANDOFF_BUCKET_REQUIRED')
    endpoint=_env(settings.s3_endpoint_env)
    if endpoint!='aws' and not endpoint.startswith('https://'):raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
    objects=S3Objects.from_settings(settings,endpoint,bucket=settings.handoff_bucket)
    marker=queue.root/'discovery.json'
    with lock(queue.root/'discovery.lock'):
        state=read_json(marker) if marker.exists() else dict(target=queue.target,continuation=None)
        if state['target']!=queue.target:raise Conflict('DISCOVERY_TARGET_CHANGED')
        args=dict(Bucket=settings.handoff_bucket,Prefix='handoff/ready/',MaxKeys=100)
        if state.get('continuation'):args['ContinuationToken']=state['continuation']
        page=objects.client.list_objects_v2(**args);count=0
        for row in page.get('Contents',[]):
            key=row['Key']
            if not re.fullmatch(r'handoff/ready/[a-f0-9]{64}\.ready\.json',key):continue
            queue.enqueue('RECEIVE',dict(ready_key=key,output=str(Path(config['receive_root'])/Path(key).name[:-11]),
                config=str(conf),config_sha256=sha_file(conf)))
            count+=1
        # Advance only after every listed task has been durably registered.
        atomic_json(marker,dict(target=queue.target,continuation=page.get('NextContinuationToken')))
    return count
