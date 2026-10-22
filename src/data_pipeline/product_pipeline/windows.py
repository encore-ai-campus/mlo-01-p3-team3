"""Bounded immutable input windows; advance only after downstream publication."""
import re
import time
from pathlib import Path
from .contracts import digest, now
from .errors import PipelineError, Conflict
from .files import read_json, atomic_json


PERMANENT_ERRORS={'GOLD_EVALUATION_SELECTION_REQUIRED','GOLD_HISTORY_SUPPLEMENT_INCOMPLETE',
    'GOLD_HISTORY_BRONZE_SOURCE_MISSING_OR_CHANGED','GOLD_PRODUCT_HISTORY_LIMIT',
    'GOLD_REVIEW_SPOOL_LIMIT','GOLD_REVIEW_SPOOL_DISK_FULL','GOLD_CURRENT_SELECTION_NOT_CONFIRMED'}


def checked_stage(run,args,state_dir,stage):
    """Preserve a CLI's actual failure instead of retrying an opaque RuntimeError."""
    audits=Path(state_dir)/'runs';before=set(audits.glob('*.json'))
    code=run(args)
    if not code:return
    created=set(audits.glob('*.json'))-before
    failure=read_json(next(iter(created))) if len(created)==1 else {}
    # Spark cancellation may surface before the heartbeat's PipelineError.
    # Preserve the lease loss so the coordinator never retries a latched writer.
    code=failure.get('error_code') or stage.upper()+'_FAILED'
    diagnostic=failure.get('lease_failure') or {}
    if diagnostic.get('phase')=='renew' and code not in {'GOLD_SHUTDOWN_UNCONFIRMED','SILVER_SHUTDOWN_UNCONFIRMED'}:
        code='BRONZE_WRITER_LEASE_LOST'
    error=PipelineError(code)
    error.lease_failure=failure.get('lease_failure');error.lease_owner_ref=failure.get('lease_owner_ref');raise error


def record_limit(value='100'):
    try:
        if isinstance(value,bool):raise ValueError()
        size=int(value)
        if str(size)!=str(value) or not 1<=size<=100:raise ValueError()
        return size
    except (TypeError,ValueError):raise PipelineError('PIPELINE_WINDOW_RECORD_LIMIT') from None


def validate_window(value):
    try:
        if set(value)!={'contract_version','bronze_version','base_bronze_version','after','observation_keys','product_keys','content_hash'}:raise ValueError()
        if value['contract_version']!=1 or type(value['bronze_version']) is not int or value['bronze_version']<0:raise ValueError()
        if type(value['base_bronze_version']) is not int or not -1<=value['base_bronze_version']<=value['bronze_version']:raise ValueError()
        keys=value['observation_keys'];products=value['product_keys'];after=value['after']
        if after is not None and not re.fullmatch('[a-f0-9]{64}',after):raise ValueError()
        if not isinstance(keys,list) or not 1<=len(keys)<=100 or keys!=sorted(set(keys)):raise ValueError()
        if any(not re.fullmatch('[a-f0-9]{64}',k) or (after is not None and k<=after) for k in keys):raise ValueError()
        if not isinstance(products,list) or not 1<=len(products)<=len(keys) or products!=sorted(set(products)):raise ValueError()
        if any(not isinstance(p,str) or ':' not in p or len(p)>200 for p in products):raise ValueError()
        if value['content_hash']!=digest({k:v for k,v in value.items() if k!='content_hash'}):raise ValueError()
    except (ValueError,TypeError,KeyError):raise PipelineError('PIPELINE_WINDOW_INVALID') from None
    return value


def seal_window(version, after, docs, *, base_version=-1):
    value=dict(contract_version=1,bronze_version=version,base_bronze_version=base_version,after=after,
        observation_keys=sorted(d['observation_key'] for d in docs),
        product_keys=sorted({d['source_code']+':'+d['source_product_id'] for d in docs}))
    value['content_hash']=digest(value)
    return validate_window(value)


def run_windows(root, bronze_version, execute, *, progress=None, base_version=-1,pause_requested=None):
    """execute(stage, window_dir) returns a durable successful stage result.

    execute must freeze selection.json in plan, using scope.json's pinned Bronze
    version and exclusive lower bound. The cursor is never a substitute for the
    independent Silver/Gold/SQL commit receipts.
    """
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    path=root/'windows-progress.json'
    state=read_json(path) if path.exists() else dict(contract_version=1,bronze_version=bronze_version,base_bronze_version=base_version,window=0,after=None,status='RUNNING')
    if state.get('bronze_version')!=bronze_version or state.get('base_bronze_version')!=base_version:raise Conflict('PIPELINE_WINDOW_VERSION_CHANGED')
    if state['status']=='COMPLETED':return state
    while True:
        window_started=time.monotonic();reused_stages=[]
        cut=root/'windows'/('%06d'%state['window']);cut.mkdir(parents=True,exist_ok=True)
        scope=dict(bronze_version=bronze_version,base_bronze_version=base_version,after=state['after'])
        scope_path=cut/'scope.json'
        if scope_path.exists() and read_json(scope_path)!=scope:raise Conflict('PIPELINE_WINDOW_SCOPE_CHANGED')
        if not scope_path.exists():atomic_json(scope_path,scope)
        selection=None
        for stage in ('plan','silver','gold','publish','activation'):
            state.pop('error_code',None)
            state.update(status='RUNNING',stage=stage,execution_root=str(cut),updated_at=now())
            atomic_json(path,state)
            if progress:progress(state)
            result_path=cut/(stage+'-result.json')
            try:
                if result_path.exists():
                    reused_stages.append(stage);result=read_json(result_path)
                else:result=execute(stage,cut)
                if result.get('status')!='COMPLETED':raise PipelineError('PIPELINE_WINDOW_STAGE_FAILED')
                if not result_path.exists():atomic_json(result_path,result)
                if stage=='plan':
                    if result.get('drained'):
                        state.update(status='COMPLETED',updated_at=now());atomic_json(path,state)
                        if progress:progress(state)
                        return state
                    selection=validate_window(read_json(cut/'selection.json'))
                    if selection['bronze_version']!=bronze_version or selection['base_bronze_version']!=base_version or selection['after']!=state['after']:
                        raise Conflict('PIPELINE_WINDOW_SCOPE_CHANGED')
                elif result.get('window_hash',selection['content_hash'])!=selection['content_hash']:
                    raise Conflict('PIPELINE_WINDOW_RESULT_CHANGED')
            except BaseException as exc:
                state.update(status='INTERRUPTED',error_code=getattr(exc,'code',type(exc).__name__),lease_failure=getattr(exc,'lease_failure',None),lease_owner_ref=getattr(exc,'lease_owner_ref',None),updated_at=now());atomic_json(path,state)
                if progress:progress(state)
                raise
        durations={phase:read_json(cut/(phase+'-result.json')).get('duration_ms',0)
                   for phase in ('plan','silver','gold','publish','activation')}
        state.update(after=selection['observation_keys'][-1],window=state['window']+1,
                     last_published_at=now(),published_windows=state.get('published_windows',0)+1,
                     last_batch=dict(observations=len(selection['observation_keys']),products=len(selection['product_keys']),
                         stage_duration_ms=durations,recorded_stage_total_ms=sum(durations.values()),
                         reused_stages=reused_stages,duration_ms=round((time.monotonic()-window_started)*1000)))
        atomic_json(path,state)
        if pause_requested is not None and pause_requested(state):
            state.update(status='PAUSED_AFTER_BATCH',stage='paused',updated_at=now())
            atomic_json(path,state)
            if progress:progress(state)
            return state
