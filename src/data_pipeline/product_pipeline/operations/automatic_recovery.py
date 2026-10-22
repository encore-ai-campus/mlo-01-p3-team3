"""Bounded recovery of a proven dead writer's transient Mongo renewal failure."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import os,sys
from ..contracts import now,digest
from ..files import read_json,atomic_json
from ..errors import PipelineError,Conflict
from .. import lease_recovery as recovery

NETWORK_ERRORS={'_OperationCancelled','AutoReconnect','ConnectionFailure','NetworkTimeout',
                'NotPrimaryError','ServerSelectionTimeoutError'}


def eligible(flow):
    failure=flow.get('last_failure',{});diagnostic=failure.get('lease_failure') or {}
    transient=(diagnostic.get('exception_type') in NETWORK_ERRORS or
        (diagnostic.get('exception_type')=='WTimeoutError' and diagnostic.get('server_code')==64))
    exited=(flow.get('error_code')=='SPARK_DRIVER_JVM_EXITED' and diagnostic.get('phase')=='jvm'
        and diagnostic.get('reason')=='PROCESS_EXITED' and diagnostic.get('exception_type')=='JVMProcessExited'
        and type(diagnostic.get('exit_code')) is int and diagnostic['exit_code']!=0)
    return flow.get('status')=='RECOVERY_REQUIRED' and (exited or (flow.get('error_code')=='BRONZE_WRITER_LEASE_LOST'
        and diagnostic.get('phase')=='renew' and diagnostic.get('reason')=='DATABASE_OPERATION_FAILED'
        and transient))


def checked_owner(root,ref):
    if not isinstance(ref,str):raise PipelineError('AUTO_RECOVERY_OWNER_EVIDENCE_REQUIRED')
    path=Path(ref)
    if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():raise PipelineError('AUTO_RECOVERY_OWNER_PATH_INVALID')
    if path.stat().st_size>16384:raise PipelineError('AUTO_RECOVERY_OWNER_EVIDENCE_INVALID')
    if os.name!='nt' and (path.stat().st_mode&0o077 or path.stat().st_uid!=os.getuid()):
        raise PipelineError('AUTO_RECOVERY_PRIVATE_OWNER_REQUIRED')
    owner=read_json(path)
    if owner.get('contract_version')!=1 or not owner.get('token') or not owner.get('claimed'):
        raise PipelineError('AUTO_RECOVERY_OWNER_EVIDENCE_INVALID')
    return owner


def check_plan_owner(plan,owner):
    if plan['target']!=owner['target']:raise Conflict('AUTO_RECOVERY_TARGET_CHANGED')
    rows=plan['claims']
    if rows and ({d['token'] for d in rows}!={owner['token']} or {d['_id'] for d in rows}!=set(owner['claimed'])):
        raise Conflict('AUTO_RECOVERY_OWNER_CHANGED')


def assert_owner_dead(owner,proc=Path('/proc')):
    pid=owner.get('pid')
    if type(pid) is not int or pid<=0:raise PipelineError('AUTO_RECOVERY_OWNER_PID_REQUIRED')
    path=proc/str(pid)
    if not path.exists():return
    stat=path/'stat'
    if owner.get('process_start') and stat.exists():
        if stat.read_text().split(') ',1)[1].split()[19]!=owner['process_start']:return
    raise PipelineError('AUTO_RECOVERY_OWNER_STILL_ALIVE')


def quota(state,anchor,limit,at):
    if state.get('anchor')!=anchor:state=dict(anchor=anchor,attempts=0)
    if state.get('attempts',0)>=limit:raise PipelineError('AUTO_RECOVERY_RETRY_EXHAUSTED')
    if state.get('next_allowed_at') and at<state['next_allowed_at']:raise PipelineError('AUTO_RECOVERY_COOLDOWN')
    return state


def recover(root,live):
    """Caller owns controller.lock; release uses an exact-owner DB transaction.

    A recovered permit stays tied to the original failure. It does not rewrite
    task/checkpoint status. launch() resumes through the ordinary fenced writer.
    """
    root=Path(root);policy_path=root/'auto-recovery-policy.json'
    if not policy_path.exists():raise PipelineError('AUTO_RECOVERY_NOT_ENABLED')
    policy=read_json(policy_path)
    if policy!={'contract_version':1,'enabled':True,'max_attempts':3,'cooldown_seconds':60}:
        raise PipelineError('AUTO_RECOVERY_POLICY_INVALID')
    flow=read_json(root/'flow-progress.json')
    if not eligible(flow):raise PipelineError('AUTO_RECOVERY_FAILURE_NOT_ELIGIBLE')
    failure=flow['last_failure'];owner=checked_owner(root,failure.get('lease_owner_ref'));assert_owner_dead(owner)
    incident=digest({k:flow.get(k) for k in ('epoch','recovery_stage','last_failure')})
    state_dir=root/'auto-recovery';state_dir.mkdir(exist_ok=True,mode=0o700)
    state_path=state_dir/'state.json';state=read_json(state_path) if state_path.exists() else {}
    state=quota(state,flow.get('last_mysql_commit_at'),policy['max_attempts'],now())
    cut=Path(failure.get('execution_root',str(root/'epochs'/('%06d'%flow['epoch']))))
    if not cut.resolve().is_relative_to(root.resolve()):raise PipelineError('AUTO_RECOVERY_CONFIG_PATH_INVALID')
    stage=failure.get('stage',flow.get('recovery_stage'))
    parent=Path(failure['lease_owner_ref']).resolve().parent.parent
    if parent.name in {'silver-state','gold-state'}:
        stage=parent.name.split('-')[0];cut=parent.parent
    if stage not in {'bronze','silver','gold'}:raise PipelineError('AUTO_RECOVERY_STAGE_NOT_SUPPORTED')
    config=cut/(stage+'.json')
    # Fixed deployment helper performs the same proxy conversion as each CLI.
    sys.path.insert(0,str(root))
    import production_work
    from recover_writer_lease import settings_for
    from ..bronze.lease import MongoLease
    production_work.setup();settings=settings_for(stage,config)
    lease=MongoLease(settings,os.environ[settings.lease_uri_env])
    def audit(value):
        path=state_dir/(incident+'-'+value['status'].lower()+'.json')
        atomic_json(path,value);path.chmod(0o600)
    try:
        plan=recovery.inspect(lease,live);check_plan_owner(plan,owner)
        # Preserve the original plan across an ambiguous release response.
        saved=state_dir/(incident+'-plan.json')
        if saved.exists():
            original=read_json(saved);check_plan_owner(original,owner)
            if plan['claims'] and plan['claims']!=original['claims']:raise Conflict('AUTO_RECOVERY_OWNER_CHANGED')
            plan=original
        else:atomic_json(saved,plan);saved.chmod(0o600)
        state.update(attempts=state.get('attempts',0)+1,incident=incident,
            next_allowed_at=(datetime.now(timezone.utc)+timedelta(seconds=60)).isoformat(timespec='milliseconds').replace('+00:00','Z'))
        atomic_json(state_path,state);state_path.chmod(0o600)
        result=recovery.apply(lease,plan,live,audit)
        if result.get('new_claims_present'):raise Conflict('AUTO_RECOVERY_NEW_OWNER_PRESENT')
        permit=dict(status='RECOVERED',incident=incident,released=result['released'],at=now(),attempt=state['attempts'])
        audit(permit);return permit
    finally:lease.close(release=False)
