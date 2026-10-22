"""Fixed server flow launch shared by manual control and the NiFi trigger.

The trigger never reclaims a lease or changes a failed task to READY. Only a
fresh installation, or an explicit READY_TO_START operator decision, may start.
"""
import json
import os
import subprocess
import time
from pathlib import Path
from ..contracts import now
from ..files import atomic_json, lock, read_json
from ..errors import PipelineError

SCRIPTS=('production_work.py','continuous_work.py','production_window_work.py',
         'production_downstream_work.py','repair_current_batch.py','recover_offer_observations.py')


def worker(root, *, proc=Path('/proc')):
    pidfile=root/'worker.pid'
    if not pidfile.exists():return None
    pid=int(pidfile.read_text().strip())
    path=proc/str(pid)/'cmdline'
    if not path.exists():return None
    args=path.read_bytes().split(b'\0')
    owned={str(root/name).encode() for name in SCRIPTS}
    if any(a in owned for a in args):return pid
    if path.read_bytes():raise PipelineError('PIPELINE_PID_IDENTITY_CHANGED')
    return None


def decision(root, *, pid=None):
    if pid is not None:return 'PAUSE_REQUESTED' if (root/'pause-after-window.json').exists() else 'ALREADY_RUNNING'
    if (root/'pause-after-window.json').exists():return 'PAUSED'
    stopped=root/'manual-stop.json';attempt=root/'flow-attempt.json'
    if stopped.exists():
        # A subsequent explicit manual start supersedes historical stop evidence.
        from datetime import datetime
        try:newer=attempt.exists() and datetime.fromisoformat(read_json(attempt)['started_at'].replace('Z','+00:00')).timestamp()>stopped.stat().st_mtime
        except (KeyError,ValueError):newer=False
        if not newer:return 'PAUSED'
    p=root/'flow-progress.json'
    if not p.exists():return 'START'
    status=read_json(p).get('status')
    if status=='COMPLETED':return 'COMPLETED'
    if status=='READY_TO_START':return 'START'
    return 'RECOVERY_REQUIRED'


def launch(root,base,python,stage,*,uid=None,gid=None,automatic=False,popen=subprocess.Popen):
    root=Path(root);base=Path(base)
    with lock(root/'controller.lock'):
        pid=worker(root)
        recovered=None
        if automatic:
            state=decision(root,pid=pid)
            from ..recovery_probe import probe
            from ..lease_recovery import assert_quiet
            def live():return probe(master_url='http://192.168.0.121:8080/json/',
                ca='/etc/pj3-secrets/spark/ca.pem',client_cert='/etc/pj3-secrets/spark/cert.pem',
                client_key='/etc/pj3-secrets/spark/key.pem',
                worker_hosts=['192.168.0.122','192.168.0.123','192.168.0.124'])
            if state=='RECOVERY_REQUIRED':
                from .automatic_recovery import recover
                recovered=recover(root,live);state='START'
            if state!='START':return dict(status=state,pid=pid,started=False,at=now())
            assert_quiet(live())
        elif pid is not None:raise PipelineError('PRODUCTION_WORKER_ALREADY_RUNNING')
        from ..runtime_overrides import load
        records,runtime_path,runtime=load(root,base)
        env=dict(PATH='/usr/bin:/bin',HOME=str(root),JAVA_HOME='/usr/lib/jvm/java-21-openjdk-amd64',
            PIPELINE_DELTA_KEY_INDEX_DIR=str(root/'key-index-cache'),
            SPARK_CONF_DIR='/etc/pj3/spark',SPARK_LOCAL_IP=runtime['driver_host'],PYTHONPATH=str(root/'source'),
            PYSPARK_PYTHON=runtime['executor_python'],PYSPARK_DRIVER_PYTHON=python,LANG='C.UTF-8',
            AWS_CA_BUNDLE='/etc/pj3-clients/s3/ca.pem',AWS_DEFAULT_REGION='us-east-1',
            PIPELINE_S3_ENDPOINT='https://192.168.0.141:8443',PIPELINE_S3_CREDENTIALS_FILE='/etc/pj3-clients/s3/spark.properties',
            PIPELINE_SPARK_S3_PROFILE='garage_v241',PIPELINE_SPARK_S3_CREDENTIALS_FILE='/etc/pj3-clients/s3/spark.properties',
            PIPELINE_SPARK_S3_BUCKET='pj3-data',
            SPARK_SUBMIT_OPTS='-Djavax.net.ssl.trustStore=/etc/pj3-clients/s3/ca-truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit',
            PIPELINE_WINDOW_RECORDS=str(records),PIPELINE_SPARK_RUNTIME_FILE=runtime_path,PIPELINE_SHARED_BRONZE_SESSION='1')
        if stage=='downstream':env.update(PIPELINE_RESUME_FLOW_AFTER_DOWNSTREAM='1',PIPELINE_WINDOWED_FLOW='1')
        command=[python,str(root/('continuous_work.py' if stage=='flow' else 'production_downstream_work.py'))] if stage in {'flow','downstream'} else [python,str(root/'production_work.py'),stage]
        if automatic and stage!='flow':raise PipelineError('NIFI_FLOW_ONLY')
        atomic_json(root/(stage+'-attempt.json'),dict(started_at=now()))
        if uid is not None:os.chown(root/(stage+'-attempt.json'),uid,gid)
        kwargs={}
        if uid is not None:kwargs=dict(user=uid,group=gid,extra_groups=[])
        path=root/(stage+'.private.log')
        with path.open('ab') as log:
            path.chmod(0o600)
            if uid is not None:os.chown(path,uid,gid)
            p=popen(command,cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,**kwargs)
        (root/'worker.pid').write_text(str(p.pid))
        if uid is not None:os.chown(root/'worker.pid',uid,gid)
        return dict(status='STARTED',pid=p.pid,started=True,at=now(),window_records=records,recovery=recovered)


def tick(root,base,python):
    root=Path(root)
    started=time.monotonic()
    config=read_json(root/'nifi-control.json')
    if config!={'contract_version':1,'enabled':True,'root':str(root)}:
        raise PipelineError('NIFI_CONTROL_NOT_ENABLED')
    try:result=launch(root,base,python,'flow',automatic=True)
    except PipelineError as exc:result=dict(status='BUSY' if exc.code=='WRITER_ACTIVE' else 'RECOVERY_REQUIRED',error_code=exc.code,started=False,at=now())
    result['duration_ms']=round((time.monotonic()-started)*1000)
    atomic_json(root/'nifi-last-tick.json',result)
    audit=root/'nifi-tick-audit.jsonl'
    with audit.open('a',encoding='utf-8') as out:
        audit.chmod(0o600);out.write(json.dumps(result,sort_keys=True)+'\n');out.flush();os.fsync(out.fileno())
    return result
