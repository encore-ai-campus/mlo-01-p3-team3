from ansible.module_utils.basic import AnsibleModule
import hashlib,json,os,pwd,shutil,subprocess,tarfile
from pathlib import Path
BASE=Path('/var/lib/pj3/product-pipeline/spark')
ROOT=BASE/'production-pending-20261019'
STAGE=Path('/home/team3ops/crawler-connect-0685b031-20261019')
PY=str(BASE/'venvs/128dbd37d879b397-spark/bin/python')
def main():
    m=AnsibleModule(argument_spec=dict(action=dict(default='status',choices=['deploy','start','status','preflight','refresh','stop','prepare-gold']),stage=dict(default='bronze',choices=['preflight','bronze','silver','gold','publish','flow','downstream']),source_sha256=dict(default='')))
    u=pwd.getpwnam('pj3-spark');action=m.params['action'];stage=m.params['stage']
    ROOT.mkdir(exist_ok=True,mode=0o700);os.chown(ROOT,u.pw_uid,u.pw_gid)
    pidfile=ROOT/'worker.pid';alive=False
    if pidfile.exists():
        pid=int(pidfile.read_text());proc=Path(f'/proc/{pid}/cmdline');alive=proc.exists() and any(s in proc.read_bytes() for s in [b'production_work.py',b'continuous_work.py',b'production_downstream_work.py'])
        if alive and action=='status':
            args=proc.read_bytes().split(b'\0')
            stage='downstream' if b'production_downstream_work.py' in proc.read_bytes() else ('flow' if b'continuous_work.py' in proc.read_bytes() else next((s for s in ['preflight','bronze','silver','gold','publish'] if s.encode() in args),stage))
    if action in {'deploy','start','refresh'} and alive:m.fail_json(msg='PRODUCTION_WORKER_ALREADY_RUNNING')
    if action=='prepare-gold':
        progress=json.loads((ROOT/'downstream-progress.json').read_text())
        if not alive or progress.get('stage')!='silver':m.fail_json(msg='GOLD_PREPARATION_REQUIRES_ACTIVE_SILVER')
        current=(ROOT/'production_work.py').read_text();prepared=(STAGE/'production_work.py').read_text()
        needle="elif db.one('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(config['database'],'pipeline_review_events'))['n']:"
        replacement=needle[:-1]+" and db.one('SELECT COUNT(*) AS n FROM pipeline_review_events')['n']:"
        if prepared!=current.replace(needle,replacement):m.fail_json(msg='PREPARATION_DIFF_OUTSIDE_EMPTY_REVIEW_GUARD')
        backup=ROOT/'worker-backups';backup.mkdir(exist_ok=True,mode=0o700)
        shutil.copyfile(ROOT/'production_work.py',backup/(hashlib.sha256(current.encode()).hexdigest()+'.py'))
        target=ROOT/'production_work.next.py';target.write_text(prepared);os.chown(target,u.pw_uid,u.pw_gid);os.chmod(target,0o600)
        target.replace(ROOT/'production_work.py')
    if action=='stop' and alive:
        import signal,time
        command=Path(f'/proc/{pid}/cmdline').read_bytes()
        if b'continuous_work.py' not in command:m.fail_json(msg='STOP_REQUIRES_FLOW_COORDINATOR')
        children=Path(f'/proc/{pid}/task/{pid}/children').read_text().split()
        workers=[]
        for child in children:
            path=Path('/proc')/child/'cmdline'
            args=path.read_bytes().split(b'\0') if path.exists() else []
            if str(ROOT/'production_work.py').encode() in args:
                if b'bronze' not in args:m.fail_json(msg='STOP_REQUIRES_BRONZE_BOUNDARY')
                workers.append(int(child))
        # Stop only the verified coordinator and its Bronze Python process.
        # Source producers and the JVM process group are not signalled.
        os.kill(pid,signal.SIGTERM)
        for worker in workers:
            try:os.kill(worker,signal.SIGTERM)
            except ProcessLookupError:pass
        deadline=time.monotonic()+50
        while time.monotonic()<deadline:
            live=[p for p in [pid,*workers] if (Path(f'/proc/{p}/cmdline').exists() and Path(f'/proc/{p}/cmdline').read_bytes())]
            if not live:break
            time.sleep(1)
        else:m.fail_json(msg='GRACEFUL_STOP_UNCONFIRMED_NO_FORCE_KILL',pids=live)
        alive=False
        (ROOT/'manual-stop.json').write_text(json.dumps(dict(coordinator=pid,bronze_workers=workers,method='SIGTERM_GRACEFUL_NO_CHECKPOINT_DELETION')))
    if action=='refresh':
        marker=STAGE/'production-batch.sha256'
        if marker.exists():
            archive=STAGE/'production-batch.tar.gz';checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
            if checksum!=marker.read_text().strip():m.fail_json(msg='SOURCE_HASH_MISMATCH')
            previous=json.loads((ROOT/'deployment.json').read_text())
            if checksum!=previous['source_sha256']:
                releases=ROOT/'releases';releases.mkdir(exist_ok=True)
                saved=releases/previous['source_sha256']
                if saved.exists():m.fail_json(msg='SOURCE_RELEASE_ALREADY_ARCHIVED')
                (ROOT/'source').rename(saved)
                source=ROOT/'source';source.mkdir(mode=0o700)
                with tarfile.open(archive,'r:gz') as tar:tar.extractall(source,filter='data')
                (ROOT/'deployment.json').write_text(json.dumps(dict(source_sha256=checksum)))
                for p in [source,*(source.rglob('*'))]:os.chown(p,u.pw_uid,u.pw_gid)
        shutil.copyfile(STAGE/'production_work.py',ROOT/'production_work.py');os.chown(ROOT/'production_work.py',u.pw_uid,u.pw_gid)
        for name in ['continuous_work.py','raw_relay.py','production_downstream_work.py']:
            if (STAGE/name).exists():
                shutil.copyfile(STAGE/name,ROOT/name);os.chown(ROOT/name,u.pw_uid,u.pw_gid);os.chmod(ROOT/name,0o600)
        if (STAGE/'connect.py').exists():
            target=BASE/'crawler-connect-0685b031-20261019/connect.py'
            shutil.copyfile(STAGE/'connect.py',target);os.chown(target,u.pw_uid,u.pw_gid)
    if action=='deploy':
        archive=STAGE/'production-source.tar.gz';checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
        if checksum!=m.params['source_sha256']:m.fail_json(msg='SOURCE_HASH_MISMATCH')
        source=ROOT/'source';source.mkdir(exist_ok=True,mode=0o700)
        with tarfile.open(archive,'r:gz') as tar:tar.extractall(source,filter='data')
        shutil.copyfile(STAGE/'production_work.py',ROOT/'production_work.py')
        (ROOT/'deployment.json').write_text(json.dumps(dict(source_sha256=checksum)))
        for p in [ROOT,*ROOT.rglob('*')]:os.chown(p,u.pw_uid,u.pw_gid)
    if action=='start':
        from datetime import datetime,timezone
        attempt=dict(started_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'))
        (ROOT/(stage+'-attempt.json')).write_text(json.dumps(attempt))
        logpath=ROOT/(stage+'.private.log');log=logpath.open('ab');os.chmod(logpath,0o600)
        env={'PATH':'/usr/bin:/bin','HOME':str(ROOT),'JAVA_HOME':'/usr/lib/jvm/java-21-openjdk-amd64','SPARK_CONF_DIR':'/etc/pj3/spark','SPARK_LOCAL_IP':'127.0.0.1','PYTHONPATH':str(ROOT/'source'),'PYSPARK_PYTHON':PY,'PYSPARK_DRIVER_PYTHON':PY,'LANG':'C.UTF-8',
            'AWS_CA_BUNDLE':'/etc/pj3-clients/s3/ca.pem','AWS_DEFAULT_REGION':'us-east-1','PIPELINE_S3_ENDPOINT':'https://192.168.0.141:8443','PIPELINE_S3_CREDENTIALS_FILE':'/etc/pj3-clients/s3/spark.properties',
            'PIPELINE_SPARK_S3_PROFILE':'garage_v241','PIPELINE_SPARK_S3_CREDENTIALS_FILE':'/etc/pj3-clients/s3/spark.properties','PIPELINE_SPARK_S3_BUCKET':'pj3-data',
            'SPARK_SUBMIT_OPTS':'-Djavax.net.ssl.trustStore=/etc/pj3-clients/s3/ca-truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit'}
        command=([PY,str(ROOT/'production_downstream_work.py')] if stage=='downstream' else
            ([PY,str(ROOT/'continuous_work.py')] if stage=='flow' else [PY,str(ROOT/'production_work.py'),stage]))
        p=subprocess.Popen(command,cwd=ROOT,env=env,user=u.pw_uid,group=u.pw_gid,extra_groups=[],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
        pidfile.write_text(str(p.pid));alive=True
    result={'worker_alive':alive,'stage':stage,'root':str(ROOT)}
    if (ROOT/'deployment.json').exists():result['deployment']=json.loads((ROOT/'deployment.json').read_text())
    delta=ROOT/'source/product_pipeline/bronze/delta.py'
    if delta.exists():result['bronze_delta_sha256']=hashlib.sha256(delta.read_bytes()).hexdigest()
    for name in ['flow-progress.json','downstream-progress.json','raw-relay-progress.json','raw-relay-failure.json']:
        if (ROOT/name).exists():result[name]=json.loads((ROOT/name).read_text())
    if (ROOT/(stage+'-attempt.json')).exists():result['attempt_started_at']=json.loads((ROOT/(stage+'-attempt.json')).read_text())['started_at']
    if action=='preflight':
        p=subprocess.run([PY,str(ROOT/'production_work.py'),'preflight'],cwd=ROOT,user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=40,env={'PATH':'/usr/bin:/bin','HOME':str(ROOT),'PYTHONPATH':str(ROOT/'source')})
        result['preflight_returncode']=p.returncode
    progress=[]
    log=ROOT/(stage+'.private.log')
    if log.exists():
        import time
        result['log_bytes']=log.stat().st_size
        result['log_age_seconds']=round(time.time()-log.stat().st_mtime)
        all_progress=[]
        for line in log.read_text(errors='replace').splitlines():
            if line.startswith('{'):
                try:all_progress.append(json.loads(line))
                except ValueError:pass
        result['completed_observations']=sum(p.get('stage')=='silver_observation' for p in all_progress)
        result['latest_observation']=next((p for p in reversed(all_progress) if p.get('stage')=='silver_observation'),None)
        for line in log.read_text(errors='replace').splitlines()[-100:]:
            if line.startswith('{'):
                try:progress.append(json.loads(line))
                except ValueError:pass
        result['recent_progress']=progress[-3:]
        import re
        result['error_types']=list(dict.fromkeys(re.findall(r'\b(?:[A-Za-z]+Error|[A-Za-z]+Exception):',log.read_text(errors='replace')[-20000:])))
        result['sql_error_codes']=re.findall(r'OperationalError: \((\d+),',log.read_text(errors='replace')[-20000:])
        result['spark_progress']=re.findall(r'\[Stage [^\]\r\n]+\]',log.read_text(errors='replace')[-10000:])[-2:]
    for p in sorted(ROOT.glob('*-result.json')):result[p.name]=json.loads(p.read_text())
    for p in sorted(ROOT.glob('*-failure.json')):result[p.name]=json.loads(p.read_text())
    if (ROOT/'flow-progress.json').exists() or (ROOT/'downstream-progress.json').exists():
        import re
        if stage=='downstream' and (ROOT/'downstream-progress.json').exists():
            flow=json.loads((ROOT/'downstream-progress.json').read_text());epoch=Path(flow['execution_root'])
        else:
            flow=json.loads((ROOT/'flow-progress.json').read_text());epoch=ROOT/'epochs'/('%06d'%int(flow.get('epoch',0)))
        diag={}
        for phase in ['bronze','silver','gold','publish']:
            file=epoch/(phase+'.private.log')
            if not file.exists():continue
            tail=file.read_text(errors='replace')[-30000:]
            diag[phase]=dict(frames=re.findall(r'File "[^"\n]+", line \d+, in [^\n]+',tail)[-15:],
                spark_stages=re.findall(r'\[Stage [^\]\r\n]+\]',tail)[-3:],log_bytes=file.stat().st_size,
                java_causes=re.findall(r'Caused by: ([A-Za-z0-9_.$]+)',tail)[-12:],
                java_errors=re.findall(r'(?m)^([a-z][a-z0-9_.]+(?:Error|Exception)):',tail)[-12:],
                json_progress=[json.loads(line) for line in tail.splitlines() if line.startswith('{') and line.endswith('}')][-5:],
                codes=re.findall(r'\b[A-Z][A-Z0-9_]{5,}\b',tail)[-30:],
                errors=re.findall(r'\b\w+(?:Error|Exception):[^\n]*',tail)[-4:])
        result['epoch_diagnostic']=diag
    m.exit_json(changed=action!='status',result=result)
if __name__=='__main__':main()
