"""Apply a checked release only after the requested completed-batch pause.

This is a one-shot deployment workflow, not the production scheduler. Source
inputs, checkpoints, approvals and historic Delta versions are never deleted.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
from datetime import datetime,timezone


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--release',type=Path,required=True)
    args=parser.parse_args();root=args.root.resolve();release=args.release.resolve()
    assert release.is_relative_to(root) and not release.is_symlink()
    sys.path[:0]=[str(root),str(root/'source')]
    from product_pipeline.files import atomic_json,read_json,lock
    from product_pipeline.operations.server_flow import worker
    from product_pipeline.recovery_probe import probe
    from product_pipeline.lease_recovery import assert_quiet
    from product_pipeline.contracts import now
    manifest=read_json(release/'manifest.json');status_path=root/'perf-20261020/range-maintenance-state.json'
    pause=root/'pause-after-window.json';pause_hash=hashlib.sha256(pause.read_bytes()).hexdigest()
    def save(status,**kw):atomic_json(status_path,dict(status=status,at=now(),pid=os.getpid(),release=str(release),**kw))
    def quiet():assert_quiet(probe(master_url='http://192.168.0.121:8080/json/',ca='/etc/pj3-secrets/spark/ca.pem',
        client_cert='/etc/pj3-secrets/spark/cert.pem',client_key='/etc/pj3-secrets/spark/key.pem',
        worker_hosts=['192.168.0.122','192.168.0.123','192.168.0.124']))
    def settled_quiet():
        from product_pipeline.errors import PipelineError
        until=time.monotonic()+120
        while True:
            try:quiet();return
            except PipelineError as exc:
                if exc.code!='LEASE_RECOVERY_CLUSTER_NOT_QUIESCENT' or time.monotonic()>until:raise
                time.sleep(5)
    def unchanged_pause():assert pause.exists() and hashlib.sha256(pause.read_bytes()).hexdigest()==pause_hash,'STOP_REQUEST_CHANGED'
    env=dict(os.environ,PYTHONPATH=str(root/'source'),JAVA_HOME='/usr/lib/jvm/java-21-openjdk-amd64',
        SPARK_CONF_DIR='/etc/pj3/spark',SPARK_LOCAL_IP='192.168.0.121',PYSPARK_PYTHON='/usr/bin/python3',
        PYSPARK_DRIVER_PYTHON=sys.executable,AWS_CA_BUNDLE='/etc/pj3-clients/s3/ca.pem',AWS_DEFAULT_REGION='us-east-1',
        PIPELINE_S3_ENDPOINT='https://192.168.0.141:8443',PIPELINE_SPARK_S3_PROFILE='garage_v241',
        PIPELINE_SPARK_S3_CREDENTIALS_FILE='/etc/pj3-clients/s3/spark.properties',PIPELINE_SPARK_S3_BUCKET='pj3-data',
        PIPELINE_SPARK_RUNTIME_FILE=read_json(root/'runtime-overrides.json')['spark_runtime_file'],
        PIPELINE_DELTA_KEY_INDEX_DIR=str(root/'key-index-cache'))
    def child(argv,name,timeout,isolated=False):
        path=root/'perf-20261020'/name
        with path.open('ab') as stream:
            path.chmod(0o600)
            child_env={k:v for k,v in env.items() if not (isolated and k.startswith(('PIPELINE_','AWS_','BRONZE_','SILVER_','GOLD_','PUBLICATION_')))}
            p=subprocess.Popen(argv,cwd=root,env=child_env,stdout=stream,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL)
            p.wait(timeout=timeout)
            assert p.returncode==0,'MAINTENANCE_CHILD_FAILED_'+name
    with lock(root/'maintenance-controller.lock'):
        try:
            save('WAITING_BATCH_BOUNDARY');deadline=time.monotonic()+3*60*60
            while worker(root) is not None:
                unchanged_pause()
                if time.monotonic()>deadline:raise RuntimeError('BATCH_BOUNDARY_TIMEOUT')
                time.sleep(10)
            unchanged_pause();flow=read_json(root/'flow-progress.json')
            assert flow.get('status')=='PAUSED_AFTER_BATCH','COMPLETED_BATCH_PAUSE_REQUIRED'
            settled_quiet();save('DEPLOYING',paused_epoch=flow['epoch'])
            with lock(root/'controller.lock'):
                unchanged_pause();assert worker(root) is None
                for row in manifest['files']:
                    source=release/row['source'];target=root/row['target']
                    assert source.resolve().is_relative_to(release) and target.resolve().is_relative_to(root)
                    assert not source.is_symlink() and not target.is_symlink()
                    assert hashlib.sha256(source.read_bytes()).hexdigest()==row['sha256']
                    actual=hashlib.sha256(target.read_bytes()).hexdigest() if target.exists() else None
                    assert actual==row['expected_current_sha256'],'DEPLOYED_CODE_CHANGED_SINCE_PREPARATION'
                for index,row in enumerate(manifest['files']):
                    source=release/row['source'];target=root/row['target'];target.parent.mkdir(parents=True,exist_ok=True)
                    backup=release/'before'/str(index);backup.parent.mkdir(exist_ok=True,mode=0o700)
                    if target.exists():shutil.copy2(target,backup);backup.chmod(0o600)
                    temporary=target.with_name(target.name+'.maintenance-next')
                    shutil.copyfile(source,temporary);temporary.chmod(0o600);temporary.replace(target)
                deployment=root/'deployment.json';current=read_json(deployment)
                shutil.copy2(deployment,release/'before/deployment.json')
                source_identity={row['target']:row['sha256'] for row in manifest['files']}
                current.update(performance_patch_sha256=hashlib.sha256(json.dumps(source_identity,sort_keys=True).encode()).hexdigest(),
                    maintenance_release=str(release.relative_to(root)),maintenance_applied_at=now())
                atomic_json(deployment,current)
            save('SERVER_TESTS');child([sys.executable,str(release/'server_tests.py')],'range-server-tests.private.log',180,isolated=True)
            save('COMPACTING');child([sys.executable,str(root/'compact_pipeline_tables.py'),'--deployment-root',str(root)],'range-compaction.private.log',7200)
            assert read_json(root/'perf-20261020/range-compaction-20261022.json')['status']=='COMPLETED'
            settled_quiet();unchanged_pause()
            # The user explicitly requested discussion before any restart.
            # Keep the same stop marker so NiFi cannot resume this deployment.
            save('COMPLETED_PAUSED',worker_pid=None,old_versions_deleted=False,
                continuous_restart=False,window_records=100)
        except BaseException as exc:
            save('FAILED',error_type=type(exc).__name__,error=str(exc)[:180],pause_preserved=pause.exists());raise


if __name__=='__main__':main()
