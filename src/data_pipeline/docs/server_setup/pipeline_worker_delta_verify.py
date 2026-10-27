"""Ansible: one isolated local Spark Driver, private Maven cache, no service install."""
import json
import hashlib
import os
from pathlib import Path
import pwd
import re
import subprocess
from ansible.module_utils.basic import AnsibleModule


def main():
    module=AnsibleModule(argument_spec=dict(run_id=dict(required=True),script=dict(required=True),
        delta_version=dict(default='4.4.1',choices=['4.1.0','4.4.1'])))
    delta_version=module.params['delta_version']
    run=module.params['run_id']
    if not re.fullmatch(r'pipeline-worker-delta-[a-f0-9]{32}',run):module.fail_json(msg='INVALID_TEST_RUN')
    identity=pwd.getpwnam('pj3-spark')
    root=Path('/var/lib/pj3/spark')/run
    root.mkdir(mode=0o700,exist_ok=False);os.chown(root,identity.pw_uid,identity.pw_gid)
    script=root/'verify.scala';script.write_text(module.params['script']);script.chmod(0o600);os.chown(script,identity.pw_uid,identity.pw_gid)
    # Resolve with the OS CA before launching the JVM with its private S3 CA.
    # Download only fixed Delta runtime jars, never another Hadoop/AWS SDK.
    deps=root/'dependencies';deps.mkdir(mode=0o700);os.chown(deps,identity.pw_uid,identity.pw_gid)
    ivy=next(Path('/opt/pj3/spark/4.1.3/jars').glob('ivy-*.jar'))
    settings=root/'ivysettings.xml'
    settings.write_text('<ivysettings><settings defaultResolver="official"/><resolvers>'
        '<ibiblio name="official" m2compatible="true" root="https://repo.maven.apache.org/maven2/"/>'
        '</resolvers></ivysettings>')
    settings.chmod(0o600);os.chown(settings,identity.pw_uid,identity.pw_gid)
    resolver_env=dict(PATH='/usr/bin:/bin',HOME=str(root),JAVA_HOME='/usr/lib/jvm/java-21-openjdk-amd64')
    resolved=subprocess.run(['/usr/lib/jvm/java-21-openjdk-amd64/bin/java','-jar',str(ivy),
        '-settings',str(settings),'-dependency','io.delta','delta-spark_4.1_2.13',delta_version,
        '-confs','default','-cache',str(root/'ivy'),'-retrieve',str(deps/'[artifact]-[revision].[ext]')],
        capture_output=True,text=True,env=resolver_env,cwd=root,user=identity.pw_uid,
        group=identity.pw_gid,extra_groups=[],timeout=100)
    if resolved.returncode:
        module.exit_json(changed=True,result=dict(status='DEPENDENCY_RESOLUTION_FAILED',run_id=run,
                         service_config_modified=False))
    proofs={};delta_jars=[]
    existing={re.split(r'-[0-9]',jar.name,maxsplit=1)[0] for jar in Path('/opt/pj3/spark/4.1.3/jars').glob('*.jar')}
    for jar in sorted(deps.glob('*.jar')):
        artifact=re.split(r'-[0-9]',jar.name,maxsplit=1)[0]
        if artifact in existing:continue
        if artifact.startswith('hadoop-') or artifact == 'bundle' or artifact.startswith('aws-'):
            module.fail_json(msg='DELTA_TEST_CONFLICTING_INFRA_DEPENDENCY')
        delta_jars.append(str(jar));proofs[jar.name]=hashlib.sha256(jar.read_bytes()).hexdigest()
    trust=('-Djavax.net.ssl.trustStore=/etc/pj3-clients/s3/ca-truststore.p12 '
           '-Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit')
    env=dict(PATH='/usr/bin:/bin',JAVA_HOME='/usr/lib/jvm/java-21-openjdk-amd64',
             SPARK_CONF_DIR='/etc/pj3/spark',SPARK_LOCAL_IP='192.168.0.121',HOME=str(root),
             LANG='C.UTF-8',SPARK_SUBMIT_OPTS=trust,PJ3_GARAGE_DELTA_RUN=run)
    options={
      'spark.ui.enabled':'false','spark.executor.cores':'1','spark.cores.max':'3','spark.executor.memory':'512m','spark.sql.shuffle.partitions':'1','spark.databricks.delta.snapshotPartitions':'1',
      'spark.jars.ivy':str(root/'ivy'),
      'spark.sql.extensions':'io.delta.sql.DeltaSparkSessionExtension',
      'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
      'spark.delta.logStore.s3a.impl':'io.delta.storage.S3SingleDriverLogStore',
      'spark.executor.extraJavaOptions':trust,
      'spark.hadoop.fs.s3a.endpoint':'https://192.168.0.141:8443',
      'spark.hadoop.fs.s3a.endpoint.region':'us-east-1','spark.hadoop.fs.s3a.path.style.access':'true',
      'spark.hadoop.fs.s3a.connection.ssl.enabled':'true',
      'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
      'spark.hadoop.fs.s3a.change.detection.source':'etag','spark.hadoop.fs.s3a.change.detection.mode':'server',
      'spark.hadoop.fs.s3a.change.detection.version.required':'false','spark.hadoop.fs.s3a.bucket.probe':'0',
      'spark.redaction.regex':'(?i)secret|password|token|access[.]?key',
    }
    args=['/opt/pj3/spark/4.1.3/bin/spark-shell','--master','spark://192.168.0.121:7077','--driver-memory','512m',
          '--jars',','.join(['local:/opt/pj3/s3-clients/hadoop-3.4.2/hadoop-aws-3.4.2.jar',
             'local:/opt/pj3/s3-clients/hadoop-3.4.2/bundle-2.29.52.jar',*delta_jars])]
    for key,value in options.items():args.extend(['--conf',key+'='+value])
    try:
        job=subprocess.run(args,input=script.read_text()+'\n:quit\n',capture_output=True,text=True,env=env,cwd=root,
             user=identity.pw_uid,group=identity.pw_gid,extra_groups=[],timeout=200)
        # Keep only explicit public markers, not Spark output or properties content.
        log=job.stdout+'\n'+job.stderr
        markers=re.findall(r'(?m)^PJ3_GARAGE_DELTA_(?:PASS|CLEANED|ERROR=[A-Za-z0-9_.$]+|MISSING=[A-Za-z0-9_.$/]+)\s*$',log)
        markers=[marker.strip() for marker in markers]
        proof=json.loads((root/'delta-proof.json').read_text()) if (root/'delta-proof.json').is_file() else None
        passed=job.returncode==0 and proof==dict(status='VERIFIED',run_id=run,current_rows=2,
                                                  version_zero_rows=1,test_prefix_deleted=True,worker_hosts=['192.168.0.122','192.168.0.123','192.168.0.124'])
        result=dict(status='GARAGE_DELTA_CLUSTER_VERIFIED' if passed else 'FAILED',
            returncode=job.returncode,markers=markers,run_id=run,delta_version=delta_version,
            local_driver_only=False,cluster_workers_tested=passed,mongo_lease_tested=False,
            dependencies_cache=str(deps),jar_sha256=proofs,service_config_modified=False,
            unresolved_dependency='unresolved dependency' in log.lower())
        result['proof']=proof
        result['worker_hosts_observed']=re.findall(r'PIPELINE_WORKER_HOSTS=([0-9.,]+)',log)
        if not passed:
            result['public_diagnostics']=[line[:300] for line in log.splitlines()
                if re.search(r'error:|Syntax Error|Not Found Error|not found:|bad option|Ignoring',line)
                and not re.search(r'(?i)secret|password|access.?key|token|authorization',line)][:8]
    except subprocess.TimeoutExpired:
        result=dict(status='TIMEOUT_SHUTDOWN_UNCONFIRMED',run_id=run,
                    test_prefix='s3a://pj3-data/verification/'+run,service_config_modified=False)
    module.exit_json(changed=True,result=result)


if __name__=='__main__':main()
