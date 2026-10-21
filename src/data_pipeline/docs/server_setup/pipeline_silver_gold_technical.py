from ansible.module_utils.basic import AnsibleModule
import pathlib,os,pwd,subprocess,json
CODE=r"""
import pathlib,json,os,sys,time
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4
root=pathlib.Path.cwd()
# Reuse the exact protected runtime setup from the successful Bronze test.
setup=(root/'live_acceptance_resumed.py').read_text().split("sys.argv=['tests.smoke_live_services'")[0]
exec(compile(setup,'verified-protected-runtime','exec'))
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from product_pipeline.bronze.lease import MongoLease
from product_pipeline.spark_storage import storage_options,bind_spark_credentials
from product_pipeline.contracts import now,canonical
from product_pipeline.silver.config import SilverSettings
from product_pipeline.silver.source import BronzeSource,MongoStates
from product_pipeline.silver.inputs import input_snapshot
from product_pipeline.silver.delta import DeltaSink as SilverSink
from product_pipeline.silver.checkpoint import Checkpoint as SilverCheckpoint
from product_pipeline.silver.runner import process as silver_process
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.source import SilverSource
from product_pipeline.gold.storage import DeltaSink as GoldSink,Checkpoint as GoldCheckpoint
from product_pipeline.gold.runner import process as gold_process
from product_pipeline.gold.contracts import INPUT_TABLES
from tests.smoke_gold_delta import empty_inputs
from product_pipeline.errors import PipelineError
from product_pipeline.files import atomic_json
prefix='s3a://pj3-data/verification/'+root.name
report={'status':'FAIL','scope':'real server Mongo/S3/Delta with NOT_PROVIDED owner inputs','business_registration':'NOT_PROVIDED','owner_approval_fabricated':False,'stages':[]};spark=lease=states=None;stopped=True;started=time.monotonic()
def session(settings,target):
 global spark,lease,stopped
 proxy=SimpleNamespace(**{**asdict(settings),'bronze_path':target,'events_path':target,'quarantine_path':target,'fingerprint':settings.fingerprint})
 lease=MongoLease(proxy,uri('pipeline_spark')).acquire()
 opts=storage_options(json.loads(os.environ['BRONZE_SPARK_OPTIONS']),writer=True,lease=lease)
 b=SparkSession.builder.appName('pipeline-silver-gold-server-fixture')
 for k,v in opts.items():b=b.config(k,v)
 spark=b.config('spark.sql.session.timeZone','UTC').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate();stopped=False
 bind_spark_credentials(spark,writer=True,lease=lease);lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs());spark.sparkContext.setLogLevel('ERROR')
def close():
 global spark,lease,states,stopped
 if states is not None:states.close();states=None
 if spark is not None:spark.stop();stopped=True;spark=None
 if lease is not None:lease.close(release=stopped);lease=None
try:
 s=SilverSettings('pj3-verify-0594f7b7','pj3_products_raw',prefix+'/bronze',prefix+'/silver-technical',prefix+'/silver-technical-checkpoint',max_records=1)
 session(s,s.silver_root);version=int(DeltaTable.forPath(spark,s.bronze_path).history(1).first()['version']);docs=BronzeSource(spark,s,version).page(None);assert len(docs)==1
 states=MongoStates(s,uri('pipeline_spark'));side={k:input_snapshot(k)for k in ['mappings','reviews','links','rules']};cp=SilverCheckpoint(spark,s,lease);cp.archive(side);sink=SilverSink(spark,s,lease=lease);as_of=now();run_id=str(uuid4())
 first=silver_process(docs[0],states,sink,side,processing_run_id=run_id,as_of=as_of,bronze_version=version,bronze_path=s.bronze_path,archive=cp.archive)
 repeated=silver_process(docs[0],states,sink,side,processing_run_id=str(uuid4()),as_of=as_of,bronze_version=version,bronze_path=s.bronze_path,archive=cp.archive)
 counts={k:spark.read.format('delta').load(p).count()for k,p in sink.paths.items()};assert counts['product_observations']==counts['pipeline_evaluations']==1 and repeated['reused']and first['status']=='QUALITY_WAIT'
 versions={t:int(DeltaTable.forPath(spark,s.silver_root+'/'+t).history(1).first()['version'])for t in INPUT_TABLES}
 report['stages'].append({'stage':'silver','status':'PASS','counts':counts,'quality':first['status'],'repeat_reused':True,'fixed_bronze_version':version,'mongo_states':True,'mongo_lease':True})
 atomic_json(root/'silver-technical-report.json',report['stages'][-1]);close()
 g=GoldSettings('pj3-verify-0594f7b7',s.silver_root,prefix+'/gold-technical',prefix+'/gold-technical-checkpoint');session(g,g.gold_root)
 source=SilverSource(spark,g,versions);products=source.products();assert len(products)==1;side=empty_inputs();side['policy']['evidence_ref']='fixture://server-technical-verification';from product_pipeline.contracts import digest
 side['content_hash']=digest({k:v for k,v in side.items()if k!='content_hash'});cp=GoldCheckpoint(spark,g,lease);cp.archive(side);sink=GoldSink(spark,g,lease=lease);groups=source.groups(products[0]);run_id=str(uuid4())
 first=gold_process(groups,side,versions,sink,processing_run_id=run_id,as_of=as_of);repeated=gold_process(groups,side,versions,sink,processing_run_id=str(uuid4()),as_of=as_of)
 counts={k:spark.read.format('delta').load(p).count()for k,p in sink.paths.items()};assert counts['product_catalog']==counts['pipeline_evaluations']==1 and repeated['reused'];assert spark.read.format('delta').load(sink.paths['product_catalog']).filter('recommendation_ready = true').count()==0
 report['stages'].append({'stage':'gold','status':'PASS','counts':counts,'quality':first['status'],'repeat_reused':True,'recommendation_ready_count':0,'fixed_silver_versions':versions,'mongo_lease':True});atomic_json(root/'gold-technical-report.json',report['stages'][-1]);report['status']='TECHNICAL_FIXTURE_PASS'
except Exception as e:report['error_code']=e.code if isinstance(e,PipelineError)else type(e).__name__
finally:
 try:close()
 except Exception:report['shutdown_unconfirmed']=True;report['status']='FAIL'
 report['duration_ms']=round((time.monotonic()-started)*1000);atomic_json(root/'silver-gold-technical-result.json',report);print(canonical(report))
"""
def main():
 m=AnsibleModule(argument_spec={});u=pwd.getpwnam('pj3-spark');root=pathlib.Path('/var/lib/pj3/product-pipeline/spark/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af');script=root/'silver_gold_technical.py';script.write_text(CODE);script.chmod(0o600);os.chown(script,u.pw_uid,u.pw_gid)
 py='/var/lib/pj3/product-pipeline/spark/venvs/ce456c2fd3972154-spark/bin/python';env={'PATH':'/usr/bin:/bin','HOME':str(root),'JAVA_HOME':'/usr/lib/jvm/java-21-openjdk-amd64','SPARK_CONF_DIR':'/etc/pj3/spark','SPARK_LOCAL_IP':'127.0.0.1','PYTHONPATH':str(root/'fixed-source'),'PYSPARK_PYTHON':py,'LANG':'C.UTF-8','AWS_CA_BUNDLE':'/etc/pj3-clients/s3/ca.pem','AWS_DEFAULT_REGION':'us-east-1','PIPELINE_S3_ENDPOINT':'https://192.168.0.141:8443','PIPELINE_S3_CREDENTIALS_FILE':'/etc/pj3-clients/s3/spark.properties','PIPELINE_SPARK_S3_PROFILE':'garage_v241','PIPELINE_SPARK_S3_CREDENTIALS_FILE':'/etc/pj3-clients/s3/spark.properties','SPARK_SUBMIT_OPTS':'-Djavax.net.ssl.trustStore=/etc/pj3-clients/s3/ca-truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit'}
 try:
  p=subprocess.run([py,str(script)],cwd=root,env=env,user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=650);log=root/'silver-gold-private.log';log.write_text(p.stdout+'\n'+p.stderr);log.chmod(0o600);os.chown(log,u.pw_uid,u.pw_gid)
  f=root/'silver-gold-technical-result.json';result=json.loads(f.read_text())if f.exists()else{'status':'PROCESS_FAILED_BEFORE_REPORT'};result['process_returncode']=p.returncode
 except subprocess.TimeoutExpired:result={'status':'TIMEOUT_SHUTDOWN_UNCONFIRMED'}
 m.exit_json(changed=True,result=result)
if __name__=='__main__':main()
