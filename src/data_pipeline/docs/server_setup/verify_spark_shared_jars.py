"""Read-only cluster check. Use the protected production launcher environment.

Run twice in separate Python processes; compare Worker bundle counts outside
the application. Writes only verification reports under the execution root.
"""
import json,os,sys,time,traceback
from pathlib import Path
ROOT=Path(os.environ.get('PIPELINE_EXECUTION_ROOT','/var/lib/pj3/product-pipeline/spark/production-pending-20261019'))
sys.path[:0]=[str(ROOT),str(ROOT/'source')]
from production_work import setup,session,LAKE
from product_pipeline.files import atomic_json
out=ROOT/'perf-20261020';result={'status':'FAILED'};spark=None
try:
 _,options=setup();started=time.monotonic();spark=session(options)
 result['app_id']=spark.sparkContext.applicationId
 result['jars']=spark.sparkContext.getConf().get('spark.jars','')
 result['driver_classpath']=spark.sparkContext.getConf().get('spark.driver.extraClassPath','')
 result['executor_classpath']=spark.sparkContext.getConf().get('spark.executor.extraClassPath','')
 loader=spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
 result['classes']={name:str(loader.loadClass(name).getProtectionDomain().getCodeSource().getLocation())
                    for name in ('org.apache.hadoop.fs.s3a.S3AFileSystem','software.amazon.awssdk.services.s3.S3Client')}
 assert all(v.startswith('file:/opt/pj3/s3-clients/hadoop-3.4.2/') for v in result['classes'].values())
 assert 'bundle-' not in result['jars'] and 'hadoop-aws' not in result['jars']
 # Read-only actual Delta/S3 input: verifies executor JVM classpath, not just driver startup.
 result['read_rows']=len(spark.read.format('delta').load(LAKE+'/bronze/products').limit(1).collect())
 result['hosts']=sorted(set(r['host'] for r in spark.range(0,30,1,6).selectExpr("reflect('java.net.InetAddress','getLocalHost') as host").collect()))
 result['duration_seconds']=round(time.monotonic()-started,2);result['status']='VERIFIED'
except Exception as exc:
 result.update(error_code=getattr(exc,'code',type(exc).__name__),error=str(exc)[-1500:],frames=traceback.format_exc()[-2000:])
finally:
 if spark is not None:spark.stop()
 atomic_json(out/'jar-smoke-result.json',result)
 if result.get('app_id'):atomic_json(out/('jar-smoke-'+result['app_id']+'.json'),result)
if result['status']!='VERIFIED':raise SystemExit(2)
