"""Actual VM Mongo client test body used on 2026-10-18.

Opt-in verification only: writes one temporary document, tests live MongoLease,
then deletes only its test collection and owned claims. Run as pj3-spark with
its protected mongo-accounts.json on stdin. Never supply secrets on argv.
Parse status and test_collection_deleted; process exit alone is not a verdict.
This fixture targets the explicitly approved pj3_products_raw / pipeline_ops.
"""
import json,os,sys,time,uuid
from pathlib import Path
from urllib.parse import quote
from pymongo import MongoClient,ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.bronze.lease import MongoLease
from product_pipeline.errors import PipelineError
spec=json.load(sys.stdin);started=time.monotonic();accounts={a['username']:a for a in spec['accounts']};ca=spec['ca']
def uri(user,host=None):
 a=accounts[user];return 'mongodb://'+quote(a['username'],safe='')+':'+quote(a['password'],safe='')+'@'+(host or 'mongo01.pj3.internal:27017,mongo02.pj3.internal:27017,mongo03.pj3.internal:27017')+'/?authSource=admin&tls=true&tlsCAFile='+quote(ca,safe='')+('&directConnection=true' if host else '&replicaSet=rs-products')
result={'status':'FAILED','business_records_written':False};client=None;lease=None;owned=False;name='_pipeline_verify_'+uuid.uuid4().hex
try:
 client=MongoClient(uri('pipeline_ingest'),retryWrites=False,serverSelectionTimeoutMS=7000,socketTimeoutMS=12000)
 hello=client.admin.command('hello');result['replica_set']=hello['setName']
 db=client.get_database('pj3_products_raw',write_concern=WriteConcern(w='majority',j=True,wtimeout=10000),read_concern=ReadConcern('majority'))
 if name in db.list_collection_names():raise ValueError('TEST_COLLECTION_EXISTS')
 col=db[name];doc={'_id':uuid.uuid4().hex,'purpose':'minimal-replication-verification','value':1};owned=True
 ack=col.insert_one(doc);assert ack.acknowledged and col.find_one({'_id':doc['_id']})==doc
 reads=[]
 for host in hello['hosts']:
  with MongoClient(uri('pipeline_ingest',host),retryWrites=False,serverSelectionTimeoutMS=7000,socketTimeoutMS=12000)as member:
   mode=member.admin.command('hello');pref=ReadPreference.PRIMARY if mode.get('isWritablePrimary') else ReadPreference.SECONDARY
   target=member.get_database('pj3_products_raw',read_preference=pref,read_concern=ReadConcern('majority'))[name]
   for i in range(15):
    if target.find_one({'_id':doc['_id']})==doc:break
    time.sleep(0.2)
   else:raise ValueError('MEMBER_READ_MISMATCH')
   reads.append({'host':host,'primary':bool(mode.get('isWritablePrimary')),'majority_read_matched':True})
 result.update(write_concern={'w':'majority','j':True},member_reads=reads)
 if spec['role']=='spark':
  prefix='s3a://pj3-data/verification/lease-'+uuid.uuid4().hex
  settings=BronzeSettings(environment_id='pj3-vm-verification',mongo_database='pj3_products_raw',kafka_cluster_id='AGceilLlygifiV1hss1Rbw',bronze_path=prefix+'/raw',events_path=prefix+'/events',quarantine_path=prefix+'/quarantine',checkpoint_path=prefix+'/checkpoint',single_writer_confirmed=True)
  lease=MongoLease(settings,uri('pipeline_spark')).acquire();lease.check(settings);lease.check_s3_bucket('pj3-data')
  other=MongoLease(settings,uri('pipeline_spark'))
  try:
   try:other.acquire();raise ValueError('DUPLICATE_LEASE_NOT_BLOCKED')
   except PipelineError as e:
    if e.code!='BRONZE_WRITER_ALREADY_CLAIMED':raise
  finally:other.close(release=True)
  lease.close(release=True);lease=None
  with MongoClient(uri('pipeline_spark'),serverSelectionTimeoutMS=7000)as s:
   assert s.pipeline_ops.bronze_writer_leases.count_documents({'target_fingerprint':settings.fingerprint})==0
  result['mongo_lease']={'acquire_renew':True,'same_paths_second_writer_blocked':True,'owned_claims_released':True}
 result['status']='MONGO_CLIENT_REPLICATION_VERIFIED'
except Exception as e:result['error_type']=type(e).__name__;result['error_code']=e.code if isinstance(e,PipelineError)else 'VERIFICATION_FAILED'
finally:
 if lease is not None:lease.close(release=False)
 if owned and client is not None:
  try:db.drop_collection(name);result['test_collection_deleted']=name not in db.list_collection_names()
  except Exception:result['test_collection_deleted']=False;result['status']='CLEANUP_FAILED'
 if client is not None:client.close()
 result['duration_ms']=round((time.monotonic()-started)*1000)
print(json.dumps(result))
