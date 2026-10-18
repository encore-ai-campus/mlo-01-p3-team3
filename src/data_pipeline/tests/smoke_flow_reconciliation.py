"""Exercise production reconciliation on saved actual Delta, with explicit DB/S3 doubles."""
import argparse
from pathlib import Path
from unittest.mock import patch

from product_pipeline.config import Settings
from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.gold.config import GoldSettings
from product_pipeline.silver.config import SilverSettings
from product_pipeline.files import read_json,atomic_json,sha_file,confined
from product_pipeline.mongo import bson_document
from product_pipeline.operations.flow_runtime import spark_session
from product_pipeline.operations.flow_audit import live_audit


def main():
    p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--spark-options',type=Path,required=True);a=p.parse_args()
    source=a.source.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    import os
    os.environ['GOLD_SPARK_OPTIONS']=a.spark_options.read_text(encoding='utf-8')
    spark=spark_session();spark.sparkContext.setLogLevel('ERROR')
    originals=[read_json(file) for file in (source/'ledger/observations').glob('*.json')]
    class MongoDouble:
        def __init__(self,*args):self.raw=self
        def get(self,key):return next(r for r in originals if r['observation_key']==key)
        def find_one(self,query):return bson_document(next(r['document'] for r in originals if r['document']['_id']==str(query['_id'])))
        def close(self):pass
    class ObjectsDouble:
        def __init__(self,*args):pass
        def _exists(self,key,checksum,size):
            path=confined(source/'objects',key)
            return path.is_file() and path.stat().st_size==size and sha_file(path)==checksum
    class SparkProxy:
        def __getattr__(self,name):return getattr(spark,name)
        def stop(self):pass
    settings=Settings('rehearsal','rehearsal','rehearsal-images')
    bronze=BronzeSettings('flow-local','rehearsal',*((source/k).as_uri() for k in ('bronze','events-delta','quarantine','bronze-checkpoint')),
                         'fixture-kafka',single_writer_confirmed=True)
    silver=SilverSettings('flow-local','rehearsal',bronze.bronze_path,(source/'silver').as_uri(),(source/'silver-checkpoint').as_uri())
    gold=GoldSettings('flow-local',silver.silver_root,(source/'gold').as_uri(),(source/'gold-checkpoint').as_uri())
    plan=read_json(source/'flow/flow.json');outputs={file.stem:read_json(file)['output'] for file in (source/'flow/receipts').glob('*.json')}
    context=dict(flow=plan,root=str(source/'flow'),outputs=outputs,audit_directory=str(out))
    try:
        with patch('product_pipeline.mongo.MongoLedger',MongoDouble),patch('product_pipeline.objects.S3Objects',ObjectsDouble),\
                patch('product_pipeline.cli._env',return_value='aws'),patch('product_pipeline.operations.flow_runtime.spark_session',return_value=SparkProxy()):
            result=live_audit(context,{},settings,bronze,silver,gold)
            assert result['observations']==result['bronze_raw']==result['bronze_event_receipts']==1
            assert result['gold_products']==result['blocked_products']==1 and result['sql']['products']==0
            assert result['available_image_relations']==2
            from product_pipeline.errors import Conflict
            with patch.object(ObjectsDouble,'_exists',return_value=False):
                try:live_audit(context,{},settings,bronze,silver,gold);raise AssertionError('missing object accepted')
                except Conflict as exc:assert exc.code=='FLOW_IMAGE_OBJECT_MISSING_OR_CHANGED'
        report=dict(passed=True,production_reconciliation_logic_exercised=True,actual_delta=True,
            mongo_s3_backends='explicit-local-doubles',mysql_checked='separate-isolated-smoke',
            external_service_connections=0,missing_image_object_rejected=True,result=result)
        atomic_json(out/'report.json',report);print(__import__('json').dumps(report));return 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
