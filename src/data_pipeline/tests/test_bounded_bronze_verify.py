import sys,unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.bronze.delta import DeltaSink
from product_pipeline.errors import Conflict


class BoundedVerifyTests(unittest.TestCase):
    def verify(self,values,count):
        spark=MagicMock();reader=spark.read.schema.return_value;frame=reader.parquet.return_value.limit.return_value
        frame.withColumn.return_value=frame;frame.collect.return_value=[SimpleNamespace(asDict=lambda v=v:v) for v in values]
        sink=SimpleNamespace(spark=spark,paths={'events':'s3a://bucket/events'},schemas={'events':'schema'},
            settings=SimpleNamespace(fingerprint='f'),target_metadata={'events':{'table_id':'id','version':1}})
        current=dict(table_id='id',version=2,schema='schema',properties={'delta.appendOnly':'true','pipeline.targetFingerprint':'f'})
        with patch('product_pipeline.bronze.metadata.read_metadata',return_value=current),patch('product_pipeline.delta_range.append_files',return_value=['s3a://bucket/events/own.parquet']),patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=MagicMock())}):
            result=DeltaSink._newly_stored(sink,'events',count)
        reader.parquet.assert_called_once_with('s3a://bucket/events/own.parquet')
        frame.collect.assert_called_once();return result
    def test_postwrite_reads_only_confirmed_new_files(self):
        self.assertEqual(self.verify([{'event_id':'new'}],1),{'new':{'event_id':'new'}})
    def test_missing_extra_or_duplicate_rows_are_not_success(self):
        for values,count in [([],1),([{'event_id':'new'},{'event_id':'extra'}],1),([{'event_id':'same'},{'event_id':'same'}],2)]:
            with self.assertRaises(Conflict):self.verify(values,count)
