import copy
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,Mock,patch

from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.spark_session import SessionScope
from product_pipeline.delta_proofs import verify_many,existing_hashes_many
from product_pipeline.silver.checkpoint import Checkpoint


class SharedProofTests(unittest.TestCase):
    def sink(self,table_id='one',version=5):
        return SimpleNamespace(settings=object(),lease=SimpleNamespace(check=Mock()),_proofs={},
            paths={'a':'s3a://test/silver/a'},target_metadata={'a':{'table_id':table_id}},
            versions_many=Mock(return_value={'a':version}))

    def test_fixed_version_is_reused_between_sinks_but_new_version_and_table_are_not(self):
        with SessionScope(),patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(self.sink(),[('a',5,{'k':'hash'})])
            verify_many(self.sink(),[('a',5,{'k':'hash'})]);self.assertEqual(read.call_count,1)
            verify_many(self.sink(version=6),[('a',5,{'k':'hash'})])
            self.assertEqual(read.call_args.args[1],[('a',6,{'k':'hash'})])
            verify_many(self.sink(table_id='replaced'),[('a',5,{'k':'hash'})]);self.assertEqual(read.call_count,3)
        with SessionScope(),patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(self.sink(),[('a',5,{'k':'hash'})]);read.assert_called_once()

    def test_reuse_subtracts_only_confirmed_rows_and_failure_never_promotes_partial_rows(self):
        with SessionScope(),patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(self.sink(),[('a',5,{'old':'h'})])
            read.side_effect=Conflict('read failed')
            with self.assertRaises(Conflict):verify_many(self.sink(),[('a',5,{'old':'h','new':'n'})])
            self.assertEqual(read.call_args.args[1],[('a',5,{'new':'n'})])
            read.side_effect=None
            verify_many(self.sink(),[('a',5,{'old':'h','new':'n'})])
            self.assertEqual(read.call_args.args[1],[('a',5,{'new':'n'})])

    def test_prewrite_reuses_positive_proofs_but_never_missing_rows(self):
        with SessionScope(),patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(self.sink(),[('a',5,{'old':'h'})])
            read.return_value={('a',5):{}}
            result=existing_hashes_many(self.sink(),{'a':{'old':'h','new':'n'}})
            self.assertEqual(result,{'a':{'old':'h'}})
            self.assertEqual(read.call_args.args[1],[('a',5,{'new':'n'})])
            existing_hashes_many(self.sink(),{'a':{'new':'n'}})
            self.assertEqual(read.call_count,3)


class ArchiveBundleTests(unittest.TestCase):
    def checkpoint(self):
        cp=Checkpoint.__new__(Checkpoint);cp.path=SimpleNamespace(toString=lambda:'s3a://test/checkpoint')
        cp.saved={}
        def archive(value):
            ref=cp.path.toString()+'/inputs/'+digest(value)+'.json'
            cp.saved[ref]=copy.deepcopy(value);return ref
        cp.archive=Mock(side_effect=archive)
        cp.jvm=SimpleNamespace(org=SimpleNamespace(apache=SimpleNamespace(hadoop=SimpleNamespace(
            fs=SimpleNamespace(Path=lambda value:value)))))
        cp._read=lambda p:copy.deepcopy(cp.saved[p])
        return cp

    def test_one_verified_bundle_preserves_every_entry_and_legacy_resolution(self):
        cp=self.checkpoint();values=[{'source':'one'},{'source':'two'},{'source':'one'}]
        with SessionScope():
            refs=cp.archive_many(values);self.assertEqual(cp.archive.call_count,1)
            self.assertEqual([cp.read_input(r) for r in refs],values);self.assertEqual(refs[0],refs[2])
            self.assertEqual(cp.archive_many(values[:1]),refs[:1]);self.assertEqual(cp.archive.call_count,1)
        old=cp.archive({'old':True});self.assertEqual(cp.read_input(old),{'old':True})

    def test_restart_rechecks_storage_and_rejects_changed_bundle_or_entry(self):
        cp=self.checkpoint()
        with SessionScope():ref=cp.archive_many([{'a':1}])[0]
        with SessionScope():cp.archive_many([{'a':1}])
        self.assertEqual(cp.archive.call_count,2)
        base=ref.split('#')[0];cp.saved[base]['entries'].clear()
        with self.assertRaises(Conflict):cp.read_input(ref)
        with self.assertRaises(PipelineError):cp.read_input('s3a://other/checkpoint/inputs/'+'a'*64+'.json')

    def test_unconfirmed_bundle_never_publishes_entry_cache(self):
        cp=self.checkpoint();original=cp.archive.side_effect
        with SessionScope():
            cp.archive.side_effect=OSError('readback failed')
            with self.assertRaises(OSError):cp.archive_many([{'a':1}])
            cp.archive.side_effect=original
            cp.archive_many([{'a':1}]);self.assertEqual(cp.archive.call_count,2)


class BoundedReadTests(unittest.TestCase):
    def run_rows(self,rows,max_rows=2,max_bytes=8):
        from product_pipeline.bounded_read import payload_rows
        frame=MagicMock();frame.limit.return_value.withColumn.return_value.withColumn.return_value.toLocalIterator.return_value=iter(rows)
        functions=MagicMock()
        functions.col.return_value.__le__=Mock(return_value=True)
        with patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=functions)}):
            return list(payload_rows(frame,max_rows=max_rows,max_bytes=max_bytes,error_code='BUDGET',mode='stream'))

    def test_accepts_exact_bound_and_rejects_rows_bytes_or_oversize_sentinel(self):
        row={'payload':'1234','_payload_bytes':4}
        self.assertEqual(len(self.run_rows([row,row])),2)
        for rows in ([row]*3,[dict(row,_payload_bytes=9)], [{'payload':None,'_payload_bytes':9}]):
            with self.assertRaisesRegex(PipelineError,'BUDGET'):self.run_rows(rows)


class TargetMetadataTests(unittest.TestCase):
    def test_silver_and_gold_use_fresh_schema_properties_without_detail_jobs(self):
        from product_pipeline.silver.delta import DeltaSink
        from product_pipeline.gold.storage import DeltaSink as GoldSink
        from pyspark.sql.types import StructType,StructField,StringType
        settings=SimpleNamespace(silver_root='s3a://test/silver',gold_root='s3a://test/gold',fingerprint='expected')
        class SmallSink(DeltaSink):
            schema_tables={'a':None};schema_columns=staticmethod(lambda t:['id'])
        class SmallGold(GoldSink):
            schema_tables={'a':None};schema_columns=staticmethod(lambda t:['id'])
        schema=StructType([StructField('id',StringType(),True)])
        metadata=dict(schema=schema,version=4,table_id='identity',properties={'delta.appendOnly':'true','pipeline.targetFingerprint':'expected'})
        for kind in (SmallSink,SmallGold):
            spark=MagicMock();spark._jvm.org.apache.hadoop.fs.Path.side_effect=lambda p:SimpleNamespace(toString=lambda:p)
            delta=MagicMock()
            with patch.dict(sys.modules,{'delta.tables':SimpleNamespace(DeltaTable=delta)}),patch(
                    'product_pipeline.bronze.metadata.read_metadata',return_value=metadata) as read:
                sink=kind(spark,settings,lease=MagicMock())
                self.assertEqual(sink._versions,{'a':4});read.assert_called_once()
                spark.read.format.assert_not_called();delta.isDeltaTable.assert_not_called();delta.forPath.assert_not_called()
                with patch('product_pipeline.bronze.metadata.read_metadata',return_value=None):
                    with self.assertRaises(Conflict):kind(spark,settings,lease=MagicMock())


if __name__=='__main__':unittest.main()
