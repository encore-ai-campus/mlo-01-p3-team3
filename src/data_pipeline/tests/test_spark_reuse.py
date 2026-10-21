import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,patch

from product_pipeline.errors import PipelineError,Conflict


class SessionTests(unittest.TestCase):
    def test_failed_substage_still_records_duration(self):
        from product_pipeline.spark_session import SessionScope,timed
        with SessionScope() as scope:
            with self.assertRaises(RuntimeError):
                with timed('trial'):raise RuntimeError('trial')
            self.assertEqual(scope.metrics['trial_calls'],1)
            self.assertGreaterEqual(scope.metrics['trial_ms'],0)

    def test_one_session_across_stages_and_exactly_one_final_stop(self):
        from product_pipeline.spark_session import SessionScope,get_or_create,release_spark
        spark=MagicMock();spark.streams.active=[]
        tracker=spark.sparkContext.statusTracker.return_value
        tracker.getActiveJobsIds.return_value=[];tracker.getActiveStageIds.return_value=[]
        builder=MagicMock();builder.getOrCreate.return_value=spark
        with SessionScope() as runtime:
            self.assertIs(get_or_create(builder,{'spark.master':'spark://master:7077'}),spark)
            release_spark(spark);spark.stop.assert_not_called()
            self.assertIs(get_or_create(builder,{'spark.master':'spark://master:7077'}),spark)
            builder.getOrCreate.assert_called_once()
            self.assertEqual(runtime.metrics['spark_sessions_started'],1)
        spark.stop.assert_called_once()

    def test_different_master_or_unfinished_work_cannot_release_writer(self):
        from product_pipeline.spark_session import SessionScope,get_or_create,release_spark
        spark=MagicMock();spark.streams.active=[]
        spark.sparkContext.statusTracker.return_value.getActiveJobsIds.return_value=[12]
        b=MagicMock();b.getOrCreate.return_value=spark
        with SessionScope():
            get_or_create(b,{'spark.master':'spark://a:7077'})
            with self.assertRaises(PipelineError):get_or_create(b,{'spark.master':'spark://b:7077'})
            with patch('product_pipeline.spark_session.time.monotonic',side_effect=[0,6]):
                with self.assertRaises(PipelineError):release_spark(spark)

    def test_bounded_cache_is_private_to_the_current_driver(self):
        from product_pipeline.spark_session import SessionScope,cache_get,cache_put
        with SessionScope(max_cache_bytes=80):
            cache_put('silver',('path',1),{'rows':[1]})
            value=cache_get('silver',('path',1));value['rows'].append(2)
            self.assertEqual(cache_get('silver',('path',1)),{'rows':[1]})
            self.assertIsNone(cache_get('silver',('path',2)))
            cache_put('silver','too-large',{'data':'x'*100})
            self.assertIsNone(cache_get('silver','too-large'))
        with SessionScope():self.assertIsNone(cache_get('silver',('path',1)))

    def test_shared_session_cannot_change_node_local_classpath(self):
        from product_pipeline.spark_session import SessionScope,get_or_create
        for key in ('spark.driver.extraClassPath','spark.executor.extraClassPath'):
            with self.subTest(key=key),SessionScope():
                builder=MagicMock()
                get_or_create(builder,{key:'/opt/fixed.jar'})
                with self.assertRaisesRegex(PipelineError,'SPARK_SHARED_CONFIG_CHANGED'):
                    get_or_create(builder,{key:'/opt/changed.jar'})


class DeltaProofTests(unittest.TestCase):
    def sink(self, current=5):
        from product_pipeline.silver.delta import DeltaSink
        sink=DeltaSink.__new__(DeltaSink)
        sink.lease=SimpleNamespace(check=lambda *a:None);sink.settings=object()
        sink.version=MagicMock(return_value=current)
        sink._read=MagicMock(return_value={'id':{'record_hash':'h'}})
        return sink

    def test_same_fixed_and_current_version_is_read_only_once(self):
        sink=self.sink();sink.verify('t',{'id':'h'},5)
        self.assertEqual(sink._read.call_count,1)
        sink.verify('t',{'id':'h'},5)
        self.assertEqual(sink._read.call_count,1)

    def test_old_version_and_changed_current_are_both_verified(self):
        sink=self.sink(current=6);sink.verify('t',{'id':'h'},5)
        self.assertEqual(sink._read.call_count,2)
        sink._read.return_value={'id':{'record_hash':'different'}}
        sink.version.return_value=7
        with self.assertRaises(Conflict):sink.verify('t',{'id':'h'},5)


class ArchiveTests(unittest.TestCase):
    def test_verified_archive_is_reused_only_inside_one_driver_and_not_after_restart(self):
        from product_pipeline.silver.checkpoint import Checkpoint
        from product_pipeline.spark_session import SessionScope
        checkpoint=Checkpoint.__new__(Checkpoint)
        checkpoint.fs=SimpleNamespace(mkdirs=MagicMock(),exists=MagicMock(return_value=True))
        checkpoint._read=MagicMock(return_value={'v':1})
        checkpoint._path=lambda name:SimpleNamespace(getParent=lambda:'inputs',toString=lambda:'s3a://bucket/'+name)
        with SessionScope():
            first=checkpoint.archive({'v':1});second=checkpoint.archive({'v':1})
            self.assertEqual(first,second);self.assertEqual(checkpoint._read.call_count,1)
            self.assertEqual(checkpoint.fs.exists.call_count,1)
        checkpoint.archive({'v':1})
        self.assertEqual(checkpoint._read.call_count,2)
        checkpoint.fs.mkdirs.assert_called_once()

    def test_existing_content_addressed_input_is_not_read_twice(self):
        from product_pipeline.silver.checkpoint import Checkpoint
        from product_pipeline.contracts import digest
        checkpoint=Checkpoint.__new__(Checkpoint);checkpoint._path=lambda name:name
        checkpoint.fs=SimpleNamespace(mkdirs=lambda *a:None,exists=lambda *a:True)
        checkpoint._read=MagicMock(return_value={'v':1})
        checkpoint._path=lambda name:SimpleNamespace(getParent=lambda:'inputs',toString=lambda:'s3a://bucket/'+name)
        ref=checkpoint.archive({'v':1})
        self.assertIn(digest({'v':1}),ref);self.assertEqual(checkpoint._read.call_count,1)


if __name__=='__main__':unittest.main()
