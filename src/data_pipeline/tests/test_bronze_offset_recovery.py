from dataclasses import replace
import copy,tempfile,unittest
from pathlib import Path
from unittest.mock import MagicMock
from test_bronze_lease import settings
from product_pipeline.contracts import digest,now
from product_pipeline.errors import PipelineError,Conflict
from product_pipeline.bronze.recovery import recovery,checkpoint_path,assert_offsets,checkpoint_offsets
from product_pipeline.bronze.stream import kafka_options
from product_pipeline.bronze.lease import MongoLease,RECOVERY_ERRORS


def recovered():
    base=settings();rid='a'*32
    v=dict(contract_version=1,reason='KAFKA_RETENTION_GAP',recovery_id=rid,
        original_target_fingerprint=base.fingerprint,original_checkpoint_path=base.checkpoint_path,
        checkpoint_path=base.checkpoint_path+'-recovery/'+rid,kafka_cluster_id=base.kafka_cluster_id,
        kafka_topic=base.kafka_topic,kafka_topic_id='fixed-topic',starting_offsets={base.kafka_topic:{'0':81882}},
        replay_manifest_hash='b'*64,replay_manifest_file=str(Path(tempfile.gettempdir()).resolve()/'manifest.json'),created_at=now())
    v['content_hash']=digest(v)
    return replace(base,checkpoint_recovery=v)


class RecoveryTests(unittest.TestCase):
    def test_generation_keeps_original_table_identity_but_gets_a_distinct_path(self):
        s=recovered();self.assertEqual(s.fingerprint,settings().fingerprint)
        self.assertNotEqual(checkpoint_path(s),s.checkpoint_path)
        lease=MongoLease(s,'unused',collection=MagicMock())
        self.assertIn(s.checkpoint_path,lease._targets);self.assertIn(checkpoint_path(s),lease._targets)
        self.assertEqual(len(lease._targets),5)
        opts=kafka_options({'kafka.bootstrap.servers':'host:9092'},s)
        self.assertIn('81882',opts['startingOffsets']);self.assertEqual(opts['failOnDataLoss'],'true')
    def test_changed_target_or_manifest_cannot_silently_redirect_checkpoint(self):
        s=recovered()
        for key,value in [('checkpoint_path',s.checkpoint_path),('original_target_fingerprint','bad'),('kafka_cluster_id','other')]:
            v=copy.deepcopy(s.checkpoint_recovery);v[key]=value;v['content_hash']=digest({k:x for k,x in v.items() if k!='content_hash'})
            with self.assertRaises(Conflict):replace(s,checkpoint_recovery=v)
    def test_gap_and_truncated_end_are_recovery_required_not_transient_retries(self):
        topic=settings().kafka_topic
        for pos in (310,82000):
            with self.assertRaises(PipelineError) as found:assert_offsets({topic:{'0':pos}},{'0':(81882,81982)},topic)
            self.assertIn(found.exception.code,RECOVERY_ERRORS)
        assert_offsets({topic:{'0':81882}},{'0':(81882,81882)},topic)
    def test_partition_change_blocks(self):
        topic=settings().kafka_topic
        with self.assertRaises(PipelineError):assert_offsets({topic:{'0':1}},{'0':(0,10),'1':(0,0)},topic)
    def test_preflight_uses_last_committed_batch_not_uncommitted_end(self):
        spark=MagicMock();PathClass=spark._jvm.org.apache.hadoop.fs.Path
        fs=PathClass.return_value.getFileSystem.return_value
        fs.exists.return_value=True
        fs.listStatus.return_value=[MagicMock(),MagicMock()]
        for n,entry in zip(('2','3'),fs.listStatus.return_value):entry.getPath.return_value.getName.return_value=n
        spark._jvm.org.apache.commons.io.IOUtils.toString.return_value='v1\n{}\n{"product.raw.collected":{"0":310}}'
        self.assertEqual(checkpoint_offsets(spark,settings(),{'0':(0,81882)}),{'product.raw.collected':{'0':310}})
        self.assertEqual(PathClass.call_args.args[1],'offsets/3')
