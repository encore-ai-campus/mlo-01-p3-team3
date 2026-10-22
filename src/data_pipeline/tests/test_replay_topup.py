import importlib.util,json,sys,unittest
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
import tests.test_bronze as fixtures
from test_bronze_lease import settings
from product_pipeline.files import atomic_json,read_json
from product_pipeline.event_replay import freeze
from product_pipeline.contracts import digest,now
from product_pipeline.errors import PipelineError


class TopupTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.BronzeTests();self.fixture.setUp()
        self.root=Path(self.fixture.temp.name);self.directory=self.root/'replay';s=settings()
        doc,event=self.fixture.doc,self.fixture.event
        manifest=freeze([dict(event=event,document=doc,observation_key=doc['observation_key'],raw_confirmed=True,event_confirmed=True)],
                        self.directory,run_id=doc['collection_run_id'],expected_count=1)
        rid='a'*32
        recovery=dict(contract_version=1,reason='KAFKA_RETENTION_GAP',recovery_id=rid,original_target_fingerprint=s.fingerprint,
            original_checkpoint_path=s.checkpoint_path,checkpoint_path=s.checkpoint_path+'-recovery/'+rid,
            kafka_cluster_id=s.kafka_cluster_id,kafka_topic=s.kafka_topic,kafka_topic_id='fixed-topic',
            starting_offsets={s.kafka_topic:{'0':81882}},replay_manifest_hash=manifest['content_hash'],
            replay_manifest_file=str(self.directory/'manifest.json'),created_at=now())
        recovery['content_hash']=digest(recovery)
        atomic_json(self.root/'bronze-recovery.json',recovery)
        atomic_json(self.directory/'original-bronze-config.json',asdict(s))
        atomic_json(self.root/'config/kafka-python.json',{'bootstrap.servers':'fixture','security.protocol':'SSL'})
        module_path=Path(__file__).resolve().parents[1]/'docs/server_setup/recover_kafka_retention.py'
        spec=importlib.util.spec_from_file_location('topup_fixture',module_path)
        self.job=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.job)
        self.admin=MagicMock();self.admin.list_topics.return_value=SimpleNamespace(cluster_id=s.kafka_cluster_id,
            topics={s.kafka_topic:SimpleNamespace(partitions={0:object()})})
        self.description=SimpleNamespace(topic_id='fixed-topic')
        self.admin.describe_topics.return_value={s.kafka_topic:SimpleNamespace(result=lambda timeout:self.description)}
        self.reader=MagicMock();self.reader.get_watermark_offsets.return_value=(81882,82882)
        self.s3=MagicMock();self.s3.get_paginator.return_value.paginate.return_value=[{}]
    def tearDown(self):self.fixture.tearDown()
    def run_topup(self):
        with ExitStack() as stack:
            stack.enter_context(patch.dict(sys.modules,production_work=SimpleNamespace(BASE=self.root),
                confluent_kafka=SimpleNamespace(Consumer=lambda conf:self.reader,TopicPartition=lambda *a:a,TopicCollection=lambda v:v),
                **{'confluent_kafka.admin':SimpleNamespace(AdminClient=lambda conf:self.admin),
                   'boto3':SimpleNamespace(client=lambda *a,**k:self.s3),
                   'botocore.config':SimpleNamespace(Config=lambda **k:k)}))
            stack.enter_context(patch('product_pipeline.s3_credentials.service_credentials',return_value={}))
            publisher=stack.enter_context(patch('product_pipeline.publisher.KafkaPublisher'))
            result=self.job.topup(self.root)
            publisher.assert_not_called();return result
    def test_queue_at_limit_does_not_publish_again(self):
        result=self.run_topup()
        self.assertTrue(result['pending']);self.assertEqual(result['admission_limit'],0)
        self.assertEqual(result['queued_before'],1000)
        self.reader.close.assert_called_once()
    def test_recreated_topic_blocks_before_any_source_publication(self):
        self.description.topic_id='another-topic'
        with self.assertRaisesRegex(PipelineError,'TOPIC_CHANGED'):self.run_topup()
    def test_retention_crossing_recovery_start_requires_recovery_again(self):
        self.reader.get_watermark_offsets.return_value=(82000,82882)
        with self.assertRaisesRegex(PipelineError,'OFFSETS_OUT_OF_RANGE'):self.run_topup()
    def test_generation_factory_preserves_retained_backlog_and_target_identity(self):
        from product_pipeline.bronze.recovery import make_recovery
        s=settings();plan=read_json(self.directory/'manifest.json')
        value=make_recovery(s,plan,self.directory/'manifest.json',cluster_id=s.kafka_cluster_id,topic_id='fixed-topic',frontiers={'0':(81882,82882)})
        self.assertEqual(value['starting_offsets'][s.kafka_topic]['0'],81882)
        self.assertEqual(value['original_target_fingerprint'],s.fingerprint)
    def test_create_refuses_active_driver_without_changing_active_configuration(self):
        before=(self.root/'bronze-recovery.json').read_bytes()
        with patch.dict(sys.modules,production_work=SimpleNamespace(BASE=self.root,setup=lambda:None)),patch('product_pipeline.recovery_probe.probe',return_value={'status':'BUSY'}):
            with self.assertRaisesRegex(PipelineError,'NOT_QUIESCENT'):
                self.job.main(['create','--root',str(self.root),'--directory',str(self.directory),'--run-id',self.fixture.doc['collection_run_id'],'--source-config',str(self.directory/'original-bronze-config.json')])
        self.assertEqual((self.root/'bronze-recovery.json').read_bytes(),before)
    def test_create_reuses_the_existing_generation_without_new_config_or_broker_write(self):
        before=(self.root/'bronze-recovery.json').read_bytes()
        quiet=dict(status='QUIESCENT',driver_pids=[],active_applications=[],workers=[dict(state='ALIVE',coresused=0,memoryused=0)])
        with patch.dict(sys.modules,production_work=SimpleNamespace(BASE=self.root,setup=lambda:None)),patch('product_pipeline.recovery_probe.probe',return_value=quiet):
            self.job.main(['create','--root',str(self.root),'--directory',str(self.directory),'--run-id',self.fixture.doc['collection_run_id'],'--source-config',str(self.directory/'original-bronze-config.json')])
        self.assertEqual((self.root/'bronze-recovery.json').read_bytes(),before)
