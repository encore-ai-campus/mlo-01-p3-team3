import unittest

from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.bronze.mongo import MongoSource
from product_pipeline.bronze.stream import kafka_options
from product_pipeline.bronze.stream import bind_checkpoint, check_kafka_cluster
from product_pipeline.errors import PipelineError


class BronzeRuntimeTests(unittest.TestCase):
    def settings(self, **changes):
        return BronzeSettings(**({'environment_id': 'test', 'mongo_database': 'test',
            'bronze_path': 's3a://lake/bronze/raw_products', 'events_path': 's3a://lake/bronze/source_events',
            'quarantine_path': 's3a://lake/quarantine/product_raw',
            'checkpoint_path': 's3a://checks/raw-ingestion', 'single_writer_confirmed': True,
            'kafka_cluster_id': 'test-cluster'} | changes))
    def test_safe_separate_paths_and_limits(self):
        self.assertEqual(self.settings().max_records, 20)
        for changes in ({'checkpoint_path': 's3a://lake/bronze/raw_products/checkpoint'},
                        {'events_path': 's3a://lake/bronze/raw_products'},
                        {'bronze_path': 's3a://lake/bronze/../raw'},
                        {'single_writer_confirmed': False}, {'max_records': 0},
                        {'mongo_uri_env': 'uri with spaces'}):
            with self.subTest(changes=changes), self.assertRaises(PipelineError):
                self.settings(**changes)
    def test_environment_target_changes_fingerprint_not_timing(self):
        self.assertEqual(self.settings().fingerprint, self.settings(max_records=10).fingerprint)
        self.assertNotEqual(self.settings().fingerprint, self.settings(environment_id='other').fingerprint)
        self.assertNotEqual(self.settings().fingerprint, self.settings(kafka_cluster_id='other-cluster').fingerprint)
    def test_kafka_options_cannot_override_spark_offset_ownership(self):
        options = kafka_options({'kafka.bootstrap.servers': 'test:9092'}, self.settings())
        self.assertEqual(options['failOnDataLoss'], 'true')
        self.assertEqual(options['startingOffsets'], 'earliest')
        self.assertNotIn('kafka.enable.auto.commit', options)
        for key in ('startingOffsets', 'kafka.group.id', 'kafka.enable.auto.commit', 'subscribe'):
            with self.subTest(key=key), self.assertRaises(PipelineError):
                kafka_options({'kafka.bootstrap.servers': 'test:9092', key: 'bad'}, self.settings())
    def test_actual_kafka_cluster_is_verified_before_checkpoint_reuse(self):
        from unittest.mock import MagicMock
        spark = MagicMock()
        admin = spark._jvm.org.apache.kafka.clients.admin.AdminClient.create.return_value
        future = admin.describeCluster.return_value.clusterId.return_value
        future.get.return_value = 'test-cluster'
        check_kafka_cluster(spark, {'kafka.bootstrap.servers': 'a:9092'}, self.settings())
        admin.close.assert_called_once()
        future.get.return_value = 'another-cluster'
        with self.assertRaises(PipelineError) as found:
            check_kafka_cluster(spark, {'kafka.bootstrap.servers': 'b:9092'}, self.settings())
        self.assertEqual(found.exception.code, 'BRONZE_KAFKA_CLUSTER_CONFLICT')
    def test_checkpoint_identity_mismatch_or_unbound_history_is_rejected(self):
        from unittest.mock import MagicMock
        for marker_exists, stored, history, error in (
                (True, self.settings().fingerprint, False, None),
                (True, 'wrong-target', False, 'BRONZE_CHECKPOINT_TARGET_CONFLICT'),
                (False, '', True, 'BRONZE_CHECKPOINT_BINDING_MISSING'),
                (False, '', False, None)):
            with self.subTest(error=error):
                spark, path, marker = MagicMock(), MagicMock(), MagicMock()
                spark._jvm.org.apache.hadoop.fs.Path.side_effect = [path, marker]
                fs = path.getFileSystem.return_value
                fs.exists.side_effect = lambda p: marker_exists if p is marker else history
                fs.listStatus.return_value = [object()] if history else []
                fs.open.return_value.readUTF.return_value = stored
                if error:
                    with self.assertRaises(PipelineError) as found:
                        bind_checkpoint(spark, self.settings(), lease=MagicMock())
                    self.assertEqual(found.exception.code, error)
                else:
                    bind_checkpoint(spark, self.settings(), lease=MagicMock())
                    if not marker_exists:
                        fs.create.return_value.writeUTF.assert_called_once_with(self.settings().fingerprint)
    def test_initial_backlog_and_continuous_modes_share_checkpoint(self):
        from unittest.mock import MagicMock, patch
        from product_pipeline.bronze.stream import start
        for initial in (True, False):
            with self.subTest(initial=initial):
                spark = MagicMock()
                frame = spark.readStream.format.return_value.options.return_value.load.return_value
                writer = frame.writeStream.foreachBatch.return_value.option.return_value.queryName.return_value
                with patch('product_pipeline.bronze.stream.bind_checkpoint'), \
                        patch('product_pipeline.bronze.stream.check_kafka_cluster'), \
                        patch('product_pipeline.bronze.stream.DeltaSink'):
                    start(spark, self.settings(), object(), {'kafka.bootstrap.servers': 'test:9092'}, initial,
                          lease=MagicMock())
                frame.writeStream.foreachBatch.return_value.option.assert_called_with(
                    'checkpointLocation', self.settings().checkpoint_path)
                if initial:
                    writer.trigger.assert_called_once_with(availableNow=True)
                else:
                    writer.trigger.assert_called_once_with(processingTime='30 seconds')
    def test_mongo_reader_uses_primary_majority_and_never_creates_indexes(self):
        from datetime import datetime, timezone
        from bson import ObjectId
        class Admin:
            def command(self, name):
                return {'setName': 'rs-products', 'hosts': ['a', 'b', 'c']}
        class Collection:
            def find_one(self, query):
                self.query = query
                return {'_id': ObjectId('0123456789abcdef01234567'),
                        'collected_at': datetime(2026, 10, 16, tzinfo=timezone.utc)}
        class Client:
            admin = Admin()
            raw = Collection()
            def get_database(self, name, **kwargs):
                self.kwargs = kwargs
                return self
            def __getitem__(self, key):
                self.collection = key
                return self.raw
            def close(self):
                pass
        client = Client()
        source = MongoSource(self.settings(), 'unused', client=client)
        doc = source.get('0123456789abcdef01234567')
        self.assertEqual(client.collection, 'raw_products')
        self.assertEqual(client.kwargs['read_concern'].document, {'level': 'majority'})
        self.assertEqual(client.kwargs['read_preference'].name, 'Primary')
        self.assertEqual(doc['collected_at'], '2026-10-16T00:00:00.000Z')
        from product_pipeline.bronze.contracts import Poison
        for bad in ({'_id': ObjectId('0123456789abcdef01234567')},
                    {'_id': ObjectId('0123456789abcdef01234567'), 'collected_at': None}):
            client.raw.find_one = lambda query: bad
            with self.subTest(bad=bad), self.assertRaises(Poison):
                source.get('0123456789abcdef01234567')


if __name__ == '__main__':
    unittest.main()
