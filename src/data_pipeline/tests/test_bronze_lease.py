"""Lease behavior with an in-memory Mongo boundary; no service connections."""
import copy
import threading
import types
import unittest
from unittest.mock import MagicMock, patch

from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.errors import PipelineError


def settings(**changes):
    return BronzeSettings(**(dict(environment_id='test', mongo_database='source',
        bronze_path='s3a://lake/raw', events_path='s3a://lake/events',
        quarantine_path='s3a://lake/quarantine', checkpoint_path='s3a://lake/checkpoint',
        kafka_cluster_id='test', single_writer_confirmed=True) | changes))


class Collection:
    """Models only the emitted lease operations, including server expiry predicates."""
    def __init__(self):
        self.docs, self.now, self.calls = {}, 1000000, []
        self.fail = False
        self.indexes = [{'_id': '_id_', 'key': {'_id': 1}}]

    def list_indexes(self):
        return self.indexes

    def find_one_and_update(self, query, update, upsert=False, **kwargs):
        from pymongo.errors import DuplicateKeyError
        self.calls.append((copy.deepcopy(query), copy.deepcopy(update), upsert))
        if self.fail:
            raise OSError('server unavailable')
        key = query['_id']
        doc = self.docs.get(key)
        if doc is not None and (doc['token'] != query['token'] or
                ('$expr' in query and doc['expires_at'] <= self.now)):
            if upsert:
                raise DuplicateKeyError('duplicate _id')
            return None
        if doc is None and not upsert:
            return None
        values = update[0]['$set']
        doc = dict(doc or {'_id': key}, token=query['token'])
        for field, value in values.items():
            if value == '$$NOW':
                value = self.now
            elif isinstance(value, dict) and '$add' in value:
                value = self.now + value['$add'][1]
            doc[field] = value
        self.docs[key] = doc
        return copy.deepcopy(doc)

    def delete_one(self, query):
        doc = self.docs.get(query['_id'])
        if doc and doc['token'] == query['token']:
            del self.docs[query['_id']]


class LeaseTests(unittest.TestCase):
    def test_renewal_failure_records_safe_first_cause_without_sdk_message(self):
        collection=Collection();lease=self.lease(collection);lease.acquire();collection.fail=True
        with self.assertRaises(PipelineError):lease.check()
        proof=lease.loss_diagnostic
        self.assertEqual(proof['phase'],'renew');self.assertEqual(proof['exception_type'],'OSError')
        self.assertNotIn('server unavailable',str(proof));self.assertNotIn(lease.token,str(proof))
        with self.assertRaises(PipelineError):lease.check()
        self.assertEqual(lease.loss_diagnostic,proof)
    def lease(self, collection, target=None):
        from product_pipeline.bronze.lease import MongoLease
        return MongoLease(target or settings(), 'unused', collection=collection)

    def test_duplicate_driver_is_blocked_even_after_expiry_and_changed_environment(self):
        collection = Collection()
        first = self.lease(collection)
        first.acquire()
        collection.now += 1000000
        second = self.lease(collection, settings(environment_id='different'))
        with self.assertRaises(PipelineError) as found:
            second.acquire()
        self.assertEqual(found.exception.code, 'BRONZE_WRITER_ALREADY_CLAIMED')
        self.assertTrue(collection.docs)

    def test_lease_for_another_target_cannot_authorize_binding(self):
        from product_pipeline.bronze.stream import bind_checkpoint
        collection = Collection()
        lease = self.lease(collection)
        lease.acquire()
        spark = MagicMock()
        with self.assertRaises(PipelineError) as found:
            bind_checkpoint(spark, settings(environment_id='another'), lease=lease)
        self.assertEqual(found.exception.code, 'BRONZE_WRITER_TARGET_CONFLICT')
        self.assertEqual(spark.mock_calls, [])

    def test_ttl_collection_is_rejected_before_any_claim(self):
        collection = Collection()
        collection.indexes.append({'key': {'expires_at': 1}, 'expireAfterSeconds': 0})
        with self.assertRaises(PipelineError) as found:
            self.lease(collection).acquire()
        self.assertEqual(found.exception.code, 'BRONZE_LEASE_TTL_FORBIDDEN')
        self.assertFalse(collection.docs)

    def test_ambiguous_acquire_keeps_claims_for_manual_recovery(self):
        class LostResponse(Collection):
            def find_one_and_update(self, *args, **kwargs):
                result = super().find_one_and_update(*args, **kwargs)
                if len(self.docs) == 2:
                    raise OSError('write succeeded but response lost')
                return result
        collection = LostResponse()
        lease = self.lease(collection)
        with self.assertRaises(PipelineError) as found:
            lease.acquire()
        self.assertEqual(found.exception.code, 'BRONZE_WRITER_LEASE_UNCONFIRMED')
        lease.close(release=True)
        self.assertEqual(len(collection.docs), 2)

    def test_ops_client_uses_separate_database_majority_journal_and_primary(self):
        from product_pipeline.bronze.lease import MongoLease
        client, collection = MagicMock(), Collection()
        client.get_database.return_value.__getitem__.return_value = collection
        with patch('pymongo.MongoClient', return_value=client) as create:
            lease = MongoLease(settings(), 'ops-uri')
            lease.acquire()
        self.assertEqual(create.call_args.kwargs['socketTimeoutMS'],20000)
        self.assertFalse(create.call_args.kwargs['retryWrites'])
        args, options = client.get_database.call_args
        self.assertEqual(args, ('pipeline_ops',))
        self.assertEqual(options['read_concern'].document, {'level': 'majority'})
        self.assertEqual(options['write_concern'].document, {'w': 'majority', 'j': True, 'wtimeout': 15000})
        self.assertEqual(options['read_preference'].name, 'Primary')
        client.get_database.return_value.__getitem__.assert_called_once_with('bronze_writer_leases')

    def test_loss_during_delta_planning_prevents_commit(self):
        from product_pipeline.bronze.delta import DeltaSink
        from pyspark.sql.types import StructType
        collection = Collection()
        lease = self.lease(collection)
        lease.acquire()
        sink = object.__new__(DeltaSink)
        sink.settings = settings()
        sink._prepared={}
        sink.lease, sink.spark = lease, MagicMock()
        sink.paths, sink.schemas = {'raw': 'file:///localtest/raw'}, {'raw': StructType()}
        sink.existing = lambda *args: {}
        def plan(*args):
            collection.now += 1000000
            return MagicMock()
        sink.spark.range.side_effect = plan
        with patch('delta.tables.DeltaTable') as delta, \
                patch('pyspark.sql.functions.lit'), patch('pyspark.sql.functions.from_json'), \
                patch('pyspark.sql.functions.explode'), self.assertRaises(PipelineError):
            sink.insert('raw', [{'raw_document_id': 'a'}])
        delta.forPath.assert_not_called()
        sink.spark.range.return_value.select.return_value.select.return_value.write.format.assert_not_called()

    def test_partial_overlapping_targets_block_and_do_not_delete_owner(self):
        collection = Collection()
        first = self.lease(collection)
        first.acquire()
        before = copy.deepcopy(collection.docs)
        with self.assertRaises(PipelineError):
            self.lease(collection, settings(events_path='s3a://lake/other-events')).acquire()
        self.assertEqual(collection.docs, before)

    def test_release_allows_relaunch_and_stale_token_cannot_release_new_owner(self):
        collection = Collection()
        first = self.lease(collection)
        first.acquire()
        first.close(release=True)
        second = self.lease(collection)
        second.acquire()
        before = copy.deepcopy(collection.docs)
        first.close(release=True)
        self.assertEqual(collection.docs, before)
        second.check()

    def test_expiry_or_outage_is_terminal_and_never_releases_stale_latch(self):
        for outage in (False, True):
            with self.subTest(outage=outage):
                collection = Collection()
                lease = self.lease(collection)
                lease.acquire()
                if outage:
                    collection.fail = True
                else:
                    collection.now += 1000000
                with self.assertRaises(PipelineError) as found:
                    lease.check()
                self.assertEqual(found.exception.code, 'BRONZE_WRITER_LEASE_LOST')
                collection.fail = False
                collection.now = 1000000
                with self.assertRaises(PipelineError):
                    lease.check()
                lease.close(release=True)
                self.assertTrue(collection.docs)

    def test_heartbeat_uses_server_time_and_owner_expiry_predicate(self):
        collection = Collection()
        lease = self.lease(collection)
        lease.acquire()
        collection.now += 1000
        lease.check()
        query, update, _ = collection.calls[-1]
        self.assertIn('$expr', query)
        self.assertEqual(update[0]['$set']['heartbeat_at'], '$$NOW')
        self.assertEqual(update[0]['$set']['expires_at']['$add'][0], '$$NOW')

    def test_start_rejects_missing_or_lost_lease_before_target_mutation(self):
        from product_pipeline.bronze.stream import start
        for lease in (None, MagicMock()):
            if lease:
                lease.check.side_effect = PipelineError('BRONZE_WRITER_LEASE_LOST')
            with self.subTest(lease=lease), patch('product_pipeline.bronze.stream.bind_checkpoint') as bind, \
                    patch('product_pipeline.bronze.stream.DeltaSink') as sink:
                with self.assertRaises(PipelineError):
                    start(MagicMock(), settings(), object(), {'kafka.bootstrap.servers': 'local:9092'}, lease=lease)
                bind.assert_not_called()
                sink.assert_not_called()

    def test_direct_delta_or_checkpoint_without_lease_cannot_mutate(self):
        from product_pipeline.bronze.delta import DeltaSink
        from product_pipeline.bronze.stream import bind_checkpoint
        for operation in (DeltaSink, bind_checkpoint):
            spark = MagicMock()
            with self.subTest(operation=operation), self.assertRaises(PipelineError):
                operation(spark, settings())
            self.assertEqual(spark.mock_calls, [])

    def test_background_heartbeat_stops_query_on_ownership_loss(self):
        collection = Collection()
        lease = self.lease(collection, settings(lease_heartbeat_seconds=1))
        lease.acquire()
        stopped = threading.Event()
        lease.start_heartbeat(stopped.set)
        collection.fail = True
        self.assertTrue(stopped.wait(3))
        lease.close(release=True)
        self.assertTrue(collection.docs)

    def test_loss_after_sink_mutation_fails_callback_instead_of_acknowledging(self):
        from product_pipeline.bronze.stream import start
        spark, lease = MagicMock(), MagicMock()
        frame = spark.readStream.format.return_value.options.return_value.load.return_value
        reports = []
        with patch('product_pipeline.bronze.stream.bind_checkpoint'), \
                patch('product_pipeline.bronze.stream.check_kafka_cluster'), \
                patch('product_pipeline.bronze.stream.DeltaSink'), \
                patch('product_pipeline.bronze.stream.process_batch') as process:
            start(spark, settings(), object(), {'kafka.bootstrap.servers': 'local:9092'},
                  lease=lease, progress=reports.append)
            callback = frame.writeStream.foreachBatch.call_args.args[0]
            def committed(*args, **kwargs):
                lease.check.side_effect = PipelineError('BRONZE_WRITER_LEASE_LOST')
                return {'batch_id': 4}
            process.side_effect = committed
            with self.assertRaises(PipelineError):
                callback(MagicMock(), 4)
        self.assertEqual(reports[0]['status'], 'FAILED')


class CliLeaseTests(unittest.TestCase):
    def run_cli(self, *, acquire_error=None, stop_error=None):
        from product_pipeline.bronze.cli import main
        events = []
        lease, spark, source, query = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        lease.acquire.side_effect = acquire_error
        query.isActive = True
        query.stop.side_effect = lambda: events.append('query_stop')
        def stop_spark():
            events.append('spark_stop')
            if stop_error:
                raise OSError('failed to stop')
        spark.stop.side_effect = stop_spark
        lease.close.side_effect = lambda **kwargs: events.append(('close', kwargs['release']))
        builder = MagicMock()
        builder.appName.return_value = builder
        builder.config.return_value = builder
        builder.getOrCreate.return_value = spark
        fake_sql = types.ModuleType('pyspark.sql')
        fake_sql.SparkSession = types.SimpleNamespace(builder=builder)
        with patch.dict('sys.modules', {'pyspark.sql': fake_sql}), \
                patch('product_pipeline.bronze.service.BronzeSettings.load', return_value=settings()), \
                patch('product_pipeline.bronze.service.env', side_effect=lambda name:
                    '{"kafka.bootstrap.servers":"test:9092"}' if name == 'BRONZE_KAFKA_OPTIONS'
                    else '{}' if name == 'BRONZE_SPARK_OPTIONS' else 'unused'), \
                patch('product_pipeline.bronze.lease.MongoLease', return_value=lease), \
                patch('product_pipeline.bronze.mongo.MongoSource', return_value=source), \
                patch('product_pipeline.bronze.stream.start', return_value=query) as start, \
                patch('product_pipeline.bronze.cli.emit'):
            code = main(['--config', 'unused'])
        return code, events, builder, start, lease

    def test_duplicate_claim_rejected_before_spark_and_stream_start(self):
        code, _, builder, start, _ = self.run_cli(
            acquire_error=PipelineError('BRONZE_WRITER_ALREADY_CLAIMED'))
        self.assertEqual(code, 2)
        builder.getOrCreate.assert_not_called()
        start.assert_not_called()

    def test_release_occurs_only_after_query_and_spark_stopped(self):
        code, events, _, _, lease = self.run_cli()
        self.assertEqual(code, 0)
        self.assertEqual(events, ['query_stop', 'spark_stop', ('close', True)])
        lease.start_heartbeat.assert_called_once()

    def test_failed_spark_shutdown_keeps_latch_and_returns_failure(self):
        code, events, _, _, _ = self.run_cli(stop_error=True)
        self.assertEqual(code, 2)
        self.assertEqual(events[-1], ('close', False))


if __name__ == '__main__':
    unittest.main()
