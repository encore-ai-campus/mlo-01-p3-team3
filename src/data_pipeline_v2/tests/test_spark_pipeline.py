"""Opt-in real Spark run of the finite batch processor; Delta storage is an in-memory stand-in.

The DataFrame logic (Kafka range validation, Executor source resolution, Silver joins,
native Gold, holds, retraction and withdrawal) runs on a local Spark JVM. Delta commits,
time travel and txnAppId idempotence are NOT exercised here and need a real Delta runtime.
"""
import copy
import hashlib
import os
import sys
import tempfile
from pathlib import Path
import unittest

import test_engine as fixtures
from pipeline_v2.domain.batch import BatchLimits
from pipeline_v2.domain.contracts import seal, digest, verified, Invalid
from pipeline_v2.adapters.batch_delta import BatchDelta
from pipeline_v2.adapters.batch_store import FileBatchStore
from pipeline_v2.adapters.kafka_batch import FileBroker
from pipeline_v2.adapters.publication import FilePublisher
from pipeline_v2.adapters.spark_batch import SparkBatchProcessor
from pipeline_v2.adapters.timing import Timing
from pipeline_v2.application.batch import BatchRunner
from pipeline_v2.application.outbox import Outbox


class MemoryDelta(BatchDelta):
    """Keeps encoded rows per path; reuses BatchDelta encoding, hashing and conflict rules."""
    SCHEMA = 'row_key string, product_key string, payload_json string, record_hash string, bucket int'

    def __init__(self, spark):
        super().__init__(spark,'memory://lake',lambda:None,None)
        self.tables, self.commits = {}, 0

    def _exists(self, path):
        return path in self.tables

    def _frame(self, rows):
        return self.spark.createDataFrame(rows,self.SCHEMA)

    def history(self, name, products, shared=None):
        path = self._path(name)
        if path not in self.tables:
            return self.empty()
        return self._frame(self.tables[path]).join(products,'product_key','left_semi')

    def append_history(self, name, frame, unique=None):
        path = self._path(name)
        rows = {r[0]:tuple(r) for r in self.encode(frame).collect()}
        old = {r[0]:r for r in self.tables.get(path,[])}
        # Same key with a different record hash is a conflict, never an overwrite.
        if any(k in old and old[k][3] != v[3] for k,v in rows.items()):
            raise Invalid('DELTA_IMMUTABLE_CONFLICT')
        fresh = [v for k,v in rows.items() if k not in old]
        if fresh:
            self.tables[path] = list(old.values()) + fresh
            self.commits += 1

    def append_raw(self, job_id, frame, first):
        stored = self.tables.setdefault(self._path('input_raw'),[])
        known = {r[0]:r for r in stored}
        for r in frame.collect():
            value = (r['row_key'],r['product_key'],r['payload_json'],job_id)
            if r['row_key'] in known and known[r['row_key']] != value:
                raise Invalid('DELTA_IMMUTABLE_CONFLICT')
            if r['row_key'] not in known:
                stored.append(value)

    def raw(self, jobs, keys=None):
        rows = [r[:3] for r in self.tables.get(self._path('input_raw'),[]) if r[3] in set(jobs) and (keys is None or r[0] in keys)]
        return self.spark.createDataFrame(rows,'row_key string,product_key string,payload_json string')

    def snapshot(self, job_id, stage, frame):
        path = self.root + '/runs/' + job_id + '/' + stage
        rows = [tuple(r) for r in self.encode(frame).collect()]
        if path in self.tables and sorted(self.tables[path]) != sorted(rows):
            raise Invalid('DELTA_STAGE_RETRY_CONFLICT')
        if path not in self.tables:
            self.tables[path] = rows
            self.commits += 1
        return seal(dict(kind='DELTA_BATCH_SNAPSHOT',path=path,version=0,count=len(rows),target_hash=digest(self.root)))

    def load(self, proof):
        verified(proof)
        return self._frame(self.tables[proof['path']])

    def metadata(self, name, job_id):
        from pyspark.sql import functions as F
        path = self._path(name)
        if path not in self.tables:
            return self.empty().select('row_key','product_key','payload_json')
        return self._frame(self.tables[path]).filter(
            F.get_json_object('payload_json','$.origin_job') == job_id).select('row_key','product_key','payload_json')

    def purge(self, name, expires_before):
        import json
        path = self._path(name)
        before = self.tables.get(path,[])
        self.tables[path] = [r for r in before if json.loads(r[2])['metadata_expires_at'] > expires_before]
        return len(before) - len(self.tables[path])


@unittest.skipUnless(os.environ.get('PIPELINE_V2_TEST_SPARK') == '1','set PIPELINE_V2_TEST_SPARK=1 for a real local Spark JVM')
class SparkFinitePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.temp = tempfile.TemporaryDirectory()
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        # Python workers import the processor modules for mapPartitions and UDFs.
        paths = os.pathsep.join([str(fixtures.HERE),str(fixtures.LEGACY)])
        cls.spark = (SparkSession.builder.master('local[2]').appName('pipeline-v2-finite-batch-test')
            .config('spark.executorEnv.PYTHONPATH',paths)
            .config('spark.ui.enabled','false').config('spark.driver.host','127.0.0.1')
            .config('spark.driver.bindAddress','127.0.0.1').config('spark.sql.shuffle.partitions','2')
            .config('spark.sql.warehouse.dir',(Path(cls.temp.name)/'warehouse').as_uri())
            .config('spark.local.dir',str(Path(cls.temp.name)/'scratch')).getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def setUp(self):
        self.case = tempfile.TemporaryDirectory()
        root = self.root = Path(self.case.name)
        # Product 11 comes from the two-observation history below; keep 12 and 13 here.
        self.items = fixtures.inputs(root,products=3)[1:]
        history = root/'history'
        history.mkdir()
        self.old, self.new = fixtures.inputs(history,products=1,observations=2)
        timing = Timing(root/'state')
        store = FileBatchStore(root/'state')
        self.broker = FileBroker(root/'broker')
        limits = BatchLimits(spark_partitions=2)
        self.processor = SparkBatchProcessor(store,None,dict(mode='local',state_root=str(root/'state')),{},timing,limits,fixtures.POLICY)
        self.processor.spark, self.processor.delta = self.spark, MemoryDelta(self.spark)
        self.processor.read_kafka = self.read_kafka
        self.runner = BatchRunner(store,self.processor,FilePublisher(root/'sql'),timing,limits,fixtures.POLICY,dict(target='spark-test'))
        self.outbox = Outbox(store,self.broker,timing,limits,dict(storage_allowed=True,quarantine_allowed=True))

    def tearDown(self):
        self.case.cleanup()

    def read_kafka(self, spark, config, manifest, partitions):
        # Bounded rows from the durable local broker, exactly the manifest offset ranges.
        rows = [(r['topic'],r['partition'],r['offset'],r['key'],r['value']) for r in self.broker.rows(manifest)]
        return spark.createDataFrame(rows,'topic string, partition int, offset long, key string, value string')

    def batch(self, name, items):
        crawl = dict(schema_version=1,crawl_batch_id=name,status='SOURCE_COMPLETE',required_sources=['danawa'],
            completed_sources=['danawa'],event_count=len(items),input_sha256=hashlib.sha256(name.encode()).hexdigest(),
            started_at='2026-10-16T00:00:00.000Z',completed_at='2026-10-16T01:00:00.000Z',input_semantics='OBSERVATION_SNAPSHOT')
        self.outbox.prepare(crawl,items)
        self.outbox.relay(name)
        return self.runner.submit(name)['job_id']

    def test_spark_batch_isolates_bad_rows_reuses_normalization_and_withdraws(self):
        bad = copy.deepcopy(self.items[0])
        bad['document']['source_record_key'] = 'tampered-without-reseal'
        first = self.batch('spark-first',self.items+[bad,self.old,self.new])
        report = self.runner.work(first)
        self.assertEqual(report['status'],'COMPLETED')
        self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':3})
        self.assertEqual((report['stages']['bronze']['counts']['accepted'],report['stages']['bronze']['counts']['input_quarantine']),(4,1))
        self.assertEqual(report['stages']['silver']['counts']['valid'],4)
        self.assertEqual(self.runner.audit(first)['status'],'AUDIT_OK')
        normalized = len(self.processor.delta.tables[self.processor.delta._path('normalized_history')])

        # Same observations in a new crawl batch: reuse is decided before any source body is parsed.
        again = self.runner.work(self.batch('spark-again',self.items))
        self.assertEqual(again['status'],'COMPLETED')
        self.assertEqual(len(self.processor.delta.tables[self.processor.delta._path('normalized_history')]),normalized)

        product = 'danawa:11'
        for item in (self.old,self.new):
            self.runner.retract(dict(product_key=product,raw_document_id=item['document']['_id'],reason='SOURCE_WITHDRAWN'))
        repaired = self.runner.work(self.runner.repair([product])['job_id'])
        self.assertEqual(repaired['stages']['publication']['resolutions'],{'WITHDRAWN':1})
        self.assertEqual(repaired['stages']['gold']['counts']['source_retracted'],1)

        purged = self.runner.retention_purge('2027-01-15T01:00:00.000Z')
        self.assertEqual((purged['quarantine_raw_deleted'],purged['metadata_deleted']),(1,1))

    def test_a_product_whose_normalization_is_not_reusable_reads_its_older_raw(self):
        first = self.runner.work(self.batch('spark-raw',self.items+[self.old,self.new]))
        self.assertEqual(first['status'],'COMPLETED')
        delta = self.processor.delta
        path = delta._path('normalized_history')
        # As after a policy change: no stored normalization matches the product's identities.
        delta.tables[path] = [r for r in delta.tables[path] if r[1] != 'danawa:11']
        self.assertNotIn('item',delta.tables[delta._path('input_index')][0][2])
        repaired = self.runner.work(self.runner.repair(['danawa:11'])['job_id'])
        self.assertEqual(repaired['status'],'COMPLETED')
        # A repair has no raw of its own: both observations were parsed again from input_raw.
        self.assertEqual(len([r for r in delta.tables[path] if r[1] == 'danawa:11']),2)
        self.assertEqual(repaired['stages']['publication']['resolutions'],{'PUBLISHED':1})

    def test_a_failed_raw_commit_keeps_the_job_open_after_publication_and_a_retry_completes_it(self):
        from unittest.mock import patch
        delta = self.processor.delta
        def broken(self, job_id, frame, first):
            raise Invalid('RAW_COMMIT_FAILED')
        with patch.object(type(delta),'append_raw',broken):
            job = self.batch('spark-raw-retry',self.items)
            with self.assertRaisesRegex(Invalid,'RAW_COMMIT_FAILED'):
                self.runner.work(job)
        # Publication does not need the raw, so it finished; the job is not closed without it.
        self.assertTrue(self.processor.store.get('stage',[job,'gold']))
        self.assertTrue(self.processor.store.get('stage',[job,'publication']))
        self.assertEqual(self.processor.store.get('job',job)['status'],'RECOVERY_REQUIRED')
        self.assertFalse(delta.tables.get(delta._path('input_raw')))
        self.runner.recover(job,'test process stopped')
        report = self.runner.work(job)
        self.assertEqual(report['status'],'COMPLETED')
        # The retry read the fixed range again and stored exactly the accepted raw.
        self.assertEqual(len(delta.tables[delta._path('input_raw')]),report['stages']['bronze']['counts']['accepted'])

    def test_oversized_history_holds_only_that_product(self):
        self.processor.limits = BatchLimits(spark_partitions=2,history_observations=1)
        report = self.runner.work(self.batch('spark-limit',self.items+[self.old,self.new]))
        self.assertEqual(report['status'],'COMPLETED')
        counts = report['stages']['silver']['counts']
        self.assertEqual((counts['history_limit'],counts['quality_hold'],counts['valid']),(1,1,2))
        self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':2})
        self.assertEqual(self.runner.audit(report['job_id'])['status'],'AUDIT_OK')

    def test_kafka_range_validation_flags_each_violation_in_one_aggregate(self):
        import json
        from pyspark.sql import functions as F
        schema = 'crawl_batch_id string, event_id string, payload_hash string, product_key string'
        def body(event, batch='b1', product='danawa:1', payload='h1', extra=''):
            return json.dumps(dict(crawl_batch_id=batch,event_id=event,payload_hash=payload,product_key=product,x=extra))
        def flags(records, expected):
            decoded = self.spark.createDataFrame(records,'key string, value string').withColumn('e',F.from_json('value',schema))
            frame = self.spark.createDataFrame(expected,'_expected_id string,_expected_hash string,_expected_product string')
            row = SparkBatchProcessor.validate_range(decoded,'b1',frame)
            return {k:row[k] for k in ('unclassifiable','key_mismatch','conflict','mismatch')}
        clean = {'unclassifiable':0,'key_mismatch':0,'conflict':0,'mismatch':0}
        expected = [('e1','h1','danawa:1'),('e2','h2','danawa:2')]
        good = [('danawa:1',body('e1')),('danawa:2',body('e2',product='danawa:2',payload='h2'))]
        # Another batch interleaved in the range and an exact redelivery are both fine.
        self.assertEqual(flags(good+[good[0],('danawa:9',body('o1',batch='b0',product='danawa:9'))],expected),clean)
        self.assertEqual(flags(good+[('x','not json')],expected),dict(clean,unclassifiable=1))
        self.assertEqual(flags([('danawa:7',body('e1'))]+good[1:],expected),dict(clean,key_mismatch=1))
        self.assertEqual(flags(good+[('danawa:1',body('e1',extra='changed'))],expected),dict(clean,conflict=1))
        self.assertEqual(flags(good[:1],expected),dict(clean,mismatch=1))
        self.assertEqual(flags(good+[('danawa:3',body('e3',product='danawa:3'))],expected),dict(clean,mismatch=1))
        self.assertEqual(flags([('danawa:1',body('e1',payload='other'))]+good[1:],expected),dict(clean,mismatch=1))
        self.assertEqual(flags([],expected),dict(clean,mismatch=1))
        self.assertEqual(flags([],[]),clean)

    def test_history_append_reads_only_touched_buckets_and_keeps_rows_immutable(self):
        delta = self.processor.delta
        def encoded(rows):
            return delta.encode(self.spark.createDataFrame(rows,'row_key string,product_key string,payload_json string'))
        stored = encoded([('k1','danawa:11','{"a":1}'),('k2','danawa:12','{"a":2}')])
        incoming = encoded([('k1','danawa:11','{"a":1}'),('k3','danawa:13','{"a":3}')])
        joined = delta.join_existing(incoming,stored)
        self.assertFalse(delta.has_conflict(joined))
        self.assertEqual([r['row_key'] for r in delta.new_rows(joined).collect()],['k3'])
        changed = encoded([('k1','danawa:11','{"a":999}')])
        self.assertTrue(delta.has_conflict(delta.join_existing(changed,stored)))


if __name__ == '__main__':
    unittest.main()
