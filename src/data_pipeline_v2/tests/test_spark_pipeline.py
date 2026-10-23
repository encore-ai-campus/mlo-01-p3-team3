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

    def history(self, name, products):
        path = self._path(name)
        if path not in self.tables:
            return self.empty()
        return self._frame(self.tables[path]).join(products,'product_key','left_semi')

    def append_history(self, name, frame):
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

    def test_oversized_history_holds_only_that_product(self):
        self.processor.limits = BatchLimits(spark_partitions=2,history_observations=1)
        report = self.runner.work(self.batch('spark-limit',self.items+[self.old,self.new]))
        self.assertEqual(report['status'],'COMPLETED')
        counts = report['stages']['silver']['counts']
        self.assertEqual((counts['history_limit'],counts['quality_hold'],counts['valid']),(1,1,2))
        self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':2})
        self.assertEqual(self.runner.audit(report['job_id'])['status'],'AUDIT_OK')

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
