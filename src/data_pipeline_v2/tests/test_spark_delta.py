"""Opt-in real Delta run of the finite batch processor on a local directory.

Unlike test_spark_pipeline.py (MemoryDelta), BatchDelta itself writes here: partition layout,
history(1) proofs, txnAppId snapshots, bucket-pruned history appends and their conflict checks.
Needs delta-spark matching the server (4.4.1 on Spark 4.1.3) and, on Windows, HADOOP_HOME.
Set PIPELINE_V2_DELTA_REPORT=<file> to write job, file and content counts for before/after comparison.
"""
import json
import os
import sys
import tempfile
from pathlib import Path
import unittest

import test_engine as fixtures
from pipeline_v2.domain.batch import BatchLimits
from pipeline_v2.domain.contracts import digest, Invalid
from pipeline_v2.adapters.batch_delta import BatchDelta
from pipeline_v2.adapters.batch_store import FileBatchStore
from pipeline_v2.adapters.kafka_batch import FileBroker
from pipeline_v2.adapters.publication import FilePublisher
from pipeline_v2.adapters.spark_batch import SparkBatchProcessor
from pipeline_v2.adapters.timing import Timing
from pipeline_v2.application.batch import BatchRunner
from pipeline_v2.application.outbox import Outbox
# Module import, not the class: a TestCase name in this namespace would be collected and run again here.
import test_spark_pipeline as memory_delta


def jobs_so_far(spark):
    """Highest Spark job id so far, read with one tagged probe job (the probe itself is excluded)."""
    context = spark.sparkContext
    context.setJobGroup('pipeline-v2-test-probe','job counter')
    try:
        context.parallelize([0],1).count()
    finally:
        context.setLocalProperty('spark.jobGroup.id',None)
    return max(context.statusTracker().getJobIdsForGroup('pipeline-v2-test-probe'))


@unittest.skipUnless(os.environ.get('PIPELINE_V2_TEST_DELTA') == '1','set PIPELINE_V2_TEST_DELTA=1 with delta-spark installed')
class RealDeltaBatchTests(unittest.TestCase):
    report = {}

    @classmethod
    def setUpClass(cls):
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        # Delta jars join the classpath only when this process starts its JVM; another Spark test
        # module in the same run starts it first without them.
        if SparkContext._gateway is not None:
            raise unittest.SkipTest('JVM already started without Delta: run -p "test_spark_delta.py" in its own process')
        # Windows keeps Spark's fetched jars open until the JVM exits.
        cls.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        paths = os.pathsep.join([str(fixtures.HERE),str(fixtures.LEGACY)])
        builder = (SparkSession.builder.master('local[2]').appName('pipeline-v2-real-delta-test')
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.executorEnv.PYTHONPATH',paths)
            .config('spark.ui.enabled','false').config('spark.driver.host','127.0.0.1')
            .config('spark.driver.bindAddress','127.0.0.1').config('spark.sql.shuffle.partitions','2')
            .config('spark.sql.warehouse.dir',(Path(cls.temp.name)/'warehouse').as_uri())
            .config('spark.local.dir',str(Path(cls.temp.name)/'scratch')))
        # Optional event log: the report then counts jobs by their pipeline.v2.stage label.
        cls.events = os.environ.get('PIPELINE_V2_DELTA_EVENTS')
        if cls.events:
            Path(cls.events).mkdir(parents=True,exist_ok=True)
            # Plain single-file JSON lines, so tearDownClass can read it without a codec.
            builder = (builder.config('spark.eventLog.enabled','true').config('spark.eventLog.dir',Path(cls.events).as_uri())
                .config('spark.eventLog.compress','false').config('spark.eventLog.rolling.enabled','false'))
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()
        if cls.events:
            from collections import Counter
            stages = Counter()
            for log in (p for p in Path(cls.events).iterdir() if p.is_file() and not p.name.startswith('.')):
                for line in log.read_text(encoding='utf-8').splitlines():
                    event = json.loads(line)
                    if event.get('Event') == 'SparkListenerJobStart':
                        group = (event.get('Properties') or {}).get('spark.jobGroup.id')
                        if group != 'pipeline-v2-test-probe':
                            stages[(event.get('Properties') or {}).get('pipeline.v2.stage','unlabelled')] += 1
            cls.report['jobs_by_stage'] = dict(sorted(stages.items()))
        target = os.environ.get('PIPELINE_V2_DELTA_REPORT')
        if target and cls.report:
            Path(target).write_text(json.dumps(cls.report,indent=1,sort_keys=True),encoding='utf-8')

    def setUp(self):
        self.case = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = self.root = Path(self.case.name)
        self.lake = (root/'lake').as_uri()
        timing = Timing(root/'state')
        store = FileBatchStore(root/'state')
        self.broker = FileBroker(root/'broker')
        limits = BatchLimits(spark_partitions=2)
        self.processor = SparkBatchProcessor(store,None,dict(mode='local',state_root=str(root/'state')),{},timing,limits,fixtures.POLICY)
        self.delta = BatchDelta(self.spark,self.lake,lambda:None,timing)
        self.processor.spark, self.processor.delta = self.spark, self.delta
        self.processor.read_kafka = self.read_kafka
        self.runner = BatchRunner(store,self.processor,FilePublisher(root/'sql'),timing,limits,fixtures.POLICY,dict(target='delta-test'))
        self.outbox = Outbox(store,self.broker,timing,limits,dict(storage_allowed=True,quarantine_allowed=True))

    def tearDown(self):
        self.case.cleanup()

    read_kafka = memory_delta.SparkFinitePipelineTests.read_kafka
    batch = memory_delta.SparkFinitePipelineTests.batch

    # Helpers --------------------------------------------------------------------------------

    def files(self, path):
        """Data files (not the Delta log) under a table directory."""
        local = Path(path.replace('file:///','').replace('file:',''))
        return sum(1 for p in local.rglob('*.parquet') if '_delta_log' not in p.parts)

    def version(self, path):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,path).history(1).select('version').first()[0])

    def rows(self, frame):
        return sorted(tuple(r) for r in frame.select(*BatchDelta.encoded_columns).collect())

    def encoded(self, rows):
        return self.spark.createDataFrame(rows,'row_key string,product_key string,payload_json string')

    # Scenarios -------------------------------------------------------------------------------

    def test_clean_batch_counts_jobs_files_and_stays_idempotent(self):
        (self.root/'source').mkdir()
        items = fixtures.inputs(self.root/'source',products=20)
        name = 'delta-clean'
        job = self.batch(name,items)
        before = jobs_so_far(self.spark)
        processed = self.runner.work(job,phase='process')
        after_process = jobs_so_far(self.spark)
        self.assertEqual(processed['stages']['gold']['counts']['products'],20)
        published = self.runner.work(job,phase='publish')
        after_publish = jobs_so_far(self.spark)
        self.assertEqual(published['status'],'COMPLETED')
        self.assertEqual(published['stages']['publication']['resolutions'],{'PUBLISHED':20})
        self.assertEqual(self.runner.audit(job)['status'],'AUDIT_OK')

        stages = {name:self.processor.store.get('stage',[job,name]) for name in ('bronze','silver','gold')}
        snapshots = {}
        for stage, saved in stages.items():
            for label, proof in (saved.get('outputs') or ({'output':saved['output']} if 'output' in saved else {})).items():
                if not isinstance(proof,dict) or proof.get('kind') != 'DELTA_BATCH_SNAPSHOT':
                    continue
                rows = self.rows(self.delta.load(proof))
                self.assertEqual(len(rows),proof['count'])
                snapshots[stage+'.'+label] = dict(rows=len(rows),files=self.files(proof['path']),
                    # Parsed content without the job-scoped row key, so separate runs and JSON key order compare.
                    content=digest(sorted([r[1],json.loads(r[2])] for r in rows)))
        history = {}
        for table in ('input_index','normalized_history'):
            path = self.delta._path(table)
            history[table] = dict(files=self.files(path),version=self.version(path),
                rows=self.spark.read.format('delta').load(path).count())

        # A new crawl batch with the same observations reuses Silver and appends nothing new.
        normalized = history['normalized_history']['version']
        again = self.runner.work(self.batch('delta-again',items))
        self.assertEqual(again['status'],'COMPLETED')
        self.assertEqual(self.version(self.delta._path('normalized_history')),normalized)

        type(self).report['clean_20_products'] = dict(process_jobs=after_process-before-1,
            publish_jobs=after_publish-after_process-1,snapshots=snapshots,history=history,
            counts={k:v['counts'] for k,v in stages.items()})

    def test_bad_rows_retraction_and_withdrawal_with_real_delta(self):
        (self.root/'source').mkdir()
        items = fixtures.inputs(self.root/'source',products=3)[1:]
        (self.root/'history').mkdir()
        old, new = fixtures.inputs(self.root/'history',products=1,observations=2)
        import copy
        bad = copy.deepcopy(items[0])
        bad['document']['source_record_key'] = 'tampered-without-reseal'
        first = self.batch('delta-first',items+[bad,old,new])
        report = self.runner.work(first)
        self.assertEqual(report['status'],'COMPLETED')
        self.assertEqual((report['stages']['bronze']['counts']['accepted'],report['stages']['bronze']['counts']['input_quarantine']),(4,1))
        self.assertEqual(report['stages']['silver']['counts']['valid'],4)
        self.assertEqual(self.runner.audit(first)['status'],'AUDIT_OK')
        product = 'danawa:11'
        for item in (old,new):
            self.runner.retract(dict(product_key=product,raw_document_id=item['document']['_id'],reason='SOURCE_WITHDRAWN'))
        repaired = self.runner.work(self.runner.repair([product])['job_id'])
        self.assertEqual(repaired['stages']['publication']['resolutions'],{'WITHDRAWN':1})
        self.assertEqual(repaired['stages']['gold']['counts']['source_retracted'],1)

    def test_oversized_history_holds_only_that_product(self):
        (self.root/'source').mkdir()
        items = fixtures.inputs(self.root/'source',products=3)[1:]
        (self.root/'history').mkdir()
        old, new = fixtures.inputs(self.root/'history',products=1,observations=2)
        self.processor.limits = BatchLimits(spark_partitions=2,history_observations=1)
        report = self.runner.work(self.batch('delta-limit',items+[old,new]))
        counts = report['stages']['silver']['counts']
        self.assertEqual((counts['history_limit'],counts['quality_hold'],counts['valid']),(1,1,2))
        self.assertEqual(self.runner.audit(report['job_id'])['status'],'AUDIT_OK')

    def test_history_append_conflicts_keep_their_order_and_never_write(self):
        table = 'conflict_probe'
        path = self.delta._path(table)
        # Empty input creates no table.
        self.delta.append_history(table,self.encoded([]))
        self.assertFalse(self.delta._exists(path))
        self.delta.append_history(table,self.encoded([('k1','danawa:11','{"a":1}'),('k2','danawa:12','{"a":2}')]))
        stored = self.version(path)
        # Identical rows again: no new commit.
        self.delta.append_history(table,self.encoded([('k1','danawa:11','{"a":1}')]))
        self.assertEqual(self.version(path),stored)
        # Mixed: one existing identical, one new.
        self.delta.append_history(table,self.encoded([('k2','danawa:12','{"a":2}'),('k3','danawa:13','{"a":3}')]))
        self.assertEqual(self.version(path),stored+1)
        self.assertEqual(sorted(r[0] for r in self.spark.read.format('delta').load(path).select('row_key').collect()),['k1','k2','k3'])
        current = self.version(path)
        with self.assertRaisesRegex(Invalid,'DELTA_IMMUTABLE_CONFLICT'):
            self.delta.append_history(table,self.encoded([('k1','danawa:11','{"a":999}')]))
        with self.assertRaisesRegex(Invalid,'BATCH_OUTPUT_KEY_CONFLICT'):
            self.delta.append_history(table,self.encoded([('k9','danawa:19','{"a":1}'),('k9','danawa:19','{"a":2}')]))
        # Input conflict and stored conflict together: the input conflict is reported first.
        with self.assertRaisesRegex(Invalid,'BATCH_OUTPUT_KEY_CONFLICT'):
            self.delta.append_history(table,self.encoded([('k1','danawa:11','{"a":5}'),('k1','danawa:11','{"a":6}')]))
        self.assertEqual(self.version(path),current)

    @unittest.expectedFailure
    def test_history_append_detects_same_key_with_a_different_product(self):
        # Known gap: bucket comes from product_key and the stored-row join is on (bucket, row_key), so a
        # same row_key under another product lands in another bucket and is appended, not rejected.
        # Row keys derive from event ids that fix the product, so real inputs do not produce this.
        table = 'owner_probe'
        self.delta.append_history(table,self.encoded([('k1','danawa:11','{"a":1}')]))
        with self.assertRaisesRegex(Invalid,'DELTA_IMMUTABLE_CONFLICT'):
            self.delta.append_history(table,self.encoded([('k1','danawa:99','{"a":1}')]))

    def test_snapshot_retry_is_idempotent_and_detects_changed_content(self):
        frame = self.encoded([('k1','danawa:11','{"a":1}'),('k2','danawa:12','{"a":2}')])
        proof = self.delta.snapshot('job-1','probe',frame)
        again = self.delta.snapshot('job-1','probe',frame)
        self.assertEqual((again['version'],again['count']),(proof['version'],2))
        with self.assertRaisesRegex(Invalid,'DELTA_STAGE_RETRY_CONFLICT'):
            self.delta.snapshot('job-1','probe',self.encoded([('k1','danawa:11','{"a":1}')]))
        self.delta.audit(proof)

    def test_history_writes_one_file_per_bucket_and_compaction_keeps_rows(self):
        path = self.delta._path('input_index')
        rows = [('k%d' % i,'danawa:%d' % (i % 5),'{"a":%d}' % i) for i in range(40)]
        for start in range(0,40,10):
            self.delta.append_history('input_index',self.encoded(rows[start:start+10]))
        buckets = len({r[4] for r in self.rows(self.spark.read.format('delta').load(path))})
        before = self.files(path)
        # Four appends, each at most one file per touched bucket.
        self.assertLessEqual(before,4*buckets)
        stored = self.rows(self.spark.read.format('delta').load(path))
        result = self.delta.compact('input_index')
        self.assertEqual(self.rows(self.spark.read.format('delta').load(path)),stored)
        self.assertGreater(result['files_removed'],result['files_added'])
        self.assertEqual(result['files_added'],buckets)
        self.assertIsNone(self.delta.compact('normalized_history'))

    def test_v1_history_is_carried_rebucketed_and_v1_proofs_still_load(self):
        class V1(BatchDelta):
            NAMESPACE, LEGACY, BUCKETS = 'finite-v1', 'finite-v0', 128
        old = V1(self.spark,self.lake,lambda:None,self.delta.timing)
        stored = [('k1','danawa:11','{"a":1}'),('k2','danawa:12','{"a":2}')]
        old.append_history('input_index',self.encoded(stored))
        proof = old.snapshot('job-v1','gold',self.encoded(stored))
        products = self.spark.createDataFrame([('danawa:11',),('danawa:12',)],'product_key string')
        # First use in v2 copies the v1 table once with 16-bucket values; the rows are unchanged.
        found = self.rows(self.delta.history('input_index',products))
        self.assertEqual([r[:4] for r in found],[r[:4] for r in self.rows(old.history('input_index',products))])
        self.assertTrue(all(0 <= r[4] < BatchDelta.BUCKETS for r in found))
        self.assertEqual(self.version(self.delta._path('input_index')),0)
        # The carried rows keep immutability: a changed payload under a v1 key is still a conflict.
        with self.assertRaisesRegex(Exception,'DELTA_IMMUTABLE_CONFLICT'):
            self.delta.append_history('input_index',self.encoded([('k1','danawa:11','{"a":999}')]))
        self.delta.append_history('input_index',self.encoded([('k1','danawa:11','{"a":1}'),('k3','danawa:13','{"a":3}')]))
        self.assertEqual(self.spark.read.format('delta').load(self.delta._path('input_index')).count(),3)
        # A completed v1 job's proof still loads and audits.
        self.assertEqual(len(self.rows(self.delta.load(proof))),2)
        self.delta.audit(proof)

    def test_existing_bucket_partitioned_snapshot_still_loads_audits_and_retries(self):
        # Snapshots written by earlier releases are partitioned by bucket; they must stay readable.
        frame = self.encoded([('k1','danawa:11','{"a":1}'),('k2','danawa:12','{"a":2}')])
        path = self.delta.root+'/runs/job-legacy/probe'
        (self.delta.encode(frame).write.format('delta').mode('append').partitionBy('bucket')
            .option('txnAppId','job-legacy:probe').option('txnVersion',0).save(path))
        proof = self.delta.snapshot('job-legacy','probe',frame)
        self.assertEqual(proof['version'],0)
        self.assertEqual(self.rows(self.delta.load(proof)),self.rows(self.delta.encode(frame)))
        self.delta.audit(proof)
        with self.assertRaisesRegex(Invalid,'DELTA_STAGE_RETRY_CONFLICT'):
            self.delta.snapshot('job-legacy','probe',self.encoded([('k1','danawa:11','{"a":1}')]))


if __name__ == '__main__':
    unittest.main()
