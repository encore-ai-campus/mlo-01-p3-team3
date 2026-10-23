"""Opt-in real Spark SQL tests; no Kafka/Delta server or real product data."""
import json
import os
import sys
import tempfile
from pathlib import Path
import unittest

import test_engine as fixtures
from pipeline_v2.domain.contracts import canonical,digest
from pipeline_v2.domain.transform import normalize,gold,VERSION
from pipeline_v2.adapters.spark_gold import build_gold
from pipeline_v2.adapters.spark_metrics import report
from pipeline_v2.adapters.files import write


@unittest.skipUnless(os.environ.get('PIPELINE_V2_TEST_SPARK') == '1','set PIPELINE_V2_TEST_SPARK=1 for a real local Spark JVM')
class NativeGoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.temp = tempfile.TemporaryDirectory()
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        cls.spark = (SparkSession.builder.master('local[2]').appName('pipeline-v2-native-gold-test')
            .config('spark.ui.enabled','false').config('spark.driver.host','127.0.0.1')
            .config('spark.driver.bindAddress','127.0.0.1').config('spark.sql.shuffle.partitions','2')
            .config('spark.sql.warehouse.dir',(Path(cls.temp.name)/'warehouse').as_uri())
            .config('spark.local.dir',str(Path(cls.temp.name)/'scratch')).getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_native_gold_matches_reference_selection_without_driver_reducer(self):
        root = Path(self.temp.name)/'source'
        root.mkdir()
        source = fixtures.inputs(root,products=1,observations=2)
        normalized = [normalize(item,fixtures.POLICY) for item in source]
        history = self.spark.createDataFrame(
            [('danawa:11',canonical(item)) for item in normalized],'product_key string,normalized_json string')
        policy_hash = digest(dict(version=VERSION,**{k:fixtures.POLICY[k] for k in ('mappings','rules')}))
        policies = self.spark.createDataFrame([('danawa:11',policy_hash)],
            'product_key string,normalization_policy_hash string')
        result_frame = build_gold(history,policies,{})
        # Native windows/joins/aggregates: no Python Gold UDF in the physical plan.
        plan = result_frame._jdf.queryExecution().executedPlan().toString()
        self.assertNotIn('BatchEvalPython',plan)
        actual = json.loads(result_frame.first()['payload_json'])
        expected = gold('danawa:11',normalized,fixtures.POLICY)
        for name in ('product_key','display_name','manufacturer','category_id','comparison_url',
                     'catalog_ready','recommendation_ready','normalization_policy_hash','source_refs','lineage'):
            self.assertEqual(actual[name],expected[name],name)
        for name in ('images','price_observations','attributes','offers','links'):
            self.assertEqual(sorted(actual[name],key=canonical),sorted(expected[name],key=canonical),name)


class SparkMetricsTests(unittest.TestCase):
    def test_offline_report_counts_attempts_and_never_leaks_sql_or_executor_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.json'
            events = [
                {'Event':'SparkListenerApplicationStart','Timestamp':1000},
                {'Event':'SparkListenerJobStart','Job ID':1,'Submission Time':1100,'Stage IDs':[2],
                 'Properties':{'pipeline.v2.scope_hash':digest('batch'),'pipeline.v2.stage':'silver'}},
                {'Event':'SparkListenerTaskEnd','Stage ID':2,'Stage Attempt ID':0,
                 'Task Info':{'Task ID':4,'Launch Time':1200,'Finish Time':1400,'Failed':False},
                 'Task Metrics':{'Executor Run Time':150,'Executor CPU Time':100000,'JVM GC Time':2,
                     'Input Metrics':{'Bytes Read':50,'Records Read':5}}},
                {'Event':'SparkListenerJobEnd','Job ID':1,'Completion Time':1500},
                {'Event':'SparkListenerApplicationEnd','Timestamp':1600}
            ]
            path.write_text(''.join(json.dumps(e)+'\n' for e in events),encoding='utf-8')
            result = report(path,'batch')
            self.assertEqual(result['job_count'],1)
            self.assertEqual(result['totals']['task_attempts'],1)
            self.assertEqual(result['task_p95_ms'],200)
            self.assertEqual(result['totals']['input_bytes'],50)
            self.assertFalse(result['partial_log'])

    def test_jobs_are_grouped_by_stage_with_overlapping_wall_time_counted_once(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.json'
            tag = lambda stage:{'pipeline.v2.scope_hash':digest('batch'),'pipeline.v2.stage':stage}
            events = [
                {'Event':'SparkListenerJobStart','Job ID':1,'Submission Time':100,'Stage IDs':[1],'Properties':tag('delta.snapshot.gold.write')},
                {'Event':'SparkListenerJobStart','Job ID':2,'Submission Time':150,'Stage IDs':[2],'Properties':tag('delta.snapshot.gold.write')},
                {'Event':'SparkListenerJobEnd','Job ID':1,'Completion Time':200},
                {'Event':'SparkListenerJobEnd','Job ID':2,'Completion Time':300},
                {'Event':'SparkListenerJobStart','Job ID':3,'Submission Time':400,'Stage IDs':[3],'Properties':tag('publish.lake_read')},
                {'Event':'SparkListenerTaskEnd','Stage ID':1,'Stage Attempt ID':0,
                 'Task Info':{'Task ID':1,'Launch Time':110,'Finish Time':190},
                 'Task Metrics':{'Output Metrics':{'Bytes Written':70,'Records Written':7}}},
            ]
            path.write_text(''.join(json.dumps(e)+'\n' for e in events),encoding='utf-8')
            result = report(path,'batch')
            write_jobs = result['jobs_by_stage']['delta.snapshot.gold.write']
            self.assertEqual((write_jobs['jobs'],write_jobs['job_wall_ms']),(2,200))
            self.assertEqual(result['jobs_by_stage']['publish.lake_read']['unfinished_jobs'],1)
            self.assertEqual(result['totals']['output_bytes'],70)
