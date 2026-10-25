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


    def test_per_product_gold_matches_native_gold_payload_exactly(self):
        import copy
        from pipeline_v2.adapters import spark_product
        root = Path(self.temp.name)/'parity'
        root.mkdir()
        source = fixtures.inputs(root,products=3,observations=2)
        normalized = {}
        for item in source:
            value = normalize(item,fixtures.POLICY)
            normalized.setdefault('danawa:'+value['raw_ref']['source_product_id'],[]).append(value)
        # A synthetic product exercising equal-time conflicts, offers, comparison links and image ties.
        first, second = (copy.deepcopy(v) for v in normalized['danawa:11'][:2])
        for value in (first, second):
            value['raw_ref']['source_product_id'] = '14'
        second['raw_ref']['_id'] = 'ffffffffffffffffffffffff'
        at = first['tables']['product_observations'][0]['observed_at']
        conflict = copy.deepcopy(first['tables']['product_observations'][0])
        conflict.update(display_name='other name',raw_document_id='ffffffffffffffffffffffff')
        second['tables']['product_observations'] = [conflict]
        offer = dict(first['tables']['price_observations'][0])
        offers = []
        for index, (key, when, raw) in enumerate([('o1',at,'a'),('o1',at,'b'),('o2',None,'c'),('o2','2026-10-15T01:00:00.000Z','d')]):
            identity = {k:None for k in offer}
            identity.update(offer_key=key,observed_at=when,raw_document_id=raw,seller_name='s%d' % index)
            offers.append(identity)
        first['tables']['normalized_offers'] = offers
        prices = []
        for key, when, raw, amount in [('o1',at,'a','100'),('o1',at,'b','200'),('o1','2026-10-14T00:00:00.000Z','a','90'),('o2',at,'c','300')]:
            price = dict(offer,offer_key=key,observed_at=when,raw_document_id=raw,amount=amount,currency='KRW',numeric_status='CONFIRMED')
            prices.append(price)
        first['tables']['price_observations'] = first['tables']['price_observations'] + prices
        # Newest link time, so it is not an equal-time conflict with the fixture's own comparison link.
        link = dict(first['tables']['link_observations'][0],link_kind='COMPARISON',structure_status='VALID',source_url='https://x/1',
            observed_at='2026-10-15T09:00:00.000Z')
        first['tables']['link_observations'] = first['tables']['link_observations'] + [link]
        twin = dict(first['direct_images'][0],raw_document_id='000000000000000000000000',url='https://images.example/twin.png')
        clash = dict(first['direct_images'][0],image_slot='gallery-1',sha256='different')
        first['direct_images'] = first['direct_images'] + [twin,clash,dict(clash,sha256='another')]
        normalized['danawa:14'] = [first,second]

        policy_hash = digest(dict(version=VERSION,**{k:fixtures.POLICY[k] for k in ('mappings','rules')}))
        retracted = {'danawa:12':['r2','r1']}
        history = self.spark.createDataFrame([(p,canonical(v)) for p,values in normalized.items() for v in values],
            'product_key string,normalized_json string')
        policies = self.spark.createDataFrame([(p,policy_hash) for p in normalized],'product_key string,normalization_policy_hash string')
        lists = self.spark.createDataFrame([(p,sorted(v)) for p,v in retracted.items()],'product_key string,retracted_raw_document_ids array<string>')
        layout = spark_product.schema()
        for gold_policy in ({},{'require_image':True,'require_comparison_link':True}):
            native = {r['product_key']:json.loads(r['payload_json']) for r in build_gold(history,policies,gold_policy,lists).collect()}
            self.assertEqual(set(native),set(normalized))
            # The synthetic cases are actually exercised, not vacuously equal.
            synthetic = native['danawa:14']
            self.assertTrue(synthetic['offers'])
            self.assertEqual(synthetic['comparison_url'],'https://x/1')
            self.assertIsNone(synthetic['display_name'])
            self.assertEqual(native['danawa:12']['retracted_raw_document_ids'],['r1','r2'])
            for product, values in normalized.items():
                mine = spark_product.gold(product,[canonical(v) for v in values],policy_hash,gold_policy,retracted.get(product),layout)
                self.assertEqual(json.loads(spark_product.dumps(mine)),native[product],product)


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
