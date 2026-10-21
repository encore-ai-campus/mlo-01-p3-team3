import copy
import tempfile
import unittest
from pathlib import Path

from product_pipeline.contracts import digest
from product_pipeline.files import atomic_json, read_json
from product_pipeline.errors import PipelineError


class WindowRuntimeTests(unittest.TestCase):
    def test_server_progress_callback_accepts_already_timestamped_state(self):
        import importlib.util,types
        from unittest.mock import patch,MagicMock
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            stub=types.SimpleNamespace(setup=lambda:({},{}),pinned=lambda *a:{'products':36},LAKE='s3a://test/lake',ROOT=root)
            path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_window_work.py'
            spec=importlib.util.spec_from_file_location('window_runtime_test',path)
            job=importlib.util.module_from_spec(spec)
            with patch.dict('sys.modules',production_work=stub):spec.loader.exec_module(job)
            state=dict(status='COMPLETED',bronze_version=36,updated_at='2026-10-19T00:00:00.000Z')
            def run(*a,progress,**kw):progress(state);return dict(state)
            db=MagicMock();db.one.return_value=dict(products=0,active=0,pending=0,recommendation_ready=0)
            with patch.object(job,'run_windows',side_effect=run),patch('product_pipeline.publication.mysql.MySQL',return_value=db):
                self.assertEqual(job.main(),0)
            self.assertEqual(read_json(root/'window-status.json')['status'],'COMPLETED')


class WindowTests(unittest.TestCase):
    def test_actual_cli_failure_survives_without_reusing_a_historical_error(self):
        from product_pipeline.windows import checked_stage
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            atomic_json(root/'runs/old.json',dict(status='FAILED',error_code='OLD_ERROR'))
            def cli(args):
                atomic_json(root/'runs/new.json',dict(status='FAILED',error_code='GOLD_PRODUCT_HISTORY_LIMIT'));return 2
            with self.assertRaisesRegex(PipelineError,'GOLD_PRODUCT_HISTORY_LIMIT'):checked_stage(cli,[],root,'gold')
            with self.assertRaisesRegex(PipelineError,'GOLD_FAILED'):checked_stage(lambda a:2,[],root,'gold')

    def test_pause_occurs_only_after_publish_and_activation_without_next_plan(self):
        from product_pipeline.windows import run_windows
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);calls=[]
            def stage(name,cut):
                calls.append(name)
                if name=='plan':atomic_json(cut/'selection.json',self.window())
                return dict(status='COMPLETED')
            state=run_windows(root,36,stage,pause_requested=lambda state:True)
            self.assertEqual(calls,['plan','silver','gold','publish','activation'])
            self.assertEqual(state['status'],'PAUSED_AFTER_BATCH')
            self.assertEqual(state['window'],1)
            self.assertEqual(state['after'],'b'*64)

    def window(self, after=None):
        from product_pipeline.windows import seal_window
        return seal_window(36, after, [dict(observation_key='b'*64, source_code='danawa', source_product_id='12')])

    def test_fixed_selection_is_bounded_hashed_and_rejects_tampering(self):
        from product_pipeline.windows import seal_window, validate_window
        value=self.window();validate_window(value)
        self.assertEqual(value['product_keys'], ['danawa:12'])
        changed=copy.deepcopy(value);changed['bronze_version']=37
        with self.assertRaises(PipelineError):validate_window(changed)
        with self.assertRaises(PipelineError):seal_window(36, None, [])
        with self.assertRaises(PipelineError):seal_window(36, 'c'*64, [dict(observation_key='b'*64,source_code='danawa',source_product_id='12')])

    def test_completed_silver_selection_must_cover_the_entire_window(self):
        from product_pipeline.silver.selection import seal,validate
        window=self.window()
        choice=dict(observation_key='b'*64,product_key='danawa:12',raw_document_id='a'*24,
            evaluation_key='silver-eval-v1:'+'c'*64,evidence_ref='silver-receipt://silver-eval-v1:'+'c'*64)
        value=seal(window,[choice]);self.assertEqual(validate(value,window),[choice])
        with self.assertRaises(PipelineError):seal(window,[])
        broken=copy.deepcopy(value);broken['choices'][0]['evaluation_key']='silver-eval-v1:'+'d'*64
        with self.assertRaises(PipelineError):validate(broken,window)

    def test_legacy_recovery_requires_successful_complete_log_and_matching_raw(self):
        from product_pipeline.silver.selection import recover
        from product_pipeline.contracts import canonical
        window=self.window();raw=dict(_id='a'*24,observation_key='b'*64,source_code='danawa',source_product_id='12')
        line=dict(stage='silver_observation',raw_document_id='a'*24,evaluation_key='silver-eval-v1:'+'c'*64)
        log=canonical(line)+'\n'+canonical(dict(status='COMPLETED',processed=1))
        value=recover(log,[raw],window);self.assertEqual(len(value['choices']),1)
        with self.assertRaises(PipelineError):recover(canonical(line),[raw],window)
        with self.assertRaises(PipelineError):recover(log,[dict(raw,observation_key='d'*64)],window)

    def test_publishes_each_window_before_planning_the_next_and_resumes_failure(self):
        from product_pipeline.windows import run_windows
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);calls=[];fail=[True]
            def stage(name, cut):
                calls.append(name)
                if name=='plan':
                    if cut.name=='000001':return dict(status='COMPLETED',drained=True)
                    atomic_json(cut/'selection.json',self.window())
                if name=='publish' and fail[0]:fail[0]=False;raise RuntimeError('injected')
                return dict(status='COMPLETED')
            with self.assertRaises(RuntimeError):run_windows(root,36,stage)
            self.assertEqual(read_json(root/'windows-progress.json')['after'],None)
            run_windows(root,36,stage)
            self.assertEqual(calls,['plan','silver','gold','publish','publish','activation','plan'])
            self.assertEqual(read_json(root/'windows-progress.json')['after'],'b'*64)
            self.assertEqual(read_json(root/'windows-progress.json')['status'],'COMPLETED')
            self.assertEqual(read_json(root/'windows-progress.json')['last_batch']['reused_stages'],['plan','silver','gold'])
            with self.assertRaises(PipelineError):run_windows(root,37,stage)

    def test_failed_or_missing_stage_receipt_never_advances_window(self):
        from product_pipeline.windows import run_windows
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            def stage(name,cut):
                if name=='plan':atomic_json(cut/'selection.json',self.window())
                return dict(status='COMPLETED' if name=='plan' else 'FAILED')
            with self.assertRaises(PipelineError):run_windows(root,36,stage)
            self.assertIsNone(read_json(root/'windows-progress.json')['after'])


class ReadReuseTests(unittest.TestCase):
    def test_pending_and_activation_share_one_bounded_gold_read(self):
        from product_pipeline.publication.source import CachedGoldSource
        class Source:
            manifest={'product_count':1}
            def __init__(self):self.reads=0
            def groups_batched(self,n):self.reads+=1;yield dict(key='a',values=[1])
        source=Source();cached=CachedGoldSource(source)
        first=list(cached.groups_batched());first[0]['values'].append(2)
        self.assertEqual(list(cached.groups_batched())[0]['values'],[1])
        self.assertEqual(source.reads,1)

    def test_owner_export_reads_tables_once_per_page_not_per_product(self):
        from product_pipeline.gold.source import iter_groups
        class Source:
            def __init__(self):self.calls=[]
            def products(self,after):return ['a','b','c'] if after is None else []
            def groups_many(self,keys):self.calls.append(tuple(keys));return [[dict(key=k)] for k in keys]
            def groups(self,key):raise AssertionError('per-product scan')
        s=Source();self.assertEqual(list(iter_groups(s)),[dict(key=k) for k in ['a','b','c']])
        self.assertEqual(s.calls,[('a','b','c')])

    def test_history_budget_splits_without_skipping_products(self):
        from product_pipeline.gold.source import iter_groups
        class Source:
            def products(self,after):return [k for k in ['a','b','c'] if after is None or k>after]
            def groups_many(self,keys):
                if len(keys)>1:raise PipelineError('GOLD_PRODUCT_HISTORY_LIMIT')
                return [[dict(key=keys[0])]]
        self.assertEqual([x['key'] for x in iter_groups(Source())],['a','b','c'])

    def test_materialized_review_inputs_have_a_total_byte_budget(self):
        from product_pipeline.gold.source import bounded_groups
        class Source:
            def products(self,after):return ['a'] if after is None else []
            def groups_many(self,keys):return [[dict(payload='x'*100)]]
        with self.assertRaisesRegex(PipelineError,'HISTORY_LIMIT'):bounded_groups(Source(),max_bytes=50)

    def test_base_version_change_cannot_reuse_a_completed_range(self):
        from product_pipeline.windows import run_windows
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            run_windows(root,36,lambda *a:dict(status='COMPLETED',drained=True),base_version=35)
            with self.assertRaises(PipelineError):run_windows(root,36,lambda *a:None,base_version=34)


class ClusterProfileTests(unittest.TestCase):
    def test_requires_explicit_cluster_and_reachable_driver(self):
        from product_pipeline.spark_runtime import cluster_options
        value=dict(master='spark://192.168.0.121:7077',driver_host='192.168.0.121',executor_cores=1,total_cores=3,executor_memory='768m',driver_memory='768m',shuffle_partitions=3,executor_python='/usr/bin/python3')
        result=cluster_options(value)
        self.assertEqual(result['spark.master'],value['master'])
        self.assertEqual(result['spark.driver.host'],'192.168.0.121')
        for patch in [dict(master='local[1]'),dict(driver_host='127.0.0.1'),dict(total_cores=0)]:
            with self.assertRaises(PipelineError):cluster_options(dict(value,**patch))


if __name__=='__main__':unittest.main()
