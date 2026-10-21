import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from helpers import fixture
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict, PipelineError
from product_pipeline.files import atomic_json, read_json, sha_file, lock
from product_pipeline.snapshot import prepare as snapshot
from product_pipeline.operations.flow import prepare, run, recover, audit, seal, STAGES, REQUIRED, WorkerUnconfirmed
from product_pipeline.operations.flow_audit import compare_sorted, prepared, sql_plan_audit
from product_pipeline.operations.flow_worker import invoke
from product_pipeline.operations.storage import report
from product_pipeline.operations.queue import Queue


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        source,media=fixture(self.root,codes=('11',));snapshot(source,self.root/'snapshot',media)
        files={}
        for name in REQUIRED:
            path=self.root/(name+'.json');atomic_json(path,{'fixture':name});files[name]=str(path)
        self.config=self.root/'request.json'
        atomic_json(self.config,dict(contract_version=1,mode='live',target='local-test',
            as_of='2026-10-18T00:00:00Z',snapshot=str(self.root/'snapshot'),files=files))
        self.state=self.root/'state'
        # Engine tests use explicit fixtures, not real owner approval claims.
        with patch('product_pipeline.operations.flow_runtime.settings'):
            prepare(self.config,self.state)
        self.calls=[]

    def tearDown(self):self.tmp.cleanup()

    def execute(self,context):
        stage=context['stage'];self.calls.append(stage)
        for prior in STAGES[:STAGES.index(stage)]:self.assertIn(prior,context['outputs'])
        artifact=self.root/(stage+'.result.json');atomic_json(artifact,dict(stage=stage,identity=digest(stage)))
        return dict(status='VERIFIED',stage=stage,artifacts=[dict(path=str(artifact),sha256=sha_file(artifact))])

    def test_order_and_repeat_skip_every_verified_stage(self):
        result=run(self.state,self.execute)
        self.assertEqual(result['status'],'COMPLETED');self.assertEqual(tuple(self.calls),STAGES)
        self.calls=[];result=run(self.state,self.execute)
        self.assertEqual(result['reused_stages'],7);self.assertEqual(self.calls,[])

    def test_failure_resumes_only_failed_and_downstream(self):
        def fail(context):
            if context['stage']=='GOLD':raise TimeoutError('secret-not-persisted')
            return self.execute(context)
        with self.assertRaises(TimeoutError):run(self.state,fail)
        self.calls=[];result=run(self.state,self.execute)
        self.assertEqual(self.calls,['GOLD','PREPARE','PUBLISH','RECONCILE'])
        self.assertEqual(result['reused_stages'],3)
        self.assertNotIn('secret-not-persisted',(self.state/'attempts/GOLD.json').read_text())

    def test_crash_requires_exact_token_and_stop_evidence(self):
        def crash(context):raise WorkerUnconfirmed('test child state unknown')
        with self.assertRaises(WorkerUnconfirmed):run(self.state,crash)
        attempt=read_json(self.state/'attempts/INGEST.json')
        with self.assertRaises(PipelineError):run(self.state,self.execute)
        with self.assertRaises(Conflict):recover(self.state,'INGEST','wrong','operator://stopped')
        recover(self.state,'INGEST',attempt['token'],'operator://child-and-jvm-stopped')
        self.assertEqual(run(self.state,self.execute)['status'],'COMPLETED')

    def test_crash_after_work_before_receipt_repeats_only_same_stage_context(self):
        def crash(stage,output):
            if stage=='SILVER':raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):run(self.state,self.execute,hook=crash)
        old=read_json(self.state/'attempts/SILVER.json')
        recover(self.state,'SILVER',old['token'],'operator://stopped')
        self.calls=[];run(self.state,self.execute)
        self.assertEqual(self.calls[0],'SILVER');self.assertNotIn('INGEST',self.calls)

    def test_changed_artifact_blocks_reuse(self):
        run(self.state,self.execute);atomic_json(self.root/'BRONZE.result.json',{'changed':True})
        with self.assertRaises(Conflict):run(self.state,self.execute)

    def test_changed_input_blocks_before_executor(self):
        atomic_json(self.root/'silver_inputs.json',{'changed':True})
        with self.assertRaises(Conflict):run(self.state,self.execute)
        self.assertEqual(self.calls,[])

    def test_deleted_middle_receipt_blocks_downstream_reuse(self):
        run(self.state,self.execute);(self.state/'receipts/SILVER.json').unlink()
        with self.assertRaises(Conflict):run(self.state,self.execute)

    def test_resealed_wrong_chain_blocks(self):
        run(self.state,self.execute);path=self.state/'receipts/GOLD.json';value=read_json(path)
        value.pop('content_hash');value['previous_hash']='0'*64;atomic_json(path,seal(value))
        with self.assertRaises(Conflict):run(self.state,self.execute)

    def test_quality_wait_is_distinct_from_service_publication(self):
        def execute(context):
            value=self.execute(context)
            if context['stage']=='PREPARE':value.update(blocked=1,plans=0)
            return value
        self.assertEqual(run(self.state,execute)['status'],'COMPLETED_WITH_QUALITY_WAIT')

    def test_fresh_audit_rechecks_only_reader_without_altering_original_receipts(self):
        run(self.state,self.execute)
        before={p.name:sha_file(p) for p in (self.state/'receipts').glob('*.json')}
        self.calls=[];result=audit(self.state,self.execute)
        self.assertEqual(result['status'],'AUDIT_VERIFIED');self.assertEqual(self.calls,['RECONCILE'])
        self.assertEqual(before,{p.name:sha_file(p) for p in (self.state/'receipts').glob('*.json')})

    def test_worker_lock_blocks_concurrent_flow(self):
        with lock(self.state/'flow.lock'):
            with self.assertRaises(PipelineError):run(self.state,self.execute)

    def test_queue_and_flow_both_latch_on_unknown_worker(self):
        q=Queue(self.root/'queue','local-test');q.enqueue('FLOW',{'flow_id':'fixed'})
        def child(_):return run(self.state,lambda _:(_ for _ in ()).throw(WorkerUnconfirmed()))
        with self.assertRaises(WorkerUnconfirmed):q.tick({'FLOW':child})
        self.assertEqual(q.summary(),{'RUNNING':1})

    def test_real_settings_reject_missing_owner_contract_before_state_creation(self):
        with self.assertRaises(Exception):prepare(self.config,self.root/'invalid')
        self.assertFalse((self.root/'invalid').exists())

    def test_production_worker_returns_redacted_failure_without_service_connections(self):
        from product_pipeline.operations.flow_runtime import execute
        plan=read_json(self.state/'flow.json')
        context=seal(dict(flow=plan,root=str(self.state),stage='INGEST',outputs={},previous_hash=plan['content_hash']))
        with self.assertRaises(PipelineError):execute(context)
        responses=list((self.state/'workers/INGEST').glob('*/response.json'))
        self.assertEqual(len(responses),1)
        result=read_json(responses[0]);self.assertEqual(result['status'],'FAILED')
        self.assertIn('error_code',result)

    def test_real_contract_preparation_and_cross_stage_target_rejection(self):
        from dataclasses import asdict
        from product_pipeline.config import Settings
        from product_pipeline.bronze.config import BronzeSettings
        from product_pipeline.silver.config import SilverSettings
        from product_pipeline.gold.config import GoldSettings
        from test_silver import inputs,RUN
        from test_gold import side
        bronze=BronzeSettings('local-test','raw',(self.root/'lake/bronze').as_uri(),
            (self.root/'lake/events').as_uri(),(self.root/'lake/quarantine').as_uri(),
            (self.root/'checkpoints/bronze').as_uri(),'kafka-test',single_writer_confirmed=True)
        silver=SilverSettings('local-test','raw',bronze.bronze_path,
            (self.root/'lake/silver').as_uri(),(self.root/'checkpoints/silver').as_uri())
        gold=GoldSettings('local-test',silver.silver_root,(self.root/'lake/gold').as_uri(),(self.root/'checkpoints/gold').as_uri())
        values=dict(pipeline_config=asdict(Settings('local-test','raw','test-images')),
            bronze_config=asdict(bronze),silver_config=asdict(silver),gold_config=asdict(gold),
            silver_inputs=inputs(),gold_inputs=side(),guard_hashes={},
            mysql_config=dict(contract_version=1,target_id='service-test',database='service',host='mysql.test',user='publisher'))
        for name,stage in [('silver_registration','NORMALIZATION'),('gold_registration','GOLD_BUILD')]:
            values[name]=dict(processing_run_id=RUN,stage=stage,status='REGISTERED',provider='MYSQL_PIPELINE_RUNS_EXPORT',evidence_ref='fixture://owner-export')
        for name,value in values.items():atomic_json(self.root/(name+'.json'),value)
        self.assertEqual(prepare(self.config,self.root/'valid')['status'],'FLOW_PREPARED')
        changed=asdict(silver);changed['mongo_database']='wrong';atomic_json(self.root/'silver_config.json',changed)
        with self.assertRaisesRegex(PipelineError,'FLOW_STAGE_TARGET_MISMATCH'):prepare(self.config,self.root/'wrong-target')
        self.assertFalse((self.root/'wrong-target').exists())


class AuditTests(unittest.TestCase):
    def test_silver_partial_cursor_bound_to_flow_and_owner_input_scope(self):
        from product_pipeline.silver.checkpoint import bind_scan
        cursor=dict(bronze_version=5,after='a'*64,epoch=3,completed=False,scan_scope='old')
        self.assertEqual(bind_scan(cursor,'old'),cursor)
        changed=bind_scan(cursor,'new')
        self.assertIsNone(changed['after']);self.assertFalse(changed['completed'])
        self.assertEqual(changed['epoch'],4);self.assertEqual(changed['scan_scope'],'new')

    def test_legacy_silver_cursor_rescans_without_deleting_outputs(self):
        from product_pipeline.silver.checkpoint import bind_scan
        value=bind_scan(dict(bronze_version=2,after='b'*64,epoch=0,completed=False),'new')
        self.assertIsNone(value['after']);self.assertEqual(value['bronze_version'],2)

    def test_same_counts_different_keys_fail(self):
        with self.assertRaises(Conflict):compare_sorted([{'id':'a'}],[{'id':'b'}],'id',[])

    def test_duplicate_and_unsorted_keys_fail(self):
        for values in ([{'id':'a'},{'id':'a'}],[{'id':'b'},{'id':'a'}]):
            with self.assertRaises(Conflict):compare_sorted(values,values,'id',[])

    def test_same_identity_wrong_hash_fails(self):
        with self.assertRaises(Conflict):compare_sorted([{'id':'a','hash':'1'}],[{'id':'a','hash':'2'}],'id',['hash'])

    def test_missing_row_fails(self):
        with self.assertRaises(Conflict):compare_sorted([{'id':'a'}],[],'id',[])

    def test_large_stream_does_not_require_materialized_lists(self):
        def stream():
            for i in range(65498):yield dict(id=f'{i:08d}',hash=str(i))
        self.assertEqual(compare_sorted(stream(),stream(),'id',['hash']),65498)

    def test_sql_receipt_not_completed_blocks(self):
        from publication_fixture import fixture as publication
        with tempfile.TemporaryDirectory() as directory:
            plan,_=publication(Path(directory),'a'*64)
            class DB:
                def get(self,*args):return None
            with self.assertRaises(Conflict):sql_plan_audit(plan,DB())

    def test_worker_nonzero_and_partial_success_are_not_stage_success(self):
        def fail(args):print('{"status":"FAILED","error_code":"EXPLICIT_FAILURE"}');return 2
        def partial(args):print('{"result":"started"}');return 0
        for function in (fail,partial):
            with self.assertRaises(PipelineError):invoke(function,[])

    def test_storage_alerts_do_not_delete_anything(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'receipt.json';path.write_text('important')
            result=report(directory,1,0)
            self.assertEqual(result['status'],'ATTENTION_REQUIRED');self.assertFalse(result['automatic_deletion'])
            self.assertEqual(path.read_text(),'important')

    def test_storage_backpressure_blocks_scheduler_before_tasks(self):
        from product_pipeline.operations.cli import tick
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);config=root/'config.json'
            atomic_json(config,dict(contract_version=1,mode='rehearse',logical_target='local',
                queue_root=str(root/'queue'),audit_root=str(root/'audit'),
                storage_policy=dict(root=str(root),max_bytes=1,min_free_bytes=0)))
            with self.assertRaisesRegex(PipelineError,'OPERATIONS_STORAGE_BACKPRESSURE'):tick(config)
            self.assertFalse((root/'queue/tasks').exists())


if __name__=='__main__':unittest.main()
