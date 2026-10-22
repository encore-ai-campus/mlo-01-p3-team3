import ast
import copy
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

HERE=Path(__file__).resolve().parents[1]
LEGACY=HERE.parent/'data_pipeline'
sys.path[:0]=[str(HERE),str(LEGACY),str(LEGACY/'tests')]

from test_silver import sample, reseal
from pipeline_v2.domain.contracts import Limits, Invalid, digest, seal
from pipeline_v2.adapters.files import FileLake, read, write
from pipeline_v2.adapters.sources import JsonSource
from pipeline_v2.adapters.publication import FilePublisher
from pipeline_v2.adapters.state import State
from pipeline_v2.application.capture import capture
from pipeline_v2.application.coordinator import Coordinator


CATEGORY='860974b0-49e6-4cd0-ad79-40d94d815eb5'
POLICY=dict(mappings=[dict(source_code='danawa',source_category_codes=['1','2'],service_category_id=CATEGORY,
    service_category_code='CHAIR',active=True,excluded=False)],rules=[],gold={})


def inputs(root, products=1, observations=1):
    documents=[]
    for product in range(products):
        directory=root/('sample'+str(product));directory.mkdir()
        original,state=sample(directory)
        for index in range(observations):
            doc=copy.deepcopy(original);image=copy.deepcopy(state)
            code=str(11+product)
            doc.update(_id=f'{product*100000+index+1:024x}',source_product_id=code,source_record_key='danawa:PRODUCT:'+code)
            doc['collector_metadata']['product_id']=code
            time=f'2026-10-16T03:{index//60:02d}:{index%60:02d}.000Z'
            doc['collected_at']=time;doc['collector_metadata']['collected_at']=time
            doc['payload']['listings'][0]['api_product']['productCode']=int(code)
            doc['payload']['listings'][0]['product_url']='https://prod.danawa.com/info/?pcode='+code
            doc['payload']['detail']['selected_product']['sku']=code
            doc['payload']['detail']['url']='https://prod.danawa.com/info/?pcode='+code
            image['raw_document_id']=doc['_id']
            reseal(doc,image)
            documents.append(dict(document=doc,image_state=image))
    write(root/'source.json',documents)
    return documents


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.root=Path(self.temporary.name)

    def tearDown(self):self.temporary.cleanup()

    def engine(self, products=1, observations=1, policy=None, **limits):
        inputs(self.root,products,observations)
        source=JsonSource(self.root/'source.json');lake=FileLake(self.root/'lake');publisher=FilePublisher(self.root/'sql')
        state=State(self.root/'state',dict(target='test'))
        config=Limits(min_free_bytes=1,**limits)
        capture(state,source,'initial',POLICY if policy is None else policy,config)
        return Coordinator(state,source,lake,publisher,config)

    def finish(self, engine, scope='initial', maximum=30):
        for _ in range(maximum):
            report=engine.status(scope)
            if report['status'].startswith('COMPLETED'):return report
            progress=engine.tick(scope)
            if progress['scan_complete'] and not progress['processed'] and not progress['delivered']:
                return engine.status(scope)
        self.fail('Engine did not settle')

    def test_direct_end_to_end_without_review_or_activation(self):
        engine=self.engine()
        report=self.finish(engine)
        self.assertEqual(report['status'],'COMPLETED')
        self.assertEqual(report['counts'],{'PUBLISHED':1})
        self.assertEqual(engine.processor.metrics['normalized'],1)
        self.assertIn('capture.source_discovery',report['timing']['stages'])
        self.assertIn('gold.reduce_product_history',report['timing']['stages'])
        self.assertIn('delivery.publish',report['timing']['stages'])
        self.assertGreater(report['timing']['event_count'],5)
        current=list((self.root/'sql/versions').glob('*.json'))
        group=read(current[0])
        self.assertTrue(group['catalog_ready']);self.assertFalse(group['recommendation_ready'])
        self.assertEqual(group['manufacturer'],'제조사')
        self.assertEqual(len(group['images']),2)
        self.assertEqual(len(engine.lake.read_keys('bronze_raw',['000000000000000000000001'])),1)

    def test_batch_commit_count_does_not_scale_per_product(self):
        engine=self.engine(products=10,pending_high=20,pending_low=1)
        self.finish(engine)
        self.assertEqual(engine.processor.metrics['normalized'],10)
        self.assertLessEqual(engine.lake.metrics['commits'],18)

    def test_101_observations_split_then_gold_contains_all(self):
        engine=self.engine(observations=101)
        result=self.finish(engine)
        self.assertEqual(result['status'],'COMPLETED')
        group=read(next((self.root/'sql/versions').glob('*.json')))
        self.assertEqual(len(group['source_refs']),101)

    def test_gold_only_policy_change_reuses_silver(self):
        engine=self.engine();self.finish(engine)
        previous=engine.processor.metrics['normalized']
        policy=copy.deepcopy(POLICY);policy['gold']={'require_comparison_link':True}
        capture(engine.state,engine.source,'changed-policy',policy,engine.limits)
        self.finish(engine,'changed-policy')
        self.assertEqual(engine.processor.metrics['normalized'],previous)
        self.assertGreater(engine.processor.metrics['silver_reused'],0)

    def test_unrelated_mapping_change_has_same_job_identity(self):
        engine=self.engine();self.finish(engine)
        first=engine._index(engine.state.scope('initial'),0)
        policy=copy.deepcopy(POLICY);policy['mappings'].append(dict(POLICY['mappings'][0],source_category_codes=['900']))
        capture(engine.state,engine.source,'unrelated',policy,engine.limits)
        self.assertEqual(first,engine._index(engine.state.scope('unrelated'),0))

    def test_quality_wait_is_a_result_without_pending_review_task(self):
        engine=self.engine(policy={})
        result=self.finish(engine)
        self.assertEqual(result['status'],'COMPLETED_WITH_QUALITY_WAIT')
        self.assertEqual(result['counts'],{'QUALITY_WAIT':1})
        self.assertEqual(engine.publisher.writes,0)

    def test_handoff_crash_reuses_gold_and_does_not_normalize_again(self):
        engine=self.engine()
        with patch.object(engine.state,'enqueue',side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):engine.tick('initial')
        normalized=engine.processor.metrics['normalized']
        self.finish(engine)
        self.assertEqual(engine.processor.metrics['normalized'],normalized)
        self.assertGreater(engine.processor.metrics['gold_reused'],0)

    def test_lost_sql_response_retries_without_duplicate_publication(self):
        engine=self.engine();engine.tick('initial')
        original=engine.publisher.publish
        def uncertain(delivery):original(delivery);raise OSError('response lost')
        with patch.object(engine.publisher,'publish',side_effect=uncertain):engine.tick('initial')
        task=engine.state.pending()[0]
        engine.state.update(task['job_id'],next_retry_at='')
        self.finish(engine)
        self.assertEqual(engine.publisher.writes,1)

    def test_unknown_running_requires_stop_evidence(self):
        engine=self.engine();key=engine._index(engine.state.scope('initial'),0)[0]
        engine.state.update(key,processing='RUNNING')
        with self.assertRaisesRegex(Invalid,'RECOVERY_EVIDENCE'):engine.tick('initial')
        with self.assertRaisesRegex(Invalid,'STOP_EVIDENCE'):engine.recover(key,'')
        engine.recover(key,'operator://old-process-stopped')
        self.assertEqual(self.finish(engine)['status'],'COMPLETED')

    def test_source_change_prevents_stale_publication(self):
        engine=self.engine();engine.tick('initial')
        changed=read(self.root/'source.json');changed[0]['image_state']['image_state_revision']+=1
        write(self.root/'source.json',changed)
        engine.tick('initial')
        self.assertEqual(engine.publisher.writes,0)
        self.assertEqual(engine.status('initial')['counts'],{'SUPERSEDED':1})

    def test_deleted_gold_output_cannot_be_reused(self):
        engine=self.engine();engine.tick('initial')
        for path in (self.root/'lake/gold_catalog/rows').rglob('*.json'):path.unlink()
        with self.assertRaisesRegex(Invalid,'OUTPUT_MISSING'):engine.tick('initial')

    def test_incomplete_capture_does_not_appear_sealed(self):
        engine=self.engine()
        with patch.object(engine.source,'references',side_effect=OSError('offline')):
            with self.assertRaises(OSError):capture(engine.state,engine.source,'incomplete',POLICY,engine.limits)
        self.assertFalse((self.root/'state/scopes/incomplete/scope.json').exists())

    def test_history_limit_does_not_count_as_quality_completion(self):
        engine=self.engine(products=2,history_rows=1)
        result=self.finish(engine)
        self.assertEqual(result['status'],'ATTENTION_REQUIRED')
        self.assertEqual(result['counts'],{'BLOCKED':2})
        self.assertFalse(result['gold_complete'])

    def test_backpressure_reserves_bytes_before_processing(self):
        engine=self.engine(pending_bytes=1)
        progress=engine.tick('initial')
        self.assertEqual(progress['status'],'WAITING_DELIVERY')
        self.assertEqual(engine.processor.metrics['normalized'],0)

    def test_sealed_scope_cannot_change_policy_or_membership(self):
        engine=self.engine()
        with self.assertRaisesRegex(Invalid,'SCOPE_ALREADY_SEALED'):
            capture(engine.state,engine.source,'initial',{},engine.limits)

    def test_overlapping_outputs_rejected(self):
        from pipeline_v2.bootstrap import configuration
        inputs(self.root)
        config=dict(contract_version=2,mode='local',target='test',source_file='source.json',state_root='state',lake_root='state/lake',publication_root='sql')
        write(self.root/'config.json',config)
        with self.assertRaisesRegex(Invalid,'OVERLAP'):configuration(self.root/'config.json')

    def test_cli_local_workflow_and_json_output(self):
        from pipeline_v2.interfaces.cli import main
        inputs(self.root)
        write(self.root/'config.json',dict(contract_version=2,mode='local',target='test',source_file='source.json',
            state_root='state',lake_root='lake',publication_root='sql',policy=POLICY,limits={'min_free_bytes':1}))
        output=io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(['--config',str(self.root/'config.json'),'capture','--scope','cli']),0)
            self.assertEqual(main(['--config',str(self.root/'config.json'),'run','--scope','cli','--max-ticks','5']),0)
        self.assertIn('"status": "COMPLETED"',output.getvalue())

    def test_dependency_direction_has_no_old_orchestration_imports(self):
        allowed={'product_pipeline.silver.normalize','product_pipeline.silver.inputs','product_pipeline.bronze.contracts',
            'product_pipeline.contracts','product_pipeline.mongo','product_pipeline.publication.mysql',
            'product_pipeline.publication.policy','product_pipeline.publication.schema','product_pipeline.bronze.lease','product_pipeline.spark_storage'}
        for path in (HERE/'pipeline_v2').rglob('*.py'):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                if isinstance(node,ast.ImportFrom) and (node.module or '').startswith('product_pipeline.'):
                    self.assertIn(node.module,allowed)
                if isinstance(node,ast.ImportFrom):self.assertNotIn('activation',(node.module or '').split('.'))


if __name__=='__main__':unittest.main()
