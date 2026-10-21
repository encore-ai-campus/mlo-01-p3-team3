import importlib.util
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from product_pipeline.contracts import digest
from product_pipeline.files import atomic_json,read_json
from product_pipeline.silver.selection import seal
from product_pipeline.windows import seal_window
from product_pipeline.errors import Conflict


def module():
    path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_activation.py'
    spec=importlib.util.spec_from_file_location('activation_fixture',path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


class ActivationTests(unittest.TestCase):
    def test_effective_date_crossing_or_missing_time_proof_requires_reevaluation(self):
        helper=module();inputs={'reviews':dict(rows=[dict(valid_from='2026-10-19T00:00:00Z',expires_at='2026-10-20T12:00:00Z')])}
        parent=dict(finished_at='2026-10-20T10:00:00Z',duration_ms=1000)
        self.assertTrue(helper.time_inputs_unchanged(inputs,parent,'2026-10-20T11:00:00Z'))
        self.assertFalse(helper.time_inputs_unchanged(inputs,parent,'2026-10-20T13:00:00Z'))
        self.assertFalse(helper.time_inputs_unchanged(inputs,{},'2026-10-20T11:00:00Z'))

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.helper=module()
        self.window=seal_window(42,None,[dict(observation_key=str(i)*64,source_code='danawa',source_product_id=str(i)) for i in (1,2)])
        self.choices=[dict(observation_key=str(i)*64,raw_document_id=str(i)*24,product_key='danawa:'+str(i),
            evaluation_key='silver-eval-v1:'+str(i)*64,evidence_ref='silver-receipt://silver-eval-v1:'+str(i)*64) for i in (1,2)]
        atomic_json(self.root/'selection.json',self.window)
        atomic_json(self.root/'silver-state/evaluation-selection.json',seal(self.window,self.choices))
        atomic_json(self.root/'silver-result.json',dict(status='COMPLETED'))
        self.inputs={'reviews':dict(rows=[])};atomic_json(self.root/'silver-inputs.json',self.inputs)
        self.work=SimpleNamespace(ROOT=self.root)
        self.target=self.root/'activation-v2'

    def waiting(self,keys=['danawa:2']):
        atomic_json(self.root/'publish-result.json',dict(reviewed=dict(waiting=len(keys),activated=2-len(keys),waiting_product_keys=keys)))
        return self.helper.waiting_selection(self.root)

    def freeze(self,selection,inputs):
        db=SimpleNamespace(read_snapshot=nullcontext,close=lambda:None)
        with patch('product_pipeline.publication.mysql.MySQL',return_value=db),patch('product_pipeline.review.export_inputs',return_value={'silver':inputs}) as export:
            self.helper.freeze_silver(self.work,{},self.target,selection)
            return export.call_count

    def test_only_waiting_product_and_its_observations_are_reevaluated(self):
        selected=self.waiting()
        self.assertEqual(selected['product_keys'],['danawa:2'])
        self.assertEqual(selected['observation_keys'],['2'*64])
        self.assertEqual(selected['bronze_version'],42)
        self.assertEqual(read_json(self.root/'selection.json'),self.window)

    def test_all_activated_needs_no_second_silver_or_gold(self):
        self.assertIsNone(self.waiting([]))
        result=self.helper.run(self.work,{},{});self.assertEqual(result['reason'],'ALL_PRODUCTS_ALREADY_ACTIVATED')

    def test_invalid_waiting_scope_is_rejected(self):
        with self.assertRaises(Conflict):self.waiting(['danawa:3'])

    def test_legacy_receipt_keeps_full_scope(self):
        atomic_json(self.root/'publish-result.json',dict(reviewed=dict(waiting=1,activated=1)))
        self.assertEqual(self.helper.waiting_selection(self.root),self.window)

    def test_identical_inputs_reuse_completed_parent_selection_and_resume_without_db(self):
        selected=self.waiting();self.freeze(selected,self.inputs)
        result=read_json(self.target/'silver-result.json')
        self.assertTrue(result['reused']);self.assertEqual(result['window_hash'],selected['content_hash'])
        self.assertEqual(read_json(self.target/'silver-state/evaluation-selection.json')['choices'],[self.choices[1]])
        self.assertEqual(self.freeze(selected,{'changed':True}),0)
        self.assertEqual(read_json(self.target/'silver-inputs.json'),self.inputs)

    def test_changed_inputs_require_real_silver_run(self):
        self.freeze(self.waiting(),{'frozen':2})
        self.assertFalse((self.target/'silver-result.json').exists())

    def test_observation_candidates_do_not_synthesize_completion_and_are_frozen(self):
        selected=self.waiting();target=self.root/'activation-v3'
        db=SimpleNamespace(read_snapshot=nullcontext,close=lambda:None)
        with patch('product_pipeline.publication.mysql.MySQL',return_value=db),patch('product_pipeline.review.export_inputs',return_value={'silver':self.inputs}) as export:
            self.helper.freeze_observation_inputs(self.work,{},target,selected)
            self.helper.freeze_observation_inputs(self.work,{},target,selected)
            self.assertEqual(export.call_count,1)
        self.assertFalse((target/'silver-result.json').exists())
        self.assertEqual(read_json(target/'silver-reuse-selection.json')['choices'],[self.choices[1]])
        self.assertEqual(read_json(target/'silver-inputs.json'),self.inputs)

    def test_incomplete_parent_cannot_authorize_reuse(self):
        atomic_json(self.root/'silver-result.json',dict(status='FAILED'))
        with self.assertRaises(Conflict):self.freeze(self.waiting(),self.inputs)

    def test_partial_activation_reuses_committed_substages(self):
        selected=self.waiting();atomic_json(self.root/'post-intake-review.json',dict(created=1))
        atomic_json(self.target/'selection.json',selected)
        atomic_json(self.target/'silver-inputs.json',self.inputs)
        atomic_json(self.target/'silver-result.json',dict(status='COMPLETED'))
        calls=[]
        def run(stage,target):
            calls.append(stage);atomic_json(target/(stage+'-result.json'),dict(status='COMPLETED',reviewed={'activated':1,'waiting':0}))
        self.work.run_at=run
        result=self.helper.run(self.work,{},{});self.helper.run(self.work,{},{})
        self.assertEqual(calls,['gold','publish']);self.assertEqual(result['skipped_already_activated'],1)
