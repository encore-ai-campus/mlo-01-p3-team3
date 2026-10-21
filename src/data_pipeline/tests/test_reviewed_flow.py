from contextlib import nullcontext
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from publication_fixture import fixture
from product_pipeline.errors import PipelineError, Conflict
from product_pipeline.publication.reviewed import publish_reviewed


class ReviewedFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        plan,_=fixture(self.root/'fixture','a'*64)
        self.group=plan['products'][0]['group']
        self.source=SimpleNamespace(manifest=dict(gold_versions=plan['gold_versions'],content_hash=plan['export_hash'],
            as_of=plan['as_of'],product_count=1,input_refs=dict(inputs='inputs')),
            price_history_mode=plan['price_history_mode'],price_max_age_seconds=plan['price_max_age_seconds'],
            groups=lambda:iter([self.group]),read=lambda _:dict(policy={}))
        self.db=SimpleNamespace(target='a'*64,read_snapshot=nullcontext)
        self.guard=patch('product_pipeline.publication.reviewed.approve_guard',return_value=dict(guard_hash='b'*64)).start()
        self.publish=patch('product_pipeline.publication.reviewed.publish',return_value=dict(status='COMPLETED')).start()
        self.audit=patch('product_pipeline.publication.reviewed.audit_publication',return_value=dict(status='VERIFIED')).start()
        self.addCleanup(patch.stopall)

    def tearDown(self):self.tmp.cleanup()

    def test_existing_approvals_activate_with_fixed_gold_and_reuse_saved_plan(self):
        out=self.root/'plans'
        result=publish_reviewed(self.source,self.db,out)
        self.assertEqual(result['activated'],1)
        self.assertTrue(self.publish.call_args.args[0]['publication_mode']=='ACTIVATE_REVIEWED')
        publish_reviewed(self.source,self.db,out)
        self.assertEqual(self.guard.call_count,1)
        self.assertEqual(self.publish.call_count,2)

    def test_quality_wait_never_creates_guard_or_activation(self):
        self.group['tables']['product_catalog'][0]['catalog_ready']=False
        result=publish_reviewed(self.source,self.db,self.root/'plans')
        self.assertEqual(result['waiting'],1);self.guard.assert_not_called();self.publish.assert_not_called()
        self.assertEqual(result['waiting_product_keys'],[self.group['receipt']['product_key']])

    def test_missing_owner_decision_is_recorded_as_wait_not_fabricated(self):
        self.guard.side_effect=PipelineError('REVIEW_APPROVAL_NOT_PROVIDED')
        result=publish_reviewed(self.source,self.db,self.root/'plans')
        self.assertEqual(result['waiting'],1);self.publish.assert_not_called()

    def test_input_conflict_fails_instead_of_being_hidden_as_pending(self):
        self.guard.side_effect=Conflict('REVIEW_GOLD_OWNER_INPUTS_CHANGED')
        with self.assertRaises(Conflict):publish_reviewed(self.source,self.db,self.root/'plans')
        self.publish.assert_not_called()
