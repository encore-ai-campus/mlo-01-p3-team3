"""V2 semantic reuse must retain real proofs and time-dependent behavior."""
import copy
import tempfile
import unittest
from test_gold import group,side,seal,ready,VERSIONS,RUN,AS_OF,MemoryGoldSink
from product_pipeline.gold.build import build
from product_pipeline.gold.runner import process
from product_pipeline.gold.batch import process_many
from product_pipeline.gold.contracts import semantic_hash
from product_pipeline.gold.identity import MODE
from product_pipeline.publication.source import GoldSource
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict


class GoldIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.group=group(self.temp.name);self.inputs=side()
        self.inputs['policy']['evaluation_identity']=MODE;seal(self.inputs)

    def build(self,**kwargs):
        return build([self.group],self.inputs,kwargs.get('versions',VERSIONS),
            processing_run_id=RUN,as_of=kwargs.get('at',AS_OF))

    def test_unrelated_table_versions_and_day_change_reuse_without_delta_writes(self):
        sink=MemoryGoldSink();refs=dict(inputs='old-input',versions='old-version',silver_root='same')
        first=process([self.group],self.inputs,VERSIONS,sink,processing_run_id=RUN,as_of=AS_OF,input_refs=refs)
        before=copy.deepcopy(sink.data);changed=dict(inputs='new-input',versions='new-version',silver_root='same')
        second=process([self.group],self.inputs,{k:v+10 for k,v in VERSIONS.items()},sink,
            processing_run_id=RUN,as_of='2026-10-18T00:00:00.000Z',input_refs=changed)
        self.assertTrue(second['reused']);self.assertEqual(first['evaluation_key'],second['evaluation_key'])
        self.assertEqual(before,sink.data)
        self.assertEqual(second['receipt_binding']['validated_input_refs_hash'],digest(changed))

    def test_unrelated_review_set_and_other_product_rows_do_not_change_identity(self):
        first=self.build()['evaluation_key']
        self.inputs['policy']['review_set_hash']='other-ledger-hash'
        self.inputs['product_states'].update(status='PROVIDED',evidence_ref='fixture://new',rows=[dict(
            product_key='danawa:unrelated',evidence_ref='fixture://other',active=True)])
        seal(self.inputs)
        self.assertEqual(first,self.build()['evaluation_key'])

    def test_policy_change_invalidates_affected_product(self):
        first=self.build()['evaluation_key']
        self.inputs['policy']['price_max_age_seconds']+=1;seal(self.inputs)
        self.assertNotEqual(first,self.build()['evaluation_key'])

    def test_expiry_changes_semantics_and_does_not_reuse_ready_result(self):
        self.group,self.inputs=ready(self.group,self.inputs);seal(self.inputs)
        ready_result=self.build()
        expired=self.build(at='2026-12-01T00:00:00.000Z')
        self.assertNotEqual(ready_result['evaluation_key'],expired['evaluation_key'])
        self.assertFalse(expired['tables']['product_catalog'][0]['recommendation_ready'])
        self.assertIsNotNone(ready_result['next_due_at'])

    def test_original_receipt_provenance_is_still_hashed(self):
        sink=MemoryGoldSink();process([self.group],self.inputs,VERSIONS,sink,processing_run_id=RUN,as_of=AS_OF)
        receipt=next(iter(sink.data['pipeline_evaluations'].values()));before=receipt['record_hash']
        receipt['input_refs_json']='{"inputs":"tampered"}'
        self.assertNotEqual(before,semantic_hash(receipt))

    def test_batch_mode_uses_same_semantic_identity_and_reuses_proofs(self):
        sink=MemoryGoldSink()
        initial=process_many([[self.group]],self.inputs,VERSIONS,sink,processing_run_id=RUN,as_of=AS_OF)
        result=process_many([[self.group]],self.inputs,{k:v+1 for k,v in VERSIONS.items()},sink,
            processing_run_id=RUN,as_of=AS_OF)
        self.assertEqual(initial[0]['evaluation_key'],result[0]['evaluation_key']);self.assertTrue(result[0]['reused'])

    def test_publication_requires_explicit_verified_reuse_binding(self):
        refs=dict(inputs='original',versions='versions',silver_root='same')
        sink=MemoryGoldSink();result=process([self.group],self.inputs,VERSIONS,sink,
            processing_run_id=RUN,as_of=AS_OF,input_refs=refs)
        receipt=next(iter(sink.data['pipeline_evaluations'].values()))
        source=GoldSource.__new__(GoldSource);source.manifest=dict(input_refs=refs);source.read=lambda _:{}
        source._check_refs(dict(receipt=receipt),dict(receipt_binding=result['receipt_binding']))
        with self.assertRaises(Conflict):source._check_refs(dict(receipt=receipt),{})
        source.manifest['input_refs']={**refs,'inputs':'different'}
        with self.assertRaises(Conflict):source._check_refs(dict(receipt=receipt),dict(receipt_binding=result['receipt_binding']))


if __name__=='__main__':unittest.main()
