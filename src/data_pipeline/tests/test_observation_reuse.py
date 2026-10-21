import copy,tempfile,unittest
from unittest.mock import patch
from test_silver import sample,inputs,reseal,RUN,AS_OF
from test_silver_batch_proofs import BatchSink
from product_pipeline.silver.batch import process_many
from product_pipeline.silver.normalize import normalize
from product_pipeline.errors import Conflict,PipelineError


class States:
    def __init__(self,values):self.values=values
    def get(self,key,observation):return copy.deepcopy(self.values[key])


class ObservationReuseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        doc,state=sample(self.tmp.name);other=copy.deepcopy(doc);second=copy.deepcopy(state)
        other['_id']='abcdefabcdefabcdefabcdef';other['payload']['listings'][0]['product_name']='다른 관측';reseal(other,second)
        second['raw_document_id']=other['_id']
        self.docs=[doc,other];self.states=States({doc['_id']:state,other['_id']:second});self.sink=BatchSink()
        self.inputs=inputs(reviews=[])
        first=self.execute()
        self.choices={d['_id']:dict(raw_document_id=d['_id'],observation_key=d['observation_key'],
            product_key='danawa:'+d['source_product_id'],evaluation_key=r['evaluation_key']) for d,r in zip(self.docs,first)}
    def execute(self,*,at=AS_OF,hook=None):
        return process_many(self.docs,self.states,self.sink,self.inputs,processing_run_id=RUN,as_of=at,
            bronze_version=1,bronze_path='file:///bronze',reuse_choices=getattr(self,'choices',None),hook=hook)
    def test_only_observation_with_image_revision_change_is_normalized(self):
        old=copy.deepcopy(self.sink.data['pipeline_evaluations'])
        self.states.values[self.docs[1]['_id']]['image_state_revision']+=1
        with patch('product_pipeline.silver.batch.normalize',wraps=normalize) as run:out=self.execute()
        self.assertEqual(run.call_count,1);self.assertEqual([r['normalization_skipped'] for r in out],[True,False])
        for key,value in old.items():self.assertEqual(self.sink.data['pipeline_evaluations'][key],value)
        self.assertEqual(len(self.sink.data['pipeline_evaluations']),3)
    def test_unrelated_approval_changes_do_not_normalize_either_observation(self):
        self.inputs=inputs(reviews=[dict(source_code='danawa',source_product_id='999',source_version='v',checksum='c',scopes=['DISPLAY'],decision='APPROVED')])
        with patch('product_pipeline.silver.batch.normalize',wraps=normalize) as run:out=self.execute()
        self.assertEqual(run.call_count,0);self.assertTrue(all(r['normalization_skipped'] for r in out))
    def test_related_approval_and_expiration_change_evaluation_identity(self):
        image=self.docs[0]['image_refs'][0]
        self.inputs=inputs(reviews=[dict(source_code='danawa',source_product_id='11',source_version=image['source_version'],
            checksum=image['sha256'],scopes=['DISPLAY'],decision='APPROVED',expires_at='2026-10-17T00:00:00Z')])
        with patch('product_pipeline.silver.batch.normalize',wraps=normalize) as run:self.execute()
        self.assertEqual(run.call_count,2)
        # Candidate identity must not hide a crossed effective-date boundary.
        out=self.execute();self.choices={d['_id']:{**self.choices[d['_id']],'evaluation_key':r['evaluation_key']} for d,r in zip(self.docs,out)}
        with patch('product_pipeline.silver.batch.normalize',wraps=normalize) as run:self.execute(at='2026-10-18T00:00:00Z')
        self.assertEqual(run.call_count,2)
    def test_missing_receipt_or_output_blocks_reuse(self):
        self.sink.data['pipeline_evaluations'].clear()
        with self.assertRaises(Conflict):self.execute()
    def test_corrupted_output_blocks_reuse(self):
        self.sink.data['product_images'].clear()
        with self.assertRaises(Conflict):self.execute()
    def test_changed_live_image_during_reuse_blocks_completion(self):
        original=self.sink.verify_many
        def change(proofs):
            original(proofs)
            if proofs:self.states.values[self.docs[0]['_id']]['image_state_revision']+=1
        with patch.object(self.sink,'verify_many',side_effect=change):
            with self.assertRaises(PipelineError):self.execute()
    def test_wrong_observation_binding_is_rejected(self):
        self.choices[self.docs[0]['_id']]['observation_key']='0'*64
        with self.assertRaises(Conflict):self.execute()
