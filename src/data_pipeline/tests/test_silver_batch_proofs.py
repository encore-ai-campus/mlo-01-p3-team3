import tempfile
import unittest
from unittest.mock import patch
from test_silver import sample,inputs,RUN,AS_OF
from test_silver_runtime import Sink,States
from product_pipeline.silver.batch import process_many
from product_pipeline.errors import Conflict,PipelineError


class BatchSink(Sink):
    def __init__(self):super().__init__();self.calls=[]
    def insert(self,table,rows):self.calls.append(('insert',table));super().insert(table,rows)
    def insert_many(self,tables,*,hook=None):
        self.calls.append(('bulk',tuple(tables)))
        for table,rows in tables.items():
            self.insert(table,rows)
            if hook:hook(table)
    def versions_many(self,tables):return {t:self.version(t) for t in tables}
    def verify_many(self,proofs):
        self.calls.append(('verify',tuple(t for t,_,_ in proofs)))
        for table,version,expected in proofs:self.verify(table,expected,version)


class SilverBatchTests(unittest.TestCase):
    def execute(self,doc,state,sink,hook=None):return process_many([doc],state,sink,inputs(),processing_run_id=RUN,
        as_of=AS_OF,bronze_version=1,bronze_path='file:///bronze',hook=hook)

    def test_bulk_verification_precedes_receipt_and_replay_does_not_write(self):
        with tempfile.TemporaryDirectory() as root:
            doc,state=sample(root);sink=BatchSink();self.execute(doc,States(state),sink)
            proof=next(i for i,c in enumerate(sink.calls) if c[0]=='verify' and c[1])
            self.assertLess(proof,sink.calls.index(('insert','pipeline_evaluations')))
            sink.calls=[];out=self.execute(doc,States(state),sink)
            self.assertTrue(out[0]['reused']);self.assertFalse(any(c[0] in {'insert','bulk'} for c in sink.calls))

    def test_failed_output_proof_has_no_receipt_and_recovers(self):
        with tempfile.TemporaryDirectory() as root:
            doc,state=sample(root);sink=BatchSink();original=sink.verify_many
            def fail(proofs):
                if proofs:raise Conflict('missing')
                return original(proofs)
            with patch.object(sink,'verify_many',side_effect=fail):
                with self.assertRaises(Conflict):self.execute(doc,States(state),sink)
            self.assertEqual(sink.data['pipeline_evaluations'],{})
            self.execute(doc,States(state),sink);self.assertEqual(len(sink.data['pipeline_evaluations']),1)

    def test_image_state_change_during_bulk_write_still_blocks_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            doc,state=sample(root);states=States(state);sink=BatchSink()
            def change(table):
                if table=='product_images':states.value['image_state_revision']+=1
            with self.assertRaises(PipelineError):self.execute(doc,states,sink,change)
            self.assertEqual(sink.data['pipeline_evaluations'],{})
