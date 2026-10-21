import tempfile
import unittest
from unittest.mock import patch
from test_gold import group,side,VERSIONS,MemoryGoldSink
from test_silver import RUN,AS_OF
from product_pipeline.gold.batch import process_many
from product_pipeline.errors import Conflict


class BatchSink(MemoryGoldSink):
    def __init__(self):super().__init__();self.calls=[]
    def insert(self,table,rows):self.calls.append(('insert',table));super().insert(table,rows)
    def insert_many(self,tables,*,hook=None):
        self.calls.append(('bulk',tuple(tables)))
        for table,rows in tables.items():
            self.insert(table,rows)
            if hook:hook(table)
    def versions_many(self,tables):
        self.calls.append(('versions',tuple(tables)));return {t:self.version(t) for t in tables}
    def verify_many(self,proofs):
        self.calls.append(('verify',tuple(t for t,_,_ in proofs)))
        for table,version,expected in proofs:self.verify(table,expected,version)


class BatchTests(unittest.TestCase):
    def run_batch(self,g,sink):return process_many([[g]],side(),VERSIONS,sink,processing_run_id=RUN,as_of=AS_OF)
    def test_all_output_proofs_precede_receipt_and_replay_does_not_write(self):
        with tempfile.TemporaryDirectory() as root:
            g=group(root);sink=BatchSink();self.run_batch(g,sink)
            proof=next(i for i,c in enumerate(sink.calls) if c[0]=='verify' and c[1])
            receipt=sink.calls.index(('insert','pipeline_evaluations'))
            self.assertLess(proof,receipt)
            self.assertTrue(all(c[0]!='insert' or c[1]=='pipeline_evaluations' for c in sink.calls[proof+1:]))
            sink.calls=[];result=self.run_batch(g,sink)
            self.assertTrue(result[0]['reused']);self.assertFalse(any(c[0]=='insert' for c in sink.calls))

    def test_failed_proof_leaves_no_receipt_and_partial_outputs_resume(self):
        with tempfile.TemporaryDirectory() as root:
            g=group(root);sink=BatchSink();original=sink.verify_many
            def fail(proofs):
                if proofs:raise Conflict('output_missing')
                return original(proofs)
            with patch.object(sink,'verify_many',side_effect=fail):
                with self.assertRaises(Conflict):self.run_batch(g,sink)
            self.assertEqual(sink.data['pipeline_evaluations'],{})
            self.run_batch(g,sink);self.assertEqual(len(sink.data['pipeline_evaluations']),1)
