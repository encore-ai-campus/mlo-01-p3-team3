import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from product_pipeline.delta_proofs import verify_many,_chunks
from product_pipeline.errors import Conflict


class ProofTests(unittest.TestCase):
    def sink(self):
        sink=SimpleNamespace(settings=object(),lease=SimpleNamespace(check=Mock()),_proofs={})
        sink.versions_many=Mock(return_value={'a':5,'b':6})
        return sink

    def test_historical_and_current_versions_are_both_checked_in_one_batch(self):
        sink=self.sink()
        with patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(sink,[('a',4,{'k':'h'}),('b',6,{'q':'j'})])
            self.assertEqual(read.call_count,1)
            self.assertEqual(read.call_args.args[1],[('a',4,{'k':'h'}),('a',5,{'k':'h'}),('b',6,{'q':'j'})])
            verify_many(sink,[('a',4,{'k':'h'}),('b',6,{'q':'j'})])
            self.assertEqual(read.call_count,1)

    def test_failure_never_accepts_partial_proofs(self):
        sink=self.sink()
        with patch('product_pipeline.delta_proofs._verify_rows',side_effect=Conflict('bad')):
            with self.assertRaises(Conflict):verify_many(sink,[('a',5,{'k':'h'})])
        self.assertEqual(sink._proofs,{})

    def test_new_current_version_is_checked_again(self):
        sink=self.sink();sink._proofs={('a',4):{'k':'h'},('a',5):{'k':'h'}}
        sink.versions_many.return_value={'a':6}
        with patch('product_pipeline.delta_proofs._verify_rows') as read:
            verify_many(sink,[('a',4,{'k':'h'})])
            self.assertEqual(read.call_args.args[1],[('a',6,{'k':'h'})])

    def test_conflicting_expectations_are_rejected(self):
        with self.assertRaises(Conflict):verify_many(self.sink(),[('a',5,{'k':'h'}),('a',5,{'k':'changed'})])

    def test_large_requests_split_without_losing_keys(self):
        expected={str(i):'h' for i in range(10001)}
        batches=list(_chunks([('a',5,expected)]))
        self.assertEqual(len(batches),3)
        self.assertTrue(all(sum(len(e) for _,_,e in b)<=5000 for b in batches))
        self.assertEqual({k:v for b in batches for _,_,e in b for k,v in e.items()},expected)
