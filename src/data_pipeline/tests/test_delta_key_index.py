import unittest
from types import SimpleNamespace
from product_pipeline import delta_key_index as index
from product_pipeline.spark_session import SessionScope


def sink(identity='id'):
    return SimpleNamespace(paths={'rows':'s3a://lake/rows'},target_metadata={'rows':{'table_id':identity}})


class KeyIndexTests(unittest.TestCase):
    def test_added_keys_are_never_reported_absent(self):
        bits=bytearray(index.BYTES);keys=[str(i) for i in range(10000)]
        for key in keys:index.add(bits,key)
        self.assertTrue(all(index.possible(bits,key) for key in keys))

    def test_unknown_version_restart_and_table_recreation_fall_back(self):
        s=sink();keys={'one','two'}
        with SessionScope():
            bits=bytearray(index.BYTES);index.add(bits,'one');index.put(s,'rows',1,bits)
            self.assertEqual(index.candidates(s,'rows',1,keys),{'one'})
            self.assertEqual(index.candidates(s,'rows',2,keys),keys)
            self.assertEqual(index.candidates(sink('new-id'),'rows',1,keys),keys)
        with SessionScope():self.assertEqual(index.candidates(s,'rows',1,keys),keys)

    def test_owned_adjacent_append_keeps_old_and_new_keys(self):
        s=sink()
        with SessionScope():
            bits=bytearray(index.BYTES);index.add(bits,'old');index.put(s,'rows',1,bits)
            index.advance(s,'rows',1,2,['new'])
            self.assertEqual(index.candidates(s,'rows',2,{'old','new','absent'}),{'old','new'})
            index.advance(s,'rows',2,4,['gap'])
            self.assertEqual(index.candidates(s,'rows',4,{'anything'}),{'anything'})

    def test_filter_positive_is_only_a_candidate_not_a_hash_proof(self):
        s=sink()
        with SessionScope():
            index.put(s,'rows',1,bytearray([255])*index.BYTES)
            self.assertEqual(index.candidates(s,'rows',1,{'not-really-present'}),{'not-really-present'})


if __name__=='__main__':unittest.main()
