import unittest
from product_pipeline.bronze.delta import write_chunks,DeltaSink


class ChunkTests(unittest.TestCase):
    def test_verified_existing_chunk_needs_no_new_spark_write(self):
        from unittest.mock import MagicMock,patch
        import sys,types
        row=dict(event_id='event1',event_hash='h',event_json='{}',raw_document_id='raw1',document_hash='d')
        class Sink:
            lease=MagicMock();settings=object();spark=MagicMock()
            def existing(self,table,keys):return {'event1':row}
        fake=types.ModuleType('delta.tables');fake.DeltaTable=MagicMock()
        with patch.dict(sys.modules,{'delta.tables':fake}):
            DeltaSink._insert_chunk(Sink(),'events',[row])
        Sink.spark.range.assert_not_called()
    def test_bytes_and_count_preserve_order_without_splitting_one_document(self):
        rows=[dict(id=i,raw='가'*40) for i in range(5)]
        chunks=list(write_chunks(rows,max_bytes=300,max_records=2))
        self.assertEqual([r for chunk in chunks for r in chunk],rows)
        self.assertTrue(all(len(c)<=2 for c in chunks))
        self.assertEqual(list(write_chunks([dict(raw='x'*1000)],max_bytes=10)),[[dict(raw='x'*1000)]])

    def test_partial_chunk_failure_replays_same_keys_without_marking_batch_done(self):
        committed={};armed=[True]
        class Sink:
            def _insert_chunk(self,table,rows):
                if rows[0]['id']==10 and armed[0]:armed[0]=False;raise OSError('injected storage failure')
                for row in rows:committed.setdefault(row['id'],row)
        sink=Sink();rows=[dict(id=i) for i in range(23)]
        with self.assertRaises(OSError):DeltaSink.insert(sink,'raw',rows)
        self.assertEqual(len(committed),10)
        DeltaSink.insert(sink,'raw',rows)
        self.assertEqual(list(committed),list(range(23)))


if __name__=='__main__':unittest.main()
