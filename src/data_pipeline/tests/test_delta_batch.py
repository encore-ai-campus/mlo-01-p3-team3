import copy
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from product_pipeline.delta_batch import insert_many
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict


class BatchLookupTests(unittest.TestCase):
    def row(self,key,value):
        body=dict(id=key,value=value);return dict(body,record_hash=digest(body))
    def sink(self):return SimpleNamespace(settings=object(),lease=SimpleNamespace(check=Mock()),keys={'a':'id','b':'id'},
        hash_row=lambda r:digest({k:v for k,v in r.items() if k!='record_hash'}),_append=Mock())

    def test_commit_response_loss_is_reconciled_from_storage_not_negative_cache(self):
        sink=self.sink();row=self.row('x',1);durable={'a':{},'b':{}}
        def lookup(s,expected):return {t:{k:r['record_hash'] for k,r in durable[t].items() if k in keys} for t,keys in expected.items()}
        def commit(table,rows):
            durable[table].update({r['id']:copy.deepcopy(r) for r in rows})
            if rows:raise OSError('commit reply lost')
        sink._append.side_effect=commit
        with patch('product_pipeline.delta_batch.existing_hashes_many',side_effect=lookup) as read:
            with self.assertRaises(OSError):insert_many(sink,{'a':[row]})
            insert_many(sink,{'a':[row]})
            self.assertEqual(read.call_count,2);self.assertEqual(durable['a'],{'x':row})
            self.assertEqual(sink._append.call_args.args[1],[])

    def test_conflict_in_later_table_prevents_every_write(self):
        sink=self.sink()
        with patch('product_pipeline.delta_batch.existing_hashes_many',return_value={'a':{},'b':{'y':'different'}}):
            with self.assertRaises(Conflict):insert_many(sink,{'a':[self.row('x',1)],'b':[self.row('y',2)]})
        sink._append.assert_not_called()

    def test_duplicate_or_forged_incoming_body_is_rejected_before_lookup(self):
        for rows in [[self.row('x',1)]*2,[dict(self.row('x',1),value=2)]]:
            sink=self.sink()
            with patch('product_pipeline.delta_batch.existing_hashes_many') as read:
                with self.assertRaises(Conflict):insert_many(sink,{'a':rows})
                read.assert_not_called();sink._append.assert_not_called()

    def test_lease_loss_after_lookup_cannot_append(self):
        sink=self.sink();sink.lease.check.side_effect=[None,Conflict('lease lost')]
        with patch('product_pipeline.delta_batch.existing_hashes_many',return_value={'a':{}}):
            with self.assertRaises(Conflict):insert_many(sink,{'a':[self.row('x',1)]})
        sink._append.assert_not_called()
