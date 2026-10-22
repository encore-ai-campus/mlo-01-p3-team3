import copy,sys,unittest
from types import SimpleNamespace
from unittest.mock import Mock,MagicMock,patch
from product_pipeline.spark_session import SessionScope,metrics
from product_pipeline.contracts import digest
from product_pipeline.silver.normalize import semantic_hash
from product_pipeline.silver.append_bridge import remember,restore,record_append
from product_pipeline.silver.delta import DeltaSink
from product_pipeline.delta_proofs import versions_many,promote_appends
from product_pipeline.errors import PipelineError,Conflict


class AppendBridgeTests(unittest.TestCase):
    def row(self,key,product='danawa:1'):
        r=dict(evaluation_key=key,product_key=product,value=key);return dict(r,record_hash=semantic_hash(r))
    def save(self,rows=None,version=2,identity='id'):
        remember('s3a://test/silver',['danawa:1'],{'product_observations':version},{'product_observations':identity},
            {'product_observations':rows or [self.row('old')]},max_rows=100,max_bytes=100000)
    def get(self,version=3,identity='id',max_bytes=100000):
        return restore('s3a://test/silver',['danawa:1'],{'product_observations':version},{'product_observations':identity},max_rows=100,max_bytes=max_bytes)
    def edge(self,base=2,version=3,rows=None):
        record_append('s3a://test/silver/product_observations','id',base,version,rows or [self.row('new')])
    def test_exact_owned_chain_merges_only_requested_products_and_keeps_base_immutable(self):
        with SessionScope():
            self.save();self.edge(rows=[self.row('new'),self.row('other','danawa:2')])
            result=self.get();self.assertEqual([r['evaluation_key'] for r in result['product_observations']],['old','new'])
            self.assertEqual(len(self.get(2)['product_observations']),1)
            self.assertEqual(metrics()['silver_snapshot_bridge_edges'],1)
    def test_unknown_gap_recreated_table_old_version_and_restart_require_full_read(self):
        with SessionScope():
            self.save();self.assertIsNone(self.get());self.edge()
            self.assertIsNone(self.get(4));self.assertIsNone(self.get(identity='different'));self.assertIsNone(self.get(1))
            self.edge(3,4,[self.row('newer')]);self.assertEqual(len(self.get(4)['product_observations']),3)
        with SessionScope():self.assertIsNone(self.get())
    def test_non_adjacent_append_is_never_evidence(self):
        with SessionScope():self.save();self.edge(2,4);self.assertIsNone(self.get(4))
    def test_waiting_subset_can_reuse_a_complete_parent_product_scope(self):
        with SessionScope():
            remember('s3a://test/silver',['danawa:1','danawa:2'],{'product_observations':2},{'product_observations':'id'},
                {'product_observations':[self.row('one'),self.row('two','danawa:2')]},max_rows=100,max_bytes=100000)
            self.edge();self.assertEqual([r['evaluation_key'] for r in self.get()['product_observations']],['one','new'])
    def test_changed_body_duplicate_and_history_budget_are_not_silently_accepted(self):
        for rows in ([dict(self.row('new'),value='forged')],[self.row('old')]):
            with SessionScope():
                self.save();self.edge(rows=rows)
                with self.assertRaises(Conflict):self.get()
        with SessionScope():
            self.save(rows=[self.row('old')]*100);self.edge()
            with self.assertRaisesRegex(PipelineError,'GOLD_PRODUCT_HISTORY_LIMIT'):self.get()


class ReceiptPreflightTests(unittest.TestCase):
    def sink(self):
        s=DeltaSink.__new__(DeltaSink);s.settings=object();s.lease=SimpleNamespace(check=Mock());s._versions={'a':2}
        s._batch_reads=None;s._read=Mock(return_value={});return s
    def test_absence_is_reused_only_within_one_batch_and_same_version(self):
        sink=self.sink()
        with sink.batch_scope():
            sink.existing('a',{'one','two'});sink.existing('a',{'one'});self.assertEqual(sink._read.call_count,1)
            sink._versions['a']=3;sink.existing('a',{'one'});self.assertEqual(sink._read.call_count,2)
        with sink.batch_scope():sink.existing('a',{'one'})
        self.assertEqual(sink._read.call_count,3);self.assertIsNone(sink._batch_reads)
    def test_failed_batch_drops_negative_reads_and_unverified_append_claims(self):
        sink=self.sink()
        with self.assertRaises(OSError):
            with sink.batch_scope():
                sink.existing('a',{'one'});sink._pending_appends={'a':{'unconfirmed':True}};raise OSError('lost reply')
        self.assertEqual(sink._pending_appends,{})
        with sink.batch_scope():sink.existing('a',{'one'})
        self.assertEqual(sink._read.call_count,2)


class OwnershipTests(unittest.TestCase):
    def sink(self,**changes):
        pending=dict(base=2,token='own-token',expected={'new':'hash'})
        s=SimpleNamespace(settings=object(),lease=SimpleNamespace(check=Mock()),_versions={},_pending_appends={'a':pending},
            paths={'a':'s3a://test/a'},spark=MagicMock(),target_metadata={'a':{'table_id':'id'}})
        row=dict(_table='a',version=3,operation='WRITE',operationParameters={'mode':'Append'},userMetadata='own-token');row.update(changes)
        table=MagicMock();table.forPath.return_value.history.return_value.select.return_value.collect.return_value=[row]
        return s,table
    def test_only_own_adjacent_append_transaction_can_be_bridged(self):
        functions=MagicMock()
        for changes in ({},{'version':4},{'userMetadata':'other'},{'operationParameters':{'mode':'Overwrite'}}):
            sink,table=self.sink(**changes)
            with patch.dict(sys.modules,{'delta.tables':SimpleNamespace(DeltaTable=table),'pyspark.sql':SimpleNamespace(functions=functions)}):
                versions_many(sink,['a'])
            self.assertEqual('a' in sink._pending_appends,not bool(changes))
    def test_every_actual_appended_row_must_be_confirmed_before_promoting_edge(self):
        sink,_=self.sink();sink._pending_appends['a']['version']=3;sink._append_reads={}
        with patch('product_pipeline.silver.append_bridge.record_append') as record:
            promote_appends(sink);record.assert_not_called()
            sink._append_reads={('a',3):{'new':{'record_hash':'wrong'}}}
            promote_appends(sink);record.assert_not_called()
            sink._append_reads={('a',3):{'new':{'record_hash':'hash'}}}
            promote_appends(sink);record.assert_called_once();self.assertEqual(sink._pending_appends,{})


class AggregateReadTests(unittest.TestCase):
    def read(self,stats):
        from product_pipeline.bounded_read import payload_rows
        frame=MagicMock();cached=frame.limit.return_value.withColumn.return_value.withColumn.return_value.persist.return_value
        cached.agg.return_value.first.return_value=stats
        cached.collect.return_value=[{'payload':'safe','_payload_bytes':4}]
        functions=MagicMock();functions.col.return_value.__le__=Mock(return_value=True)
        modules={'pyspark.sql':SimpleNamespace(functions=functions),'pyspark':SimpleNamespace(StorageLevel=SimpleNamespace(DISK_ONLY='disk'))}
        with patch.dict(sys.modules,modules):
            try:return list(payload_rows(frame,max_rows=2,max_bytes=8,error_code='LIMIT',mode='aggregate'))
            finally:
                cached.unpersist.assert_called_once()
                if stats['count']>2 or stats['bytes']>8 or stats['invalid']:cached.collect.assert_not_called()
    def test_default_candidate_collects_only_after_confirmed_row_byte_and_null_limits(self):
        self.assertEqual(len(self.read(dict(count=1,bytes=4,invalid=0))),1)
        for stats in (dict(count=3,bytes=4,invalid=0),dict(count=1,bytes=9,invalid=0),dict(count=1,bytes=4,invalid=1)):
            with self.assertRaisesRegex(PipelineError,'LIMIT'):self.read(stats)


if __name__=='__main__':unittest.main()
