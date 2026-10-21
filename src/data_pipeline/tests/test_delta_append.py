"""Replay after an uncertain append must discover already committed rows."""
import copy,json,sys,unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.silver.delta import DeltaSink
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict


class AppendReplayTests(unittest.TestCase):
    def test_lost_commit_response_then_retry_appends_no_duplicate_or_old_row(self):
        sink=DeltaSink.__new__(DeltaSink);sink.settings=object();sink.lease=SimpleNamespace(check=MagicMock())
        sink.paths={'proof':'file:///isolated'};sink.keys={'proof':'id'};sink.schemas={'proof':[]};sink._versions={'proof':5}
        sink.hash_row=lambda row:digest({k:v for k,v in row.items() if k!='record_hash'})
        def row(key,value):
            r=dict(id=key,value=value);return dict(r,record_hash=sink.hash_row(r))
        old,new=row('old','first'),row('new','second');durable={'old':old}
        sink.existing=lambda table,keys:copy.deepcopy({k:v for k,v in durable.items() if k in keys})
        functions=MagicMock();payload=[]
        functions.lit.side_effect=lambda value:payload.append(json.loads(value)) or value
        sink.spark=MagicMock();frame=sink.spark.range.return_value.select.return_value.select.return_value
        save=frame.write.format.return_value.mode.return_value.option.return_value.save
        def append_then_lose_response(path):
            self.assertEqual(payload[-1],[new]);durable.update({r['id']:r for r in payload[-1]})
            raise OSError('commit acknowledgement lost')
        save.side_effect=append_then_lose_response
        modules={'pyspark.sql':SimpleNamespace(functions=functions),'pyspark.sql.types':SimpleNamespace(ArrayType=lambda schema:schema)}
        with patch.dict(sys.modules,modules):
            with self.assertRaises(OSError):sink.insert('proof',[old,new])
            sink.insert('proof',[old,new])
            self.assertEqual(save.call_count,1);self.assertEqual(durable,{'old':old,'new':new})
            with self.assertRaises(Conflict):sink.insert('proof',[row('new','conflicting')])
            with self.assertRaises(Conflict):sink.insert('proof',[new,new])
        self.assertNotIn('proof',sink._versions)


if __name__=='__main__':unittest.main()
