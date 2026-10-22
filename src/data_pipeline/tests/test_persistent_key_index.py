import json,os,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from product_pipeline.spark_session import SessionScope
from product_pipeline.delta_key_index import BYTES,put,get,add,possible,advance


class PersistentIndexTests(unittest.TestCase):
    def sink(self,table_id='original'):
        return SimpleNamespace(paths={'rows':'s3a://bucket/rows'},target_metadata={'rows':{'table_id':table_id}})
    def test_complete_snapshot_survives_driver_scope_but_not_identity_or_version_change(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'PIPELINE_DELTA_KEY_INDEX_DIR':d}):
            if os.name!='nt':Path(d).chmod(0o700)
            bits=bytearray(BYTES);add(bits,'existing')
            with SessionScope():put(self.sink(),'rows',3,bits)
            with SessionScope():
                recovered=get(self.sink(),'rows',3);self.assertTrue(possible(recovered,'existing'))
                self.assertIsNone(get(self.sink(),'rows',4));self.assertIsNone(get(self.sink('recreated'),'rows',3))
    def test_corruption_is_a_cache_miss_and_never_false_absence(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'PIPELINE_DELTA_KEY_INDEX_DIR':d}):
            if os.name!='nt':Path(d).chmod(0o700)
            with SessionScope():put(self.sink(),'rows',1,bytearray(BYTES))
            path=next(Path(d).rglob('1.json'));value=json.loads(path.read_text());value['bits']='00';path.write_text(json.dumps(value))
            with SessionScope():self.assertIsNone(get(self.sink(),'rows',1))
    def test_owned_adjacent_append_is_durable_and_disk_versions_are_bounded(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'PIPELINE_DELTA_KEY_INDEX_DIR':d}):
            if os.name!='nt':Path(d).chmod(0o700)
            with SessionScope():
                put(self.sink(),'rows',1,bytearray(BYTES))
                advance(self.sink(),'rows',1,2,['one']);advance(self.sink(),'rows',2,3,['two'])
            self.assertEqual(len(list(Path(d).rglob('*.json'))),2)
            with SessionScope():
                bits=get(self.sink(),'rows',3);self.assertTrue(possible(bits,'one'));self.assertTrue(possible(bits,'two'))
