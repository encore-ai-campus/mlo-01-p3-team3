import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.gold.spool import review_spool


class ReviewSpoolTests(unittest.TestCase):
    def test_large_window_is_reiterable_without_spark_inside_review_transaction(self):
        state={'db':False,'read':0}
        groups=[{'receipt':{'raw':str(i)},'payload':'가'*60} for i in range(8)]
        def source():
            for group in groups:
                self.assertFalse(state['db']);state['read']+=1;yield group
        with tempfile.TemporaryDirectory() as root:
            with review_spool(source(),directory=root,max_bytes=4096,max_group_bytes=300) as spool:
                self.assertEqual(state['read'],8)
                state['db']=True
                self.assertEqual(digest(list(spool)),digest(groups))
                self.assertEqual(digest(list(spool)),digest(groups))
                self.assertEqual(len(list(Path(root).iterdir())),1)
            self.assertEqual(list(Path(root).iterdir()),[])

    def test_single_group_aggregate_disk_limit_and_low_disk_fail_closed(self):
        with self.assertRaises(PipelineError):
            with review_spool([{'x':'a'*100}],max_group_bytes=20):pass
        with self.assertRaises(PipelineError):
            with review_spool([{'x':'a'*30}]*5,max_bytes=100,max_group_bytes=100):pass
        from types import SimpleNamespace
        with patch('product_pipeline.gold.spool.shutil.disk_usage',return_value=SimpleNamespace(free=0)):
            with self.assertRaisesRegex(PipelineError,'DISK_FULL'):
                with review_spool([{'x':'a'}]):pass

    def test_tampered_or_truncated_evidence_is_never_admitted(self):
        with review_spool([{'x':'original'}]) as spool:
            (spool.root/spool.entries[0][0]).write_text('{"x":"changed"}')
            with self.assertRaises(Conflict):list(spool)

    def test_failure_cleans_disposable_files(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(RuntimeError):
                with review_spool([{'x':1}],directory=root):raise RuntimeError('DB failed')
            self.assertEqual(list(Path(root).iterdir()),[])

    def test_pinned_large_groups_reuse_disk_without_crossing_driver_lifetime(self):
        from product_pipeline.spark_session import SessionScope,cache_put,cache_get
        data=[{'product':'1','rows':['한글'*40]}]
        with SessionScope(max_cache_bytes=10,max_disk_cache_bytes=4096) as scope:
            cache_put('silver_groups',('path',1),data)
            self.assertEqual(cache_get('silver_groups',('path',1)),data)
            self.assertIsNone(cache_get('silver_groups',('path',2)))
            cache_get('silver_groups',('path',1))[0]['product']='changed'
            self.assertEqual(cache_get('silver_groups',('path',1)),data)
            root=Path(scope.disk_root.name)
            self.assertEqual(scope.metrics['silver_groups_disk_cache_writes'],1)
        self.assertFalse(root.exists())
        with SessionScope():self.assertIsNone(cache_get('silver_groups',('path',1)))

    def test_disk_cache_eviction_and_integrity_limits(self):
        from product_pipeline.spark_session import SessionScope,cache_put,cache_get
        with SessionScope(max_cache_bytes=5,max_disk_cache_bytes=100) as scope:
            cache_put('silver_groups',1,{'x':'a'*60})
            cache_put('silver_groups',2,{'x':'b'*60})
            self.assertIsNone(cache_get('silver_groups',1))
            path=next(iter(scope.disk_cache.values()))[0];path.write_text('changed')
            with self.assertRaises(PipelineError):cache_get('silver_groups',2)


if __name__=='__main__':unittest.main()
