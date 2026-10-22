"""Crash recovery, exact acknowledgements, expiry and bounded work scopes."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict
from product_pipeline.files import read_json,sha_file
from product_pipeline.incremental.policy import snapshot,first_stage,batches
from product_pipeline.incremental.service import discover,execute
from product_pipeline.infrastructure.incremental_state import IncrementalState
from product_pipeline.operations.queue import Queue

AT='2026-10-22T00:00:00.000Z'
LATER='2026-10-23T00:00:00.000Z'


def product(key='danawa:1',revision=1,stage='silver'):
    return snapshot(key,[dict(observation_key=digest(key),raw_document_id=digest(key)[:24],revision=revision)],
        {'PRODUCT:'+key:dict(hash=digest(revision),stage=stage)})


class Source:
    def __init__(self,values):self.values=values;self.acks=[]
    def snapshots(self,**kwargs):yield from self.values
    def acknowledge(self,captures):self.acks.extend(copy.deepcopy(captures))
    def discovery_completed(self):pass


class Backend:
    def __init__(self):self.calls=[];self.valid=True;self.fail=None
    def current(self,captures):return self.valid
    def freeze(self,directory,captures):return captures[0]['first_stage']
    def run(self,stage,directory):
        self.calls.append(stage)
        if stage==self.fail:raise RuntimeError('worker terminated')
        return dict(status='COMPLETED')
    def verify(self,directory,captures):
        return {c['product_key']:dict(status='QUALITY_WAIT',next_due_at=None,artifacts=[],publication_hash=None) for c in captures}


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.state=IncrementalState(self.root/'state','test')
        self.queue=Queue(self.root/'queue','test',clock=lambda:AT)

    def schedule(self,value=None):
        source=Source([value or product()]);discover(source,self.state,self.queue,at=AT)
        task=self.queue.claim();return task,source

    def test_noop_and_quality_wait_do_not_schedule_repeated_work(self):
        task,source=self.schedule();backend=Backend()
        execute(task['spec'],self.state,backend)
        self.assertEqual(discover(source,self.state,self.queue,at=AT)['scheduled'],0)
        self.assertEqual(backend.calls,['silver','gold','publish','activation'])

    def test_queue_failure_never_acknowledges_source_change(self):
        source=Source([product()])
        with patch.object(self.queue,'enqueue',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):discover(source,self.state,self.queue,at=AT)
        self.assertEqual(source.acks,[])
        self.assertEqual(discover(source,self.state,self.queue,at=AT)['scheduled'],1)

    def test_new_change_arriving_during_work_cannot_be_acknowledged_by_old_work(self):
        first=self.state.capture(product(),at=AT)
        second=self.state.capture(product(revision=2),at=AT)
        self.assertFalse(self.state.complete(first,dict(status='COMPLETED')))
        self.assertEqual(self.state.get(first['product_key'])['generation'],second['generation'])
        self.assertNotIn('completed',self.state.get(first['product_key']))

    def test_crash_resumes_verified_stage_files(self):
        task,_=self.schedule();backend=Backend();backend.fail='publish'
        with self.assertRaises(RuntimeError):execute(task['spec'],self.state,backend)
        self.assertNotIn('completed',self.state.get('danawa:1'))
        backend.fail=None;execute(task['spec'],self.state,backend)
        self.assertEqual(backend.calls,['silver','gold','publish','publish','activation'])

    def test_changed_source_before_publication_supersedes_without_mysql_write(self):
        task,_=self.schedule();backend=Backend()
        with patch.object(backend,'current',side_effect=[True,False]):
            self.assertEqual(execute(task['spec'],self.state,backend)['status'],'SUPERSEDED')
        self.assertNotIn('publish',backend.calls)
        self.assertIn('danawa:1',list(self.state.due_keys(AT)))

    def test_expiry_schedules_even_when_source_content_did_not_change(self):
        capture=self.state.capture(product(),at=AT)
        self.state.complete(capture,dict(next_due_at=LATER,artifacts=[],publication_hash=None))
        self.assertIsNone(self.state.capture(product(),at=AT))
        due=self.state.capture(product(),at=LATER)
        self.assertEqual(due['first_stage'],'gold')

    def test_gold_only_dependency_does_not_require_silver(self):
        original=product(stage='gold');changed=copy.deepcopy(original)
        changed['dependencies']['PRODUCT:danawa:1']['hash']=digest(2)
        self.assertEqual(first_stage(original,changed),'gold')
        changed['dependencies']['PRODUCT:danawa:1']['stage']='silver'
        self.assertEqual(first_stage(original,changed),'silver')

    def test_observation_limit_is_respected_across_product_batches(self):
        captures=[]
        for i in range(205):captures.append(dict(snapshot=product('danawa:'+str(i)),first_stage='gold'))
        self.assertEqual([len(b) for b in batches(captures)],[100,100,5])

    def test_work_tampering_is_blocked_before_running_stages(self):
        task,_=self.schedule();Path(task['spec']['work']).write_text('{}')
        backend=Backend()
        with self.assertRaises(Conflict):execute(task['spec'],self.state,backend)
        self.assertEqual(backend.calls,[])

    def test_reverse_dependency_lookup_is_product_scoped(self):
        self.state.capture(product(),at=AT);self.state.capture(product('danawa:2'),at=AT)
        self.assertEqual(list(self.state.affected(['PRODUCT:danawa:1'])),['danawa:1'])

    def test_nightly_reconciliation_detects_external_publication_change(self):
        p=product();capture=self.state.capture(p,at=AT)
        self.state.complete(capture,dict(next_due_at=None,artifacts=[],publication_hash='original'))
        p['publication_hash']='changed'
        self.assertIsNone(self.state.capture(p,at=AT,full=False))
        self.assertIsNotNone(self.state.capture(p,at=AT,full=True))

    def test_reverted_change_fences_queued_generation(self):
        task,_=self.schedule();execute(task['spec'],self.state,Backend())
        changed=product(revision=2);source=Source([changed])
        discover(source,self.state,self.queue,at=AT)
        queued=self.state.get('danawa:1')['desired']
        self.assertIsNone(self.state.capture(product(),at=AT))
        self.assertFalse(self.state.current([queued]))
        self.assertFalse(self.state.complete(queued,dict(status='COMPLETED')))
        self.assertIsNotNone(self.state.capture(product(revision=3),at=AT))

    def test_full_reconciliation_rebuilds_derived_indexes_without_work(self):
        p=product();capture=self.state.capture(p,at=AT)
        self.state.complete(capture,dict(next_due_at=LATER,artifacts=[],publication_hash=None))
        dependency=self.state.root/'dependencies'/digest('PRODUCT:danawa:1')/(digest('danawa:1')+'.json')
        deadline=self.state.root/'deadlines'/LATER[:10]/(digest('danawa:1')+'.json')
        dependency.unlink();deadline.unlink()
        self.assertIsNone(self.state.capture(p,at=AT,full=True))
        self.assertEqual(list(self.state.affected(['PRODUCT:danawa:1'])),['danawa:1'])
        self.assertTrue(deadline.is_file())
        self.assertEqual(set(self.state.due_keys(LATER)),{'danawa:1'})


if __name__=='__main__':unittest.main()
