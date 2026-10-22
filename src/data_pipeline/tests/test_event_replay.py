import copy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from tests.test_bronze import BronzeTests
from product_pipeline.errors import PipelineError,Conflict
from product_pipeline.event_replay import freeze,replay
from product_pipeline.files import read_json,atomic_json


class ReplayTests(BronzeTests):
    # Reuse the real envelope fixture; exercise replay rather than inherited cases.
    def saved(self):
        return dict(event=copy.deepcopy(self.event),document=copy.deepcopy(self.doc),
                    observation_key=self.doc['observation_key'],raw_confirmed=True,event_confirmed=True)
    def directory(self):return Path(self.temp.name)/'replay'
    def seal(self,records=None,count=1):
        return freeze(records or [self.saved()],self.directory(),run_id=self.doc['collection_run_id'],expected_count=count)
    def producer(self,error=None):
        p=MagicMock()
        p.produce.side_effect=lambda *a,**k:k['on_delivery'](error,SimpleNamespace(partition=lambda:0,offset=lambda:81882))
        return p
    def test_success_preserves_event_bytes_and_resume_does_not_send_again(self):
        self.seal();producer=self.producer()
        first=replay(self.directory(),producer,self.source.get)
        self.assertEqual(first['status'],'COMPLETED');self.assertEqual(first['events_sent'],1)
        replay(self.directory(),producer,self.source.get)
        producer.produce.assert_called_once()
        import json
        sent=json.loads(producer.produce.call_args.kwargs['value'])
        self.assertEqual(sent,self.event)
    def test_unknown_delivery_does_not_advance_cursor_and_reuses_same_event(self):
        self.seal()
        with self.assertRaisesRegex(PipelineError,'DELIVERY_UNCONFIRMED'):
            replay(self.directory(),self.producer(error='failed'),self.source.get)
        self.assertFalse((self.directory()/'replay-progress.json').exists())
        self.assertEqual(replay(self.directory(),self.producer(),self.source.get)['events_sent'],1)
    def test_changed_raw_blocks_before_publish(self):
        self.seal();producer=self.producer();self.source.docs[self.doc['_id']]['source_product_id']='999'
        with self.assertRaises(PipelineError):replay(self.directory(),producer,self.source.get)
        producer.produce.assert_not_called()
    def test_changed_plan_bytes_and_wrong_count_block(self):
        self.seal();path=self.directory()/'events.jsonl';path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaisesRegex(Conflict,'REPLAY_PLAN_CHANGED'):replay(self.directory(),self.producer(),self.source.get)
    def test_duplicate_event_or_wrong_run_cannot_be_sealed(self):
        with self.assertRaises(Conflict):self.seal([self.saved(),self.saved()],count=2)
        self.assertFalse((self.directory()/'manifest.json').exists())
    def test_damaged_resume_cursor_cannot_skip_events(self):
        manifest=self.seal()
        atomic_json(self.directory()/'replay-progress.json',dict(manifest_hash=manifest['content_hash'],events_sent=0,byte_cursor=manifest['bytes']))
        producer=self.producer()
        with self.assertRaisesRegex(Conflict,'REPLAY_STATE_CHANGED'):replay(self.directory(),producer,self.source.get)
        producer.produce.assert_not_called()
    def test_backpressure_counts_already_queued_events_and_blocks_another_gap(self):
        from product_pipeline.event_replay import admission_count
        self.assertEqual(admission_count(81882,81882,82882),0)
        self.assertEqual(admission_count(81982,81882,82882),100)
        self.assertEqual(admission_count(81882,81882,83882),0)
        with self.assertRaisesRegex(PipelineError,'OFFSETS_OUT_OF_RANGE'):admission_count(310,81882,82882)
    def test_confirmed_delivery_before_cursor_save_is_reused_without_new_requests(self):
        self.seal();producer=self.producer()
        from unittest.mock import patch
        from product_pipeline import event_replay
        original=event_replay.atomic_json
        def persist(path,value):
            if Path(path).name=='replay-progress.json':raise OSError('cursor interrupted')
            original(path,value)
        with patch('product_pipeline.event_replay.atomic_json',side_effect=persist),self.assertRaises(OSError):
            replay(self.directory(),producer,self.source.get)
        replay(self.directory(),producer,self.source.get)
        producer.produce.assert_called_once()
    def test_delivery_callbacks_are_matched_to_inputs_even_if_out_of_order(self):
        from product_pipeline.event_replay import send_batch
        callbacks=[];producer=MagicMock()
        producer.produce.side_effect=lambda *a,**k:callbacks.append(k['on_delivery'])
        def deliver(timeout):
            callbacks[1](None,SimpleNamespace(partition=lambda:0,offset=lambda:11))
            callbacks[0](None,SimpleNamespace(partition=lambda:0,offset=lambda:10))
        producer.poll.side_effect=deliver
        positions=send_batch(producer,'product.raw.collected',[dict(key='k',event=self.event)]*2)
        self.assertEqual([p['offset'] for p in positions],[10,11])
