import copy
from contextlib import contextmanager
from types import SimpleNamespace
import tempfile
import unittest

from test_silver import sample
from product_pipeline.contracts import event_for
from product_pipeline.mongo import bson_document
from product_pipeline.relay import relay
from product_pipeline.errors import Conflict


class Cursor(list):
    def sort(self,*args):return self
    def batch_size(self,*args):return self


class Ledger:
    settings=SimpleNamespace(fingerprint='a'*64)
    def __init__(self,doc):
        self.saved=dict(observation_key=doc['observation_key'],document=doc,
            event=event_for(doc,'00000000-0000-4000-8000-000000000001',doc['collected_at']),target_fingerprint='a'*64,raw_confirmed=True)
        self.stored=bson_document(doc);self.observations=self;self.raw=self;self.releases=0
    @contextmanager
    def exclusive(self):yield self
    def find(self,*args):return Cursor([] if self.saved.get('event_confirmed') else [self.saved])
    def find_one(self,*args):return self.stored
    def acquire(self,key):return 1
    def get(self,key):return copy.deepcopy(self.saved)
    def update(self,key,token,**fields):self.saved.update(fields)
    def release(self,key,token):self.releases+=1


class Publisher:
    def __init__(self,fail=False):self.events=[];self.fail=fail
    def send(self,event,key):
        self.events.append(copy.deepcopy(event))
        if self.fail:raise RuntimeError('lost ACK')


class RelayTests(unittest.TestCase):
    def test_ack_loss_reuses_fixed_event_and_success_is_not_resent(self):
        with tempfile.TemporaryDirectory() as root:
            doc,_=sample(root);ledger=Ledger(doc);first=Publisher(True)
            with self.assertRaises(RuntimeError):relay(ledger,first,collection_run_id=doc['collection_run_id'])
            self.assertEqual(ledger.saved['stage'],'EVENT_UNKNOWN');self.assertFalse(ledger.saved.get('event_confirmed'))
            second=Publisher();relay(ledger,second,collection_run_id=doc['collection_run_id'])
            self.assertEqual(first.events,second.events);self.assertTrue(ledger.saved['event_confirmed'])
            self.assertEqual(relay(ledger,second,collection_run_id=doc['collection_run_id'])['events_sent'],0)
            self.assertEqual(ledger.releases,2)
    def test_mismatched_raw_or_target_is_not_emitted(self):
        for change in ('raw','target'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as root:
                doc,_=sample(root);ledger=Ledger(doc);publisher=Publisher()
                if change=='raw':ledger.stored['source_product_id']='999'
                else:ledger.saved['target_fingerprint']='b'*64
                with self.assertRaises(Conflict):relay(ledger,publisher,collection_run_id=doc['collection_run_id'])
                self.assertFalse(publisher.events);self.assertEqual(ledger.releases,1)
