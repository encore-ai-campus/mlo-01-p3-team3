import copy
from pathlib import Path
import tempfile
import unittest

from product_pipeline.bronze.batch import process_batch
from product_pipeline.bronze.contracts import Message
from product_pipeline.contracts import canonical, digest, envelope, event_for
from product_pipeline.errors import PipelineError
from product_pipeline.files import rows
from product_pipeline.snapshot import prepare
from tests.helpers import fixture


class MemorySource:
    def __init__(self, docs):
        self.docs = docs
    def get(self, key):
        if key not in self.docs:
            raise PipelineError('BRONZE_RAW_UNCONFIRMED')
        return copy.deepcopy(self.docs[key])


class MemorySink:
    def __init__(self):
        self.raw, self.events, self.quarantine = {}, {}, {}
        self.fail = None
    def existing(self, table, keys):
        data = getattr(self, table)
        return {k: data[k] for k in keys if k in data}
    def observations(self, keys):
        return {v['observation_key']: v for v in self.raw.values() if v['observation_key'] in keys}
    def insert(self, table, values):
        if self.fail == table:
            raise OSError('injected storage failure')
        key = {'raw': 'raw_document_id', 'events': 'event_id', 'quarantine': 'quarantine_id'}[table]
        data = getattr(self, table)
        for value in values:
            data.setdefault(value[key], copy.deepcopy(value))


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        export, media = fixture(root, codes=('11',))
        sealed = root / 'snapshot'
        manifest = prepare(export, sealed, media)
        row = next(rows(sealed / 'observations.jsonl'))
        self.doc = envelope(row, '0123456789abcdef01234567', manifest['snapshot_id'], 'test-images')
        self.event = event_for(self.doc, 'e79f0851-9701-4db5-a924-0e4f222d8956', '2026-10-16T00:00:00.000Z')
        self.source = MemorySource({self.doc['_id']: self.doc})
        self.sink = MemorySink()
    def tearDown(self):
        self.temp.cleanup()
    def message(self, event=None, offset=0, key=None):
        return Message('product.raw.collected', 0, offset,
                       (key or self.doc['source_record_key']).encode(), canonical(event or self.event).encode())
    def run_batch(self, messages):
        return process_batch(messages, self.source, self.sink, batch_id=7)
    def test_preserves_payload_full_envelope_and_gallery_and_is_idempotent(self):
        first = self.run_batch([self.message(), self.message(offset=1)])
        second = self.run_batch([self.message(offset=2)])
        self.assertEqual((len(self.sink.raw), len(self.sink.events)), (1, 1))
        saved = self.sink.raw[self.doc['_id']]
        self.assertEqual(saved['payload_json'], canonical(self.doc['payload']))
        self.assertEqual(saved['raw_envelope_json'], canonical(self.doc))
        self.assertEqual(first['accepted'], 2)
        self.assertEqual(second['accepted'], 1)
        self.assertFalse(self.sink.quarantine)
    def test_bad_json_schema_key_and_event_hash_mismatch_are_quarantined(self):
        changed = copy.deepcopy(self.event)
        changed['payload']['document_hash'] = '0' * 64
        unsupported = {**self.event, 'schema_version': 2}
        messages = [Message('product.raw.collected', 0, 0, b'x', b'{broken'),
                    self.message(unsupported, 1), self.message(offset=2, key='wrong'), self.message(changed, 3)]
        result = self.run_batch(messages)
        self.assertEqual(result['quarantined'], 4)
        self.assertEqual(len(self.sink.raw), 0)
        self.assertEqual(len(self.sink.quarantine), 4)
        self.run_batch(messages)
        self.assertEqual(len(self.sink.quarantine), 4)
    def test_missing_raw_or_lookup_outage_fails_without_success_receipt(self):
        self.source.docs.clear()
        with self.assertRaises(PipelineError):
            self.run_batch([self.message()])
        self.assertFalse(self.sink.events)
        self.assertFalse(self.sink.quarantine)
    def test_tampered_raw_is_quarantined_instead_of_preserved(self):
        self.source.docs[self.doc['_id']]['payload']['evil'] = True
        self.run_batch([self.message()])
        self.assertFalse(self.sink.raw)
        self.assertEqual(len(self.sink.quarantine), 1)
    def test_same_event_id_with_different_body_does_not_overwrite(self):
        self.run_batch([self.message()])
        changed = {**self.event, 'occurred_at': '2026-10-17T00:00:00.000Z'}
        self.run_batch([self.message(changed, 1)])
        self.assertEqual(len(self.sink.quarantine), 1)
        self.assertEqual(self.sink.events[self.event['event_id']]['event_json'], canonical(self.event))
    def test_raw_commit_then_receipt_failure_recovers_without_duplicate(self):
        self.sink.fail = 'events'
        with self.assertRaises(OSError):
            self.run_batch([self.message()])
        self.assertEqual(len(self.sink.raw), 1)
        self.assertFalse(self.sink.events)
        self.sink.fail = None
        self.run_batch([self.message()])
        self.assertEqual((len(self.sink.raw), len(self.sink.events)), (1, 1))
    def test_quarantine_write_failure_must_fail_the_batch(self):
        self.sink.fail = 'quarantine'
        with self.assertRaises(OSError):
            self.run_batch([Message('product.raw.collected', 0, 0, b'x', b'broken')])
    def test_existing_raw_conflict_is_not_overwritten(self):
        self.run_batch([self.message()])
        self.sink.raw[self.doc['_id']]['document_hash'] = 'f' * 64
        self.run_batch([self.message(offset=1)])
        self.assertEqual(len(self.sink.quarantine), 1)
        self.assertEqual(self.sink.raw[self.doc['_id']]['document_hash'], 'f' * 64)
    def test_storage_outage_is_retry_not_poison(self):
        def broken(key):
            raise OSError('database unreachable')
        self.source.get = broken
        with self.assertRaises(OSError):
            self.run_batch([self.message()])
        self.assertFalse(self.sink.quarantine)
    def test_batch_limits_fail_before_any_commit(self):
        for kwargs in ({'max_records': 1}, {'max_raw_bytes': 1}, {'max_input_bytes': 1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(PipelineError):
                process_batch([self.message(), self.message(offset=1)], self.source, self.sink, 0, **kwargs)
        self.assertFalse(self.sink.raw)
        self.assertFalse(self.sink.events)
    def test_quarantine_failure_after_good_commit_is_replayable(self):
        bad = Message('product.raw.collected', 0, 1, None, None)
        self.sink.fail = 'quarantine'
        with self.assertRaises(OSError):
            self.run_batch([self.message(), bad])
        self.assertEqual(len(self.sink.events), 1)
        self.sink.fail = None
        self.run_batch([self.message(), bad])
        self.assertEqual((len(self.sink.raw), len(self.sink.events), len(self.sink.quarantine)), (1, 1, 1))
    def test_event_conflict_inside_same_batch_keeps_first_receipt(self):
        changed = {**self.event, 'occurred_at': '2026-10-17T00:00:00.000Z'}
        self.run_batch([self.message(), self.message(changed, 1)])
        self.assertEqual(len(self.sink.events), 1)
        self.assertEqual(len(self.sink.quarantine), 1)
    def test_non_json_confirmed_raw_is_poison_not_a_permanent_retry(self):
        self.source.docs[self.doc['_id']]['payload']['bad'] = float('nan')
        result = self.run_batch([self.message()])
        self.assertEqual(result['quarantined'], 1)
        self.assertFalse(self.sink.raw)


if __name__ == '__main__':
    unittest.main()
