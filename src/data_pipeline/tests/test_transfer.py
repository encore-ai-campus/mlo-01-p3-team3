import tempfile
import unittest
from pathlib import Path

from helpers import fixture
from product_pipeline.files import read_json, sha_file
from product_pipeline.local import FileObjects
from product_pipeline.snapshot import prepare, verify


class TransferTests(unittest.TestCase):
    def test_ready_last_resumes_chunks_and_preserves_snapshot_identity(self):
        from product_pipeline.transfer import send_snapshot, receive_snapshot
        with tempfile.TemporaryDirectory() as d:
            source, media = fixture(d)
            sealed, remote, received = Path(d)/'sealed', Path(d)/'remote', Path(d)/'received'
            original = prepare(source, sealed, media)
            store = FileObjects(remote)
            first = send_snapshot(sealed, store)
            repeat = send_snapshot(sealed, store)
            self.assertGreater(first['objects_written'], 0)
            self.assertEqual(repeat['objects_written'], 0)
            receive_snapshot(remote/first['ready_key'], lambda key: (remote/key).read_bytes(), received)
            self.assertEqual(verify(received)['snapshot_id'], original['snapshot_id'])
            self.assertEqual(sha_file(received/'observations.jsonl'), sha_file(sealed/'observations.jsonl'))

    def test_missing_or_changed_chunk_never_exposes_received_snapshot(self):
        from product_pipeline.transfer import send_snapshot, receive_snapshot
        with tempfile.TemporaryDirectory() as d:
            source, _ = fixture(d)
            sealed, remote = Path(d)/'sealed', Path(d)/'remote'
            prepare(source, sealed)
            sent = send_snapshot(sealed, FileObjects(remote))
            ready = remote/sent['ready_key']
            key = read_json(ready)['files']['observations.jsonl']['chunks'][0]['key']
            (remote/key).write_bytes(b'corrupt')
            target = Path(d)/'received'
            with self.assertRaises(Exception):
                receive_snapshot(ready, lambda k: (remote/k).read_bytes(), target)
            self.assertFalse(target.exists())

    def test_interruption_before_ready_is_not_discoverable_as_complete(self):
        from product_pipeline.transfer import send_snapshot
        with tempfile.TemporaryDirectory() as d:
            source, _ = fixture(d)
            sealed, remote = Path(d)/'sealed', Path(d)/'remote'
            prepare(source, sealed)
            store = FileObjects(remote)
            ensure = store.ensure
            def fail_ready(key, *args):
                if '/ready/' in key: raise OSError('connection lost')
                return ensure(key, *args)
            store.ensure = fail_ready
            with self.assertRaises(OSError): send_snapshot(sealed, store)
            self.assertFalse(list(remote.rglob('*.ready.json')))
            store.ensure = ensure
            self.assertGreater(send_snapshot(sealed, store)['objects_written'], 0)


if __name__ == '__main__': unittest.main()
