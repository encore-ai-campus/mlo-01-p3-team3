import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from helpers import fixture
from product_pipeline.contracts import canonical, digest
from product_pipeline.files import atomic_json, read_json, rows, sha_file
from product_pipeline.snapshot import prepare, verify
from product_pipeline.errors import PipelineError


def journal_event(seq, pairs):
    event = dict(seq=seq, records=pairs, categories=[], requests=[], report={})
    encoded = json.dumps(event, ensure_ascii=False, sort_keys=True, allow_nan=False)
    return dict(event=event, sha256=hashlib.sha256(encoded.encode()).hexdigest())


def source_fixture(root):
    source, media = fixture(root, codes=('11', '12'))
    raw, normal = list(rows(source/'product_raw.jsonl')), list(rows(source/'products.jsonl'))
    pairs = [dict(raw=r, normalized=n) for r, n in zip(raw, normal)]
    atomic_json(source/'run_config.json', dict(job_id=raw[0]['job_id'], storage='jsonl-v1', config={}))
    append(source, journal_event(1, pairs))
    commit(source, 1)
    return source, media, pairs


def append(source, envelope):
    with (source/'checkpoint_events.jsonl').open('ab') as stream:
        stream.write(('\n'+canonical(envelope)+'\n').encode())


def commit(source, seq, status='running'):
    atomic_json(source/'checkpoint_state.json', dict(job_id='fixed-collection-run', commit=seq, status=status))


class LiveTests(unittest.TestCase):
    def capture(self, source, spool, **kw):
        from product_pipeline.live import capture
        return capture(source, spool, max_spool_bytes=100_000_000, min_free_bytes=0, **kw)

    def test_commit_bound_partial_event_and_no_source_or_lock_changes(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, pairs = source_fixture(d)
            append(source, journal_event(2, [pairs[0]]))
            with (source/'checkpoint_events.jsonl').open('ab') as s:
                s.write(b'{"unfinished":')
            before = {p.name: sha_file(p) for p in source.iterdir() if p.is_file()}
            spool = Path(d)/'spool'
            first = self.capture(source, spool, max_records=1)
            self.assertEqual(first['records'], 1)
            self.assertEqual(read_json(spool/'cursor.json')['pending_seq'], 1)
            second = self.capture(source, spool, max_records=1)
            self.assertEqual(second['records'], 1)
            self.assertEqual(read_json(spool/'cursor.json')['last_seq'], 1)
            self.assertEqual(self.capture(source, spool)['result'], 'IDLE')
            self.assertEqual(before, {p.name: sha_file(p) for p in source.iterdir() if p.is_file()})
            self.assertFalse((source/'writer.lock').exists())
            self.assertEqual(verify(first['snapshot'])['kind'], 'LIVE_DELTA')

    def test_new_commit_keeps_distinct_observations_of_same_product(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, pairs = source_fixture(d)
            spool = Path(d)/'spool'
            self.capture(source, spool)
            newer = copy.deepcopy(pairs[0])
            newer['raw']['detail_error'] = 'another wrapper observation'
            append(source, journal_event(2, [pairs[0], newer]))
            commit(source, 2)
            result = self.capture(source, spool)
            self.assertEqual(result['records'], 2)
            self.assertEqual(len(list(rows(Path(result['snapshot'])/'observations.jsonl'))), 2)

    def test_committed_corruption_and_source_replacement_block_resume(self):
        for mode in ('hash', 'seq', 'truncate', 'run', 'prefix'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                source, _, pairs = source_fixture(d)
                spool = Path(d)/'spool'
                self.capture(source, spool)
                if mode == 'truncate':
                    (source/'checkpoint_events.jsonl').write_bytes(b'')
                elif mode == 'run':
                    atomic_json(source/'run_config.json', dict(job_id='different', storage='jsonl-v1', config={}))
                elif mode == 'prefix':
                    p = source/'checkpoint_events.jsonl'
                    p.write_bytes(p.read_bytes().replace(b'fixed-collection-run', b'changed-collection-x'))
                else:
                    event = journal_event(3 if mode == 'seq' else 2, pairs)
                    if mode == 'hash': event['sha256'] = '0'*64
                    append(source, event)
                    commit(source, 2)
                with self.assertRaises(PipelineError): self.capture(source, spool)

    def test_control_only_commit_and_interrupted_fragment_are_accounted(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            with (source/'checkpoint_events.jsonl').open('ab') as s:
                s.write(b'\n{"old_uncommitted_tail":\n')
            append(source, journal_event(2, []))
            commit(source, 2)
            result = self.capture(source, Path(d)/'spool')
            manifest = verify(result['snapshot'])
            self.assertEqual(manifest['source_range']['to_seq'], 2)
            self.assertEqual(len(manifest['source_fragments']), 1)

    def test_spool_backpressure_never_advances_cursor(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool = Path(d)/'spool'
            from product_pipeline.live import capture
            result = capture(source, spool, max_spool_bytes=1, min_free_bytes=0)
            self.assertEqual(result['result'], 'BACKPRESSURE')
            self.assertFalse((spool/'cursor.json').exists())

    def test_seal_before_cursor_crash_replays_without_loss(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool = Path(d)/'spool'
            def crash(stage):
                if stage == 'SEALED': raise RuntimeError('crash')
            with self.assertRaises(RuntimeError): self.capture(source, spool, hook=crash)
            self.assertFalse((spool/'cursor.json').exists())
            result = self.capture(source, spool)
            self.assertEqual(len(list((spool/'batches').iterdir())), 1)
            self.assertEqual(read_json(spool/'cursor.json')['snapshot_id'], result['snapshot_id'])

    def test_missing_sealed_evidence_blocks_cursor_resume(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool = Path(d)/'spool'
            result = self.capture(source, spool)
            (Path(result['snapshot'])/'observations.jsonl').unlink()
            with self.assertRaises(PipelineError): self.capture(source, spool)

    def test_gallery_followup_matches_full_export_and_does_not_redownload(self):
        with tempfile.TemporaryDirectory() as d:
            source, media, _ = source_fixture(d)
            spool = Path(d)/'spool'
            metadata = self.capture(source, spool)
            result = self.capture(source, spool, media_manifest=media)
            self.assertEqual(result['phase'], 'GALLERY_ACQUISITION')
            manifest = verify(result['snapshot'])
            self.assertEqual(manifest['verified_image_relations'], 4)
            sealed = Path(d)/'full'
            prepare(source, sealed, media)
            live_keys = {r['observation_key'] for r in rows(Path(result['snapshot'])/'observations.jsonl')}
            full_keys = {r['observation_key'] for r in rows(sealed/'observations.jsonl')}
            self.assertEqual(live_keys, full_keys)
            self.assertNotEqual(live_keys, {r['observation_key'] for r in rows(Path(metadata['snapshot'])/'observations.jsonl')})
            self.assertEqual(self.capture(source, spool, media_manifest=media)['result'], 'IDLE')

    def test_ambiguous_media_wrapper_is_not_guessed(self):
        with tempfile.TemporaryDirectory() as d:
            source, media, pairs = source_fixture(d)
            newer = copy.deepcopy(pairs[0])
            newer['raw']['detail_error'] = 'different-wrapper'
            append(source, journal_event(2, [newer]))
            commit(source, 2)
            spool = Path(d)/'spool'
            self.capture(source, spool)
            result = self.capture(source, spool, media_manifest=media)
            self.assertTrue(any(r['status'] == 'LINK_AMBIGUOUS' for r in result['media_wait']))
            self.assertEqual(result['records'], 1)

    def test_spool_drain_is_separate_resumable_and_failure_receipt_is_not_success(self):
        from product_pipeline.handoff import drain_spool
        with tempfile.TemporaryDirectory() as d:
            source, media, _ = source_fixture(d)
            spool, state = Path(d)/'spool', Path(d)/'state'
            self.capture(source, spool)
            self.capture(source, spool, media_manifest=media)
            result = drain_spool(spool, state, rehearse=True)
            self.assertEqual(result['result'], 'DRAINED')
            self.assertEqual(result['batches_ingested'], 2)
            repeat = drain_spool(spool, state, rehearse=True)
            self.assertEqual(repeat['batches_ingested'], 0)
            self.assertEqual(repeat['batches_reused'], 2)

    def test_active_session_reuses_verified_prefix_but_restart_revalidates(self):
        from product_pipeline.live import CaptureSession
        import product_pipeline.live as live
        with tempfile.TemporaryDirectory() as d:
            source, _, pairs = source_fixture(d)
            spool = Path(d)/'spool'
            session = CaptureSession()
            self.capture(source, spool, session=session)
            append(source, journal_event(2, pairs))
            commit(source, 2)
            with patch.object(live, 'prefix', side_effect=AssertionError('prefix must be cached')):
                self.capture(source, spool, session=session)
                self.assertEqual(self.capture(source, spool, session=session)['result'], 'IDLE')
            path = source/'checkpoint_events.jsonl'
            path.write_bytes(path.read_bytes().replace(b'fixed-collection-run', b'changed-collection-x'))
            with self.assertRaises(PipelineError):
                self.capture(source, spool, session=CaptureSession())

    def test_missing_future_media_is_wait_not_a_failed_source_capture(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool = Path(d)/'spool'
            self.capture(source, spool)
            result = self.capture(source, spool, media_manifest=source/'media'/'future.jsonl')
            self.assertEqual(result['media_status'], 'WAIT_MEDIA_MANIFEST')

    def test_live_resealing_cannot_change_pair_without_changing_event_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            result = self.capture(source, Path(d)/'spool')
            sealed = Path(result['snapshot'])
            values = list(rows(sealed/'observations.jsonl'))
            values[0]['normalized']['name'] = 'unrelated name'
            from helpers import write_rows
            write_rows(sealed/'observations.jsonl', values)
            manifest = read_json(sealed/'snapshot.json')
            manifest['files']['observations.jsonl'] = dict(sha256=sha_file(sealed/'observations.jsonl'), bytes=(sealed/'observations.jsonl').stat().st_size)
            manifest['snapshot_id'] = digest({k: v for k,v in manifest.items() if k not in {'snapshot_id', 'prepared_at'}})
            atomic_json(sealed/'snapshot.json', manifest)
            with self.assertRaisesRegex(PipelineError, 'LIVE_PAIR_CHANGED'): verify(sealed)

    def test_failed_drain_keeps_batch_pending_then_recovers(self):
        from product_pipeline.handoff import drain_spool
        from product_pipeline import handoff
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool, state = Path(d)/'spool', Path(d)/'state'
            self.capture(source, spool)
            failed = dict(failed=1, errors=[], raw_confirmed=0, events_sent=0,
                          events_reused=0, image_relations_unavailable=0)
            with patch.object(handoff, 'ingest', return_value=failed):
                self.assertEqual(drain_spool(spool, state, rehearse=True)['result'], 'RETRY_REQUIRED')
            self.assertFalse(list((state/'receipts').glob('*.json')))
            self.assertEqual(drain_spool(spool, state, rehearse=True)['result'], 'DRAINED')

    def test_local_environment_move_keeps_registered_ids_and_receipts(self):
        import shutil
        from product_pipeline.handoff import drain_spool
        with tempfile.TemporaryDirectory() as d:
            source, _, _ = source_fixture(d)
            spool, state = Path(d)/'spool', Path(d)/'state'
            self.capture(source, spool)
            drain_spool(spool, state, rehearse=True)
            before = {p.relative_to(state).as_posix(): sha_file(p) for p in (state/'ledger').rglob('*.json')}
            moved_spool, moved_state = Path(d)/'moved-spool', Path(d)/'moved-state'
            shutil.copytree(spool, moved_spool)
            shutil.copytree(state, moved_state)
            result = drain_spool(moved_spool, moved_state, rehearse=True)
            self.assertEqual((result['batches_ingested'], result['batches_reused']), (0, 1))
            after = {p.relative_to(moved_state).as_posix(): sha_file(p) for p in (moved_state/'ledger').rglob('*.json')}
            self.assertEqual(before, after)


if __name__ == '__main__': unittest.main()
