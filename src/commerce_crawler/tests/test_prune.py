"""Run-directory retention tests on synthetic run folders; no collection or network."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from product_collector.checkpoint import writer_lock
from product_collector.prune import prune_runs


def run(root, name, finished, status='completed', loaded=True, evidence=None):
    directory = Path(root) / name
    (directory / 'details' / '11').mkdir(parents=True)
    (directory / 'details' / '11' / 'page.json.gz').write_bytes(b'page')
    (directory / 'run_config.json').write_text(json.dumps({'job_id': name}), encoding='utf-8')
    (directory / 'checkpoint_state.json').write_text(
        json.dumps({'job_id': name, 'status': status, 'updated_at': finished}), encoding='utf-8')
    if loaded:
        job = name if loaded is True else loaded
        (directory / 'pipeline_loaded.json').write_text(json.dumps({'job_id': job}), encoding='utf-8')
    if evidence:
        (directory / 'run_report.json').write_text(
            json.dumps({'detail_evidence_dirs': [str((Path(root) / e).resolve()) for e in evidence]}), encoding='utf-8')
    return directory


class PruneTests(unittest.TestCase):
    def test_keeps_the_newest_loaded_runs_and_deletes_only_on_apply(self):
        with tempfile.TemporaryDirectory() as root:
            for week in range(1, 5):
                run(root, f'w{week}', f'2026-10-0{week}T00:00:00+00:00')
            dry = prune_runs(root, keep=2)
            self.assertEqual(sorted(dry['deleted']), ['w1', 'w2'])
            self.assertTrue((Path(root) / 'w1').is_dir())
            done = prune_runs(root, keep=2, apply=True)
            self.assertEqual(sorted(done['deleted']), ['w1', 'w2'])
            self.assertEqual(sorted(p.name for p in Path(root).iterdir() if p.is_dir()), ['w3', 'w4'])

    def test_unloaded_incomplete_active_and_foreign_runs_are_never_deleted(self):
        with tempfile.TemporaryDirectory() as root:
            run(root, 'unloaded', '2026-10-01T00:00:00+00:00', loaded=False)
            run(root, 'stale-marker', '2026-10-02T00:00:00+00:00', loaded='another-job')
            run(root, 'failed', '2026-10-03T00:00:00+00:00', status='failed')
            active = run(root, 'active', '2026-10-04T00:00:00+00:00')
            (Path(root) / 'active_run.json').write_text(json.dumps({'directory': str(active)}), encoding='utf-8')
            (Path(root) / 'notes').mkdir()
            for week in (5, 6):
                run(root, f'w{week}', f'2026-10-0{week}T00:00:00+00:00')
            report = prune_runs(root, keep=1, apply=True)
            self.assertEqual(report['deleted'], ['w5'])
            self.assertEqual({k: report['kept'][k] for k in ('unloaded', 'stale-marker', 'failed', 'active', 'notes', 'w6')},
                             {'unloaded': 'not_loaded', 'stale-marker': 'not_loaded', 'failed': 'status_failed',
                              'active': 'active', 'notes': 'not_a_collection_run', 'w6': 'newest_loaded'})

    def test_a_run_that_a_remaining_run_reads_pages_from_is_kept(self):
        with tempfile.TemporaryDirectory() as root:
            run(root, 'w1', '2026-10-01T00:00:00+00:00')
            run(root, 'w2', '2026-10-02T00:00:00+00:00')
            run(root, 'w3', '2026-10-03T00:00:00+00:00', evidence=['w1'])
            report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], report['kept']['w1']), (['w2'], 'referenced_evidence'))
            self.assertTrue((Path(root) / 'w1' / 'details').is_dir())

    def test_refuses_while_a_collection_holds_the_lock(self):
        with tempfile.TemporaryDirectory() as root:
            run(root, 'w1', '2026-10-01T00:00:00+00:00')
            run(root, 'w2', '2026-10-02T00:00:00+00:00')
            with writer_lock(Path(root) / '.collection.lock'):
                with self.assertRaises((ValueError, OSError)):
                    prune_runs(root, keep=1, apply=True)
            self.assertTrue((Path(root) / 'w1').is_dir())

    def test_a_run_whose_writer_lock_is_held_elsewhere_is_kept(self):
        # A run resumed from another output root holds only its own writer.lock.
        with tempfile.TemporaryDirectory() as root:
            old = run(root, 'w1', '2026-10-01T00:00:00+00:00')
            run(root, 'w2', '2026-10-02T00:00:00+00:00')
            with writer_lock(old / 'writer.lock'):
                report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], report['kept']['w1']), ([], 'in_use'))
            self.assertTrue((old / 'details').is_dir())
            self.assertFalse((Path(root) / '.deleting-w1').exists())

    def test_a_run_kept_for_its_lock_keeps_the_run_it_reads_pages_from(self):
        with tempfile.TemporaryDirectory() as root:
            run(root, 'w1', '2026-10-01T00:00:00+00:00')
            locked = run(root, 'w2', '2026-10-02T00:00:00+00:00', evidence=['w1'])
            run(root, 'w3', '2026-10-03T00:00:00+00:00')
            with writer_lock(locked / 'writer.lock'):
                report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], report['kept']['w2'], report['kept']['w1']),
                             ([], 'in_use', 'referenced_evidence'))
            self.assertTrue((Path(root) / 'w1' / 'details').is_dir())

    def test_a_reader_that_fails_to_delete_keeps_its_evidence(self):
        # The lock is taken by a collector only after planning: the reader stays, so must its evidence.
        import product_collector.prune as prune
        original = prune._delete
        with tempfile.TemporaryDirectory() as root:
            run(root, 'w1', '2026-10-01T00:00:00+00:00')
            run(root, 'w2', '2026-10-02T00:00:00+00:00', evidence=['w1'])
            run(root, 'w3', '2026-10-03T00:00:00+00:00')
            with patch.object(prune, '_delete', lambda d, a: 'in_use' if d.name == 'w2' else original(d, a)):
                report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], report['kept']['w2'], report['kept']['w1']),
                             ([], 'in_use', 'referenced_evidence'))
            self.assertTrue((Path(root) / 'w1').is_dir())

    def test_a_marker_without_the_job_identity_confirms_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            for name, finished in (('w1', '2026-10-01T00:00:00+00:00'), ('w2', '2026-10-02T00:00:00+00:00')):
                directory = run(root, name, finished)
                for file in ('run_config.json', 'checkpoint_state.json', 'pipeline_loaded.json'):
                    data = json.loads((directory / file).read_text(encoding='utf-8'))
                    data.pop('job_id')
                    (directory / file).write_text(json.dumps(data), encoding='utf-8')
            report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], set(report['kept'].values())), ([], {'not_loaded'}))

    def test_a_resume_that_loses_its_run_to_prune_never_starts_empty(self):
        # prune moves the run after open_collection checked it but before it took writer.lock.
        import product_collector.checkpoint as checkpoint
        from product_collector.danawa import DanawaAdapter
        original = checkpoint.writer_lock
        with tempfile.TemporaryDirectory() as root:
            old = run(root, 'w1', '2026-10-01T00:00:00+00:00')

            def racing(path):
                if path.name == 'writer.lock' and old.exists():
                    old.rename(old.with_name('.deleting-w1'))
                return original(path)
            with patch.object(checkpoint, 'writer_lock', racing):
                with self.assertRaisesRegex(ValueError, '정리되었습니다'):
                    with checkpoint.open_collection(Path(root) / 'other', DanawaAdapter(), {}, resume_from=old):
                        self.fail('an empty run was opened')
            self.assertFalse((old / 'run_config.json').exists())

    def test_finishes_an_interrupted_deletion(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / '.deleting-w0' / 'details').mkdir(parents=True)
            run(root, 'w1', '2026-10-01T00:00:00+00:00')
            self.assertEqual(prune_runs(root)['interrupted_deletions'], ['.deleting-w0'])
            prune_runs(root, apply=True)
            self.assertFalse((Path(root) / '.deleting-w0').exists())
            self.assertTrue((Path(root) / 'w1').is_dir())


if __name__ == '__main__':
    unittest.main()
