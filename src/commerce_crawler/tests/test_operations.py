"""Contracts the server launcher reads (operations.md): exit codes, the last stdout line, block
reasons, listing retries, abandoning a run and opt-in pruning of runs that are never resumed.
No network: fake sessions and synthetic run folders."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests

from product_collector.__main__ import main
from product_collector.abandon import main as abandon_main
from product_collector.checkpoint import CollectionCheckpoint, abandon_run, writer_lock
from product_collector.core import categories_incomplete, run_result
from product_collector.prune import prune_runs
from product_collector.media import localize_images
from product_collector.validation import verify_assets
from test_memory_equivalence import CATEGORIES, FIRST, Harness, Session, api, category_url, first_pages, listing_page
from test_prune import seal, run as synthetic_run


def last_line(printed):
    return json.loads(printed.call_args_list[-1].args[0])


def state(directory):
    return json.loads((Path(directory) / 'checkpoint_state.json').read_text(encoding='utf-8'))


def run_report(directory):
    return json.loads((Path(directory) / 'run_report.json').read_text(encoding='utf-8'))


class ExitCodeTests(unittest.TestCase):
    def test_usage_errors_are_not_partial(self):
        for argv in (['collector', '--resume-from', 'x', '--new-run'], ['collector', '--workers', '0']):
            with patch('sys.argv', argv), patch('sys.stderr'), patch('builtins.print') as printed:
                with self.assertRaises(SystemExit) as error:
                    main()
            self.assertEqual(error.exception.code, 3)
            line = last_line(printed)
            self.assertEqual((line['result'], line['exit_code']), ('not_started', 3))

    def test_a_refused_resume_changes_no_state_and_says_not_started(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            code, _ = harness.run('out', FIRST, first_pages(), stop=('GET', 'https://prod.danawa.com/info/?pcode=13'))
            self.assertEqual(code, 130)
            target = harness.runs('out')[0]
            before = (target / 'checkpoint_state.json').read_bytes()
            # Another category list is another job: the resume is refused before anything is collected.
            harness.categories.write_text(json.dumps([{'category_id': CATEGORIES[0], 'limit': None, 'max_pages': None}]),
                                          encoding='utf-8')
            with patch('builtins.print') as printed:
                argv = ['product_collector', '--categories', str(harness.categories), '--output',
                        str(harness.root / 'out'), '--pagination-template', str(harness.templates)]
                with patch('sys.argv', argv), patch('requests.Session', return_value=Session({}, {})):
                    self.assertEqual(main(), 3)
            self.assertEqual((target / 'checkpoint_state.json').read_bytes(), before)
            line = last_line(printed)
            self.assertEqual((line['result'], line['exit_code']), ('not_started', 3))

    def test_every_outcome_ends_with_a_result_line(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            with patch('product_collector.__main__.finish_line') as line:
                code, _ = harness.run('out', FIRST, first_pages(), stop=('GET', 'https://prod.danawa.com/info/?pcode=13'))
                self.assertEqual(code, 130)
                self.assertEqual({k: line.call_args.kwargs[k] for k in ('result', 'exit_code')},
                                 {'result': 'interrupted', 'exit_code': 130})
                self.assertEqual(harness.run('out', FIRST, first_pages())[0], 0)
                self.assertEqual({k: line.call_args.kwargs[k] for k in ('result', 'exit_code')},
                                 {'result': 'completed', 'exit_code': 0})
            self.assertEqual(line.call_args.kwargs['job_id'], harness.runs('out')[0].name)


class StaleResultTests(unittest.TestCase):
    def test_an_attempt_that_fails_after_a_result_does_not_keep_it(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = CollectionCheckpoint(Path(directory) / 'run', config={'source': 'danawa'})
            checkpoint.save([], {'result': 'completed', 'categories': []}, [])
            checkpoint.interrupt([], 'failed', 'image step failed')
            self.assertEqual(run_report(checkpoint.directory)['result'], 'failed')
            self.assertEqual(state(checkpoint.directory)['status'], 'failed')

    def test_an_attempt_without_a_result_gets_none(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = CollectionCheckpoint(Path(directory) / 'run', config={'source': 'danawa'})
            checkpoint.save([], {'categories': []}, [])
            checkpoint.interrupt([])
            self.assertNotIn('result', run_report(checkpoint.directory))


class ImageBlockTests(unittest.TestCase):
    def test_an_image_host_block_is_in_the_run_report(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            gets = first_pages()
            gets['https://img.example/12/1.png'] = (429, b'slow down')
            code, _ = harness.run('out', FIRST, gets)
            self.assertEqual(code, 2)
            report = run_report(harness.runs('out')[0])
            self.assertEqual((report['result'], report['image_download']['stop_reason']), ('partial', 'http_429'))

    def test_no_block_no_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            self.assertEqual(harness.run('out', FIRST, first_pages())[0], 0)
            self.assertNotIn('stop_reason', run_report(harness.runs('out')[0])['image_download'])


class CheckPaginationTests(unittest.TestCase):
    def check(self, response=None, error=None, template=True, timeout='20'):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            url = category_url(CATEGORIES[0])
            posts = {(url, 1): response if response is not None else listing_page(CATEGORIES[0], [api('11')])}
            session = Session(posts, {})
            if error is not None:
                session.post = lambda *a, **k: (_ for _ in ()).throw(error)
            path = harness.templates if template else harness.root / 'missing.json'
            argv = ['product_collector', '--check-pagination', '--pagination-template', str(path),
                    '--output', str(harness.root / 'out'), '--timeout', timeout]
            with patch('sys.argv', argv), patch('requests.Session', return_value=session), \
                    patch('builtins.print') as printed:
                code = main()
            self.assertFalse((harness.root / 'out').exists())  # nothing is saved
            line = last_line(printed)
            self.assertNotIn('TEST_ONLY_ACTION', json.dumps(line))  # header values never printed
            return code, line

    def test_outcomes(self):
        self.assertEqual(self.check()[0], 0)
        code, line = self.check((429, 'blocked'))
        self.assertEqual((code, line['status'], line['http_status']), (4, 'blocked', 429))
        self.assertEqual(self.check(error=requests.ConnectionError('down'))[:1], (5,))
        code, line = self.check('<html>new deployment</html>')
        self.assertEqual((code, line['status']), (6, 'parse_failed'))
        self.assertEqual(self.check(listing_page(CATEGORIES[0], [{'productCode': '1'}]))[0], 6)  # no usable product
        code, line = self.check(template=False)
        self.assertEqual((code, line['status']), (3, 'not_started'))
        code, line = self.check(timeout='0')
        self.assertEqual((code, line['status']), (3, 'not_started'))


class ListingResumeTests(unittest.TestCase):
    def test_an_unrecognised_empty_page_is_requested_again_on_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            broken = {CATEGORIES[0]: FIRST[CATEGORIES[0]], CATEGORIES[1]: [{'productCode': '99'}]}  # all rejected
            code, _ = harness.run('out', broken, first_pages())
            self.assertEqual(code, 2)
            target = harness.runs('out')[0]
            failed = next(c for c in run_report(target)['categories'] if c['category_id'] == CATEGORIES[1])
            self.assertEqual(failed['status'], 'no_products')
            code, session = harness.run('out', FIRST, first_pages())
            self.assertEqual(code, 0)
            self.assertIn(('POST', category_url(CATEGORIES[1]), 1), session.calls)
            self.assertEqual(harness.runs('out'), [target])
            listed = next(c for c in run_report(target)['categories'] if c['category_id'] == CATEGORIES[1])
            self.assertEqual(listed['status'], 'ok')

    def test_a_listing_failure_left_after_a_resume_keeps_the_run_partial(self):
        report = {'errors': [], 'categories': []}
        categories = [{'category_id': '1', 'status': 'ok'}, {'category_id': '2', 'status': 'request_failed'}]
        self.assertTrue(categories_incomplete(categories))
        self.assertEqual(run_result(report, [], categories_incomplete(categories)), 'partial')
        self.assertFalse(categories_incomplete([{'category_id': '1', 'status': 'ok', 'reason': 'empty_page'}]))


class AbandonTests(unittest.TestCase):
    def interrupted(self, root):
        harness = Harness(Path(root))
        code, _ = harness.run('out', FIRST, first_pages(), stop=('GET', 'https://prod.danawa.com/info/?pcode=13'))
        self.assertEqual(code, 130)
        return harness, harness.runs('out')[0]

    def test_abandoning_ends_the_active_run_and_the_next_collection_starts_new(self):
        with tempfile.TemporaryDirectory() as directory:
            harness, target = self.interrupted(directory)
            result = abandon_run(harness.root / 'out', reason='template body changed')
            self.assertEqual((result['status'], result['previous_status'], result['was_active']),
                             ('abandoned', 'interrupted', True))
            self.assertFalse((harness.root / 'out' / 'active_run.json').exists())
            self.assertEqual(state(target)['abandon_reason'], 'template body changed')
            marked = (target / 'checkpoint_state.json').read_bytes()
            for options in (('--resume-from', str(target)), ('--resume-from', str(target), '--repair-files')):
                code, _ = harness.run('out', FIRST, first_pages(), *options)
                self.assertEqual(code, 3)  # an abandoned run is not resumed, nor its state rewritten
                self.assertEqual((target / 'checkpoint_state.json').read_bytes(), marked)
            self.assertEqual(harness.run('out', FIRST, first_pages())[0], 0)
            self.assertEqual(len(harness.runs('out')), 2)

    def test_completed_busy_and_missing_runs_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            harness, target = self.interrupted(directory)
            with writer_lock(target / 'writer.lock'):
                with self.assertRaises(ValueError):
                    abandon_run(harness.root / 'out', target)
            self.assertEqual(state(target)['status'], 'interrupted')
            self.assertEqual(harness.run('out', FIRST, first_pages())[0], 0)
            with self.assertRaisesRegex(ValueError, '완료'):
                abandon_run(harness.root / 'out', target)
            with self.assertRaises(ValueError):
                abandon_run(harness.root / 'out')  # no active run left
            argv = ['abandon', '--output', str(harness.root / 'out'), '--run', str(harness.root / 'nothing')]
            with patch('sys.argv', argv), patch('sys.stderr'):
                with self.assertRaises(SystemExit) as error:
                    abandon_main()
            self.assertEqual(error.exception.code, 3)
            self.assertFalse((harness.root / 'nothing').exists())


class PruneAbandonedTests(unittest.TestCase):
    def layout(self, root):
        synthetic_run(root, 'old-abandoned', '2026-10-01T00:00:00+00:00', status='abandoned', loaded=False)
        synthetic_run(root, 'old-partial', '2026-10-02T00:00:00+00:00', status='partial', loaded=False)
        synthetic_run(root, 'old-failed', '2026-10-02T12:00:00+00:00', status='failed', loaded=False)
        synthetic_run(root, 'w1', '2026-10-03T00:00:00+00:00')
        synthetic_run(root, 'w2', '2026-10-10T00:00:00+00:00')
        synthetic_run(root, 'new-partial', '2026-10-11T00:00:00+00:00', status='partial', loaded=False)
        synthetic_run(root, 'new-abandoned', '2026-10-12T00:00:00+00:00', status='abandoned', loaded=False)

    def test_without_the_option_nothing_changes(self):
        with tempfile.TemporaryDirectory() as root:
            self.layout(root)
            report = prune_runs(root, keep=2, apply=True)
            self.assertEqual(report['deleted'], [])
            self.assertNotIn('prune_abandoned', report)
            self.assertEqual(report['kept']['old-abandoned'], 'status_abandoned')
            self.assertEqual(report['kept']['old-partial'], 'status_partial')
            self.assertEqual(len([p for p in Path(root).iterdir() if p.is_dir()]), 7)

    def test_the_option_deletes_runs_older_than_the_newest_loaded_one(self):
        with tempfile.TemporaryDirectory() as root:
            self.layout(root)
            synthetic_run(root, 'active', '2026-10-01T06:00:00+00:00', status='partial', loaded=False)
            (Path(root) / 'active_run.json').write_text(
                json.dumps({'directory': str((Path(root) / 'active').resolve())}), encoding='utf-8')
            report = prune_runs(root, keep=2, apply=True, prune_abandoned=True)
            self.assertEqual(sorted(report['deleted']), ['old-abandoned', 'old-partial'])
            self.assertFalse(any((Path(root) / n).exists() for n in ('old-abandoned', 'old-partial')))  # details/ too
            self.assertEqual(report['prune_abandoned'], {'before': '2026-10-10T00:00:00+00:00'})
            self.assertEqual(report['kept']['old-failed'], 'status_failed')
            self.assertEqual(report['kept']['new-partial'], 'status_partial')
            self.assertEqual(report['kept']['active'], 'active')
            self.assertEqual(report['kept']['w1'], 'newest_loaded')

    def test_the_option_without_a_loaded_run_deletes_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            synthetic_run(root, 'old-abandoned', '2026-10-01T00:00:00+00:00', status='abandoned', loaded=False)
            synthetic_run(root, 'unloaded', '2026-10-05T00:00:00+00:00', loaded=False)
            report = prune_runs(root, keep=2, apply=True, prune_abandoned=True)
            self.assertEqual(report['deleted'], [])

    def test_a_stale_run_still_read_by_a_kept_run_stays(self):
        with tempfile.TemporaryDirectory() as root:
            self.layout(root)
            synthetic_run(root, 'export', '2026-10-13T00:00:00+00:00', status='running', loaded=False,
                          evidence=['old-partial'])
            report = prune_runs(root, keep=2, apply=True, prune_abandoned=True)
            self.assertEqual(report['kept']['old-partial'], 'referenced_evidence')
            self.assertTrue((Path(root) / 'old-partial').is_dir())



class PruneImagesTests(unittest.TestCase):
    """Loaded runs keep everything but their image bytes, which are in object storage."""

    def loaded_runs(self, root, count=2):
        harness = Harness(Path(root))
        for _ in range(count):
            self.assertEqual(harness.run('out', FIRST, first_pages(), '--new-run')[0], 0)
        runs = harness.runs('out')
        for directory in runs:
            seal(directory)
        return harness, runs

    def test_image_bytes_stay_when_the_manifest_changed_after_loading_and_pruning_keeps_the_seal(self):
        with tempfile.TemporaryDirectory() as root:
            harness, runs = self.loaded_runs(root)
            changed, other = runs
            manifest = changed / 'media' / 'products_with_local_images.jsonl'
            manifest.write_text(manifest.read_text(encoding='utf-8') + '\n', encoding='utf-8')  # an image retry rewrote it
            report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual(report['kept'][changed.name], 'loaded_inputs_changed')  # not a loaded run any more
            self.assertTrue((changed / 'media/images').is_dir())
            self.assertEqual([item['run'] for item in report['images_pruned']], [other.name])
            # Image bytes are not loader inputs: the pruned run is still the run that was loaded.
            self.assertEqual(prune_runs(harness.root / 'out', keep=2)['kept'][other.name], 'newest_loaded')

    def test_kept_loaded_runs_lose_only_their_image_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            harness, runs = self.loaded_runs(root)
            target = runs[0]
            kept_files = sorted(p.relative_to(target).as_posix() for p in target.rglob('*') if p.is_file()
                                and not p.relative_to(target).as_posix().startswith(('media/images/', 'media/categories/')))
            images = len(list((target / 'media/images').iterdir()))
            dry = prune_runs(harness.root / 'out', keep=2)
            self.assertEqual(sorted(dry['images_prunable']), sorted(r.name for r in runs))
            self.assertTrue((target / 'media/images').is_dir())
            report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual(report['deleted'], [])
            done = {item['run']: item for item in report['images_pruned']}
            self.assertEqual(sorted(done), sorted(r.name for r in runs))
            self.assertEqual((done[target.name]['folders'], done[target.name]['files']),
                             (['media/images', 'media/categories'], images))
            self.assertGreater(done[target.name]['bytes'], 0)
            self.assertFalse((target / 'media/images').exists() or (target / 'media/categories').exists())
            remaining = sorted(p.relative_to(target).as_posix() for p in target.rglob('*') if p.is_file())
            self.assertEqual(sorted(set(remaining) - {'media/images_pruned.json'}), kept_files)
            pruned = json.loads((target / 'media/images_pruned.json').read_text(encoding='utf-8'))
            self.assertEqual((pruned['job_id'], pruned['files']), (target.name, images))
            # Verification knows the files are gone on purpose; the manifest and links still pass.
            verified = verify_assets(target / 'media/products_with_local_images.jsonl', target / 'media')
            self.assertEqual((verified['result'], verified['images_pruned']), ('completed', True))
            # Nothing to do the second time.
            again = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertNotIn('images_pruned', again)

    def test_a_pruned_run_is_not_resumed_and_the_next_run_downloads_again(self):
        with tempfile.TemporaryDirectory() as root:
            harness, runs = self.loaded_runs(root, 1)
            prune_runs(harness.root / 'out', keep=2, apply=True)
            code, _ = harness.run('out', FIRST, first_pages(), '--resume-from', str(runs[0]))
            self.assertEqual(code, 3)
            with self.assertRaisesRegex(ValueError, 'prune'):
                localize_images(runs[0] / 'products.jsonl', runs[0] / 'media', None)
            # The shared history names no local file: without storage settings the next run downloads.
            code, session = harness.run('out', FIRST, first_pages())
            self.assertEqual(code, 0)
            self.assertTrue(any('img.example' in c[1] for c in session.calls if c[0] == 'GET'))
            newest = [r for r in harness.runs('out') if r != runs[0]][0]
            self.assertTrue(any((newest / 'media/images').iterdir()))

    def test_unloaded_locked_and_interrupted_cases(self):
        with tempfile.TemporaryDirectory() as root:
            harness = Harness(Path(root))
            self.assertEqual(harness.run('out', FIRST, first_pages())[0], 0)
            target = harness.runs('out')[0]
            report = prune_runs(harness.root / 'out', keep=2, apply=True)  # no marker: nothing at all
            self.assertFalse({'images_pruned', 'images_prunable', 'images_kept'} & set(report))
            self.assertTrue((target / 'media/images').is_dir())
            seal(target)
            with writer_lock(target / 'media' / '.image-download.lock'):
                report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual(report['images_kept'], {target.name: 'in_use'})
            self.assertFalse((target / 'media/images_pruned.json').exists())
            # A removal stopped after the mark: the next prune finishes it.
            (target / 'media/images_pruned.json').write_text(json.dumps({'schema_version': 1}), encoding='utf-8')
            report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual(report['images_pruned'][0]['run'], target.name)
            self.assertFalse((target / 'media/images').exists())

    def test_another_runs_files_are_untouched(self):
        # Images are not shared between runs (logic 4): removing one run's bytes leaves the other's intact.
        with tempfile.TemporaryDirectory() as root:
            harness, runs = self.loaded_runs(root)
            (runs[1] / 'pipeline_loaded.json').unlink()
            report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual([item['run'] for item in report['images_pruned']], [runs[0].name])
            verified = verify_assets(runs[1] / 'media/products_with_local_images.jsonl', runs[1] / 'media')
            self.assertEqual(verified['result'], 'completed')
            self.assertNotIn('images_pruned', verified)

    def test_a_run_another_run_reads_keeps_its_images(self):
        with tempfile.TemporaryDirectory() as root:
            harness, runs = self.loaded_runs(root, 1)
            reader = synthetic_run(harness.root / 'out', 'reader', '2026-10-30T00:00:00+00:00', status='running',
                                   loaded=False)
            (reader / 'run_report.json').write_text(json.dumps({'detail_evidence_dirs': [str(runs[0].resolve())]}),
                                                    encoding='utf-8')
            report = prune_runs(harness.root / 'out', keep=2, apply=True)
            self.assertEqual(report['images_kept'], {runs[0].name: 'referenced_evidence'})
            self.assertTrue((runs[0] / 'media/images').is_dir())


if __name__ == '__main__':
    unittest.main()
