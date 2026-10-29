"""Policy 3 (crawler-optimization/image-retry.md): retries of this run's transient failures, the
failure rate of the image step, failed entries in verify_assets and stored stand-ins.
Fake fetchers and storage, no network; the retry waits are patched to zero."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests

from product_collector.media import (FAILURE_MIN_SAMPLE, RETRY_POLICY_VERSION, ImageStep, StepCancelled,
                                     failure_kind, localize_images)
from product_collector.media_resume import download_history
from product_collector.validation import verify_assets
from test_image_index import FakeStore, not_found
from test_media import png
from test_memory_equivalence import FIRST, Harness, first_pages


def http_error(code):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(str(code), response=response)


class SequenceFetcher:
    """Each URL answers its values in turn; the last one repeats."""
    def __init__(self, values):
        self.values, self.calls = {url: list(v) if isinstance(v, list) else [v] for url, v in values.items()}, []

    def fetch(self, url):
        self.calls.append(url)
        answers = self.values[url]
        value = answers.pop(0) if len(answers) > 1 else answers[0]
        if isinstance(value, BaseException):
            raise value
        return value


def urls(count, prefix='u'):
    return [f'https://img.example/{prefix}{i}.png' for i in range(count)]


class Base(unittest.TestCase):
    def setUp(self):
        waits = patch('product_collector.media.RETRY_WAITS', (0.0, 0.0))
        waits.start()
        self.addCleanup(waits.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def products(self, galleries, name='products.jsonl'):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [{'source': 'danawa', 'product_id': str(10 + i), 'categories': [{'category_id': '1'}],
                 'images': [{'url': u, 'role': 'gallery', 'order': n} for n, u in enumerate(gallery)]}
                for i, gallery in enumerate(galleries)]
        path.write_text(''.join(json.dumps(r) + '\n' for r in rows), encoding='utf-8')
        return path

    def run_step(self, galleries, values, **options):
        fetcher = SequenceFetcher(values)
        report = localize_images(self.products(galleries), self.root / 'media', fetcher, **options)
        return fetcher, report

    def manifest(self):
        path = self.root / 'media/products_with_local_images.jsonl'
        return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]


class RetryTests(Base):
    def test_a_transient_failure_is_tried_again_and_recovers(self):
        url = urls(1)[0]
        fetcher, report = self.run_step([[url]], {url: [requests.ConnectionError('reset'), png()]})
        self.assertEqual(fetcher.calls, [url, url])
        self.assertEqual(self.manifest()[0]['images'][0]['status'], 'downloaded')
        self.assertEqual(report['retry']['passes'], [dict(round=1, targets=1, attempted=1, recovered=1)])
        self.assertEqual((report['result'], report['failures']['failed']), ('completed', 0))
        # Attempts are counted, the final state is not: failed stays the number of failed requests.
        self.assertEqual((report['failed'], report['downloaded']), (1, 1))

    def test_at_most_two_more_tries_and_only_transient_ones(self):
        transient, gone, bad = urls(3)
        values = {transient: requests.Timeout('slow'), gone: not_found(), bad: ValueError('saved image checksum unconfirmed')}
        fetcher, report = self.run_step([[transient, gone, bad]], values)
        self.assertEqual(fetcher.calls.count(transient), 3)
        self.assertEqual((fetcher.calls.count(gone), fetcher.calls.count(bad)), (1, 1))
        rounds = [json.loads(line).get('retry_round') for line in
                  (self.root / 'media/image_downloads.jsonl').read_text(encoding='utf-8').splitlines()
                  if line and json.loads(line)['url'] == transient]
        self.assertEqual(rounds, [None, 1, 2])
        self.assertEqual(report['failures']['by_kind'], {'transient': 1, 'internal': 1, 'external': 1, 'blocked': 0})

    def test_a_resume_does_not_add_tries(self):
        url = urls(1)[0]
        _, first = self.run_step([[url]], {url: requests.ConnectionError('reset')})
        self.assertEqual(first['result'], 'partial')
        fetcher, again = self.run_step([[url]], {url: png()})
        self.assertEqual(fetcher.calls, [])  # both tries of this run directory are used
        self.assertEqual(again['result'], 'partial')

    def test_a_try_cut_short_by_a_crash_still_counts(self):
        url = urls(1)[0]
        with self.assertRaises(KeyboardInterrupt):
            self.run_step([[url]], {url: [requests.ConnectionError('reset'), KeyboardInterrupt()]})
        state = json.loads((self.root / 'media/image_retry.json').read_text(encoding='utf-8'))
        self.assertEqual((state['rounds'], state['retry_policy_version']), ({url: 1}, RETRY_POLICY_VERSION))
        fetcher, report = self.run_step([[url]], {url: png()})
        self.assertEqual(fetcher.calls, [url])  # the second try only
        self.assertEqual(report['result'], 'completed')

    def test_a_stop_during_the_wait_sends_nothing_and_publishes_what_there_is(self):
        url = urls(1)[0]
        products = self.products([[url]])
        holder = {}

        class Cancelling(SequenceFetcher):
            def fetch(self, u):
                holder['step'].cancel.set()  # a Ctrl+C arrives while this request runs
                return super().fetch(u)

        fetcher = Cancelling({url: requests.ConnectionError('reset')})
        with patch('product_collector.media.RETRY_WAITS', (30.0, 30.0)):
            step = ImageStep(self.root / 'media', fetcher).open()
            holder['step'] = step
            try:
                with self.assertRaises(StepCancelled):
                    step.run_batch(products)
            finally:
                step.close()
        self.assertEqual(fetcher.calls, [url])
        report = json.loads((self.root / 'media/download_report.json').read_text(encoding='utf-8'))
        self.assertEqual((report['result'], report['failures']['blocked_by']), ('partial', ['interrupted']))

    def test_no_retry_after_a_block_or_the_budget(self):
        blocked, other = urls(2)
        fetcher, report = self.run_step([[other, blocked]], {other: requests.ConnectionError('reset'),
                                                             blocked: http_error(429)})
        self.assertEqual(fetcher.calls, [other, blocked])
        self.assertEqual(report['failures']['blocked_by'], ['http_429'])
        self.assertEqual(report['result'], 'partial')
        # A resume does not try the blocked URL again, and the step stays partial.
        fetcher, again = self.run_step([[other, blocked]], {other: png(), blocked: png()})
        self.assertEqual(fetcher.calls, [other])
        self.assertEqual((again['failures']['blocked_by'], again['result']), (['blocked_failures'], 'partial'))
        fetcher, report = self.run_step([[u] for u in urls(2, 'b')], {u: requests.ConnectionError('x') for u in urls(2, 'b')},
                                        max_requests=2)
        self.assertEqual(len(fetcher.calls), 2)
        self.assertEqual(report['retry']['passes'], [])


class RateTests(Base):
    def run_step(self, galleries, values, **options):
        # Every judgement starts a run directory of its own.
        self.root = Path(tempfile.mkdtemp(dir=self.directory.name))
        return super().run_step(galleries, values, **options)

    def gallery(self, count, failing, error=None):
        names = urls(count)
        values = {u: png() for u in names}
        for u in names[:failing]:
            values[u] = error or requests.ConnectionError('reset')
        return [names], values

    def test_the_limit_is_one_percent_of_the_unique_urls(self):
        _, report = self.run_step(*self.gallery(100, 1))
        self.assertEqual((report['result'], report['failures']['rate']), ('completed', 0.01))
        self.assertEqual(self.manifest()[0]['image_download_status'], 'partial')  # the product still says so
        self.assertEqual(report['products_affected'], 1)
        _, report = self.run_step(*self.gallery(100, 2))
        self.assertEqual((report['result'], report['failures']['passed']), ('partial', False))

    def test_a_small_step_allows_none(self):
        _, report = self.run_step(*self.gallery(FAILURE_MIN_SAMPLE - 1, 1))
        self.assertEqual(report['result'], 'partial')
        _, report = self.run_step(*self.gallery(FAILURE_MIN_SAMPLE - 1, 0))
        self.assertEqual(report['result'], 'completed')

    def test_shared_urls_count_once(self):
        names = urls(60)
        galleries = [names, names[:10], names[:10]]
        values = {u: png() for u in names}
        values[names[0]] = requests.ConnectionError('reset')
        _, report = self.run_step(galleries, values)
        self.assertEqual((report['failures']['unique_urls'], report['failures']['failed']), (60, 1))
        self.assertEqual(report['products_affected'], 3)
        self.assertEqual(report['result'], 'partial')  # 1/60 is above 1%

    def test_images_the_site_does_not_serve_are_left_out_with_an_alert(self):
        _, report = self.run_step(*self.gallery(100, 5, not_found()))
        self.assertEqual((report['result'], report['alerts']), ('completed', []))
        _, report = self.run_step(*self.gallery(100, 6, not_found()))
        self.assertEqual(report['result'], 'completed')
        self.assertEqual([a['signal'] for a in report['alerts']], ['image_permanent_failures'])

    def test_internal_errors_count(self):
        _, report = self.run_step(*self.gallery(100, 2, ValueError('invalid image URL')))
        self.assertEqual((report['failures']['by_kind']['internal'], report['result']), (2, 'partial'))

    def test_products_without_a_gallery_do_not_block(self):
        _, report = self.run_step([[], []], {})
        self.assertEqual((report['result'], report['products_without_gallery'], report['failures']['unique_urls']),
                         ('completed', 2, 0))
        self.assertEqual(verify_assets(self.root / 'media/products_with_local_images.jsonl',
                                       self.root / 'media')['result'], 'completed')

    def test_failure_kinds(self):
        self.assertEqual(failure_kind({'http_status': 404}), 'external')
        self.assertEqual(failure_kind({'http_status': 408}), 'transient')
        self.assertEqual(failure_kind({'http_status': 503}), 'transient')
        self.assertEqual(failure_kind({'http_status': 403}), 'blocked')
        self.assertEqual(failure_kind({'error_type': 'ImageTooLarge'}), 'external')
        self.assertEqual(failure_kind({'error_type': 'ValueError', 'error': 'image response exceeds max_bytes'}), 'external')
        self.assertEqual(failure_kind({'error_type': 'UnidentifiedImageError'}), 'external')
        self.assertEqual(failure_kind({'error_type': 'ValueError', 'error': 'invalid image URL'}), 'internal')
        self.assertEqual(failure_kind({'error_type': 'ConnectionError'}), 'transient')


class SharedCacheTests(Base):
    def test_failures_another_run_left_in_a_shared_cache_are_not_retried(self):
        url = urls(1)[0]
        products = self.products([[url]])
        cache = self.root / 'cache'
        localize_images(products, self.root / 'run1', SequenceFetcher({url: requests.ConnectionError('reset')}),
                        cache_dir=cache)
        fetcher = SequenceFetcher({url: png()})
        report = localize_images(products, self.root / 'run2', fetcher, cache_dir=cache)
        self.assertEqual((fetcher.calls, report['retry']['passes']), ([], []))


class VerifyTests(Base):
    def test_a_failed_entry_needs_no_file_but_keeps_its_links(self):
        good, bad = urls(2)
        self.run_step([[good, bad]] + [[u] for u in urls(60, 'x')],
                      {good: png(), bad: not_found(), **{u: png() for u in urls(60, 'x')}})
        media = self.root / 'media'
        report = verify_assets(media / 'products_with_local_images.jsonl', media)
        self.assertEqual((report['result'], report['failed_images']), ('completed', 1))
        rows = self.manifest()
        rows[0]['images'][1]['local_path'] = 'images/forged.png'
        rows[0]['images'][0]['product_id'] = '99'
        (media / 'rows.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows), encoding='utf-8')
        reasons = {e['reason'] for e in verify_assets(media / 'rows.jsonl', media)['errors']}
        self.assertEqual(reasons, {'failed_image_invalid', 'product_link_mismatch'})


class StandInTests(Base):
    def collect(self, values, store, run):
        products = self.products([[urls(1)[0]]], f'{run}.jsonl')
        fetcher = SequenceFetcher(values)
        report = localize_images(products, self.root / run / 'media', fetcher, index_dir=self.root / '_image_index',
                                 object_store=store, retry_transient=True)
        rows = [json.loads(line) for line in
                (self.root / run / 'media/products_with_local_images.jsonl').read_text(encoding='utf-8').splitlines()]
        return fetcher, report, rows[0]['images'][0]

    def test_an_unconfirmed_earlier_download_stands_in_after_the_retries(self):
        url = urls(1)[0]
        _, _, first = self.collect({url: png()}, FakeStore(), 'run1')
        self.assertEqual(first['status'], 'downloaded')
        broken = FakeStore(error=requests.ConnectionError('storage down'))
        fetcher, report, image = self.collect({url: requests.ConnectionError('reset')}, broken, 'run2')
        self.assertEqual(fetcher.calls, [url] * 3)
        self.assertEqual((image['status'], image['storage_unverified'], image['sha256']), ('stored', True, first['sha256']))
        self.assertEqual(image['unverified_reason'], 'reset')
        self.assertEqual((report['storage_unverified'], report['result']), (1, 'completed'))
        # The shared history keeps the failure and the last download, never the stand-in.
        known = download_history(self.root / '_image_index/image_downloads.jsonl')[url]
        self.assertEqual((known['status'], known['last_success']['sha256'], known['storage_check']),
                         ('failed', first['sha256'], 'error'))

    def test_no_stand_in_when_storage_said_missing_or_without_settings(self):
        url = urls(1)[0]
        self.collect({url: png()}, FakeStore(), 'run1')
        _, report, image = self.collect({url: requests.ConnectionError('reset')}, FakeStore(), 'run2')
        self.assertEqual((image['status'], report.get('storage_unverified')), ('failed', 0))
        _, report, image = self.collect({url: requests.ConnectionError('reset')}, None, 'run3')
        self.assertEqual((image['status'], report['stored']), ('failed', 0))

    def test_no_stand_in_after_a_block(self):
        url, other = urls(2)
        products = self.products([[url, other]], 'p.jsonl')
        localize_images(products, self.root / 'run1/media', SequenceFetcher({url: png(), other: png()}),
                        index_dir=self.root / '_image_index', object_store=FakeStore(), retry_transient=True)
        broken = FakeStore(error=requests.ConnectionError('storage down'))
        report = localize_images(products, self.root / 'run2/media',
                                 SequenceFetcher({url: requests.ConnectionError('reset'), other: http_error(403)}),
                                 index_dir=self.root / '_image_index', object_store=broken, retry_transient=True)
        self.assertEqual((report.get('storage_unverified'), report['result']), (None, 'partial'))


class PolicyVersionTests(unittest.TestCase):
    def test_a_run_started_with_older_retry_rules_is_not_resumed(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = Harness(Path(directory))
            code, _ = harness.run('out', FIRST, first_pages(), stop=('GET', 'https://img.example/12/1.png'))
            self.assertEqual(code, 130)
            target = harness.runs('out')[0]
            settings = target / 'media/image_settings.json'
            saved = json.loads(settings.read_text(encoding='utf-8'))
            settings.write_text(json.dumps({**saved, 'retry_policy_version': 1}), encoding='utf-8')
            marked = (target / 'checkpoint_state.json').read_bytes()
            code, _ = harness.run('out', FIRST, first_pages())
            self.assertEqual(code, 3)
            self.assertEqual((target / 'checkpoint_state.json').read_bytes(), marked)


if __name__ == '__main__':
    unittest.main()
