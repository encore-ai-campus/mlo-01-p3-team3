"""Overlapping details and images (logic 2): fake fetchers, sessions and steps, no network."""
import hashlib
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

import requests

from product_collector.checkpoint import open_collection
from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.media import ImageFetcher, ImageStep, ImageWorker, StepCancelled, localize_images
from product_collector.media_resume import download_history
from product_collector.storage import write_jsonl
from tests.test_media import FakeFetcher, png
from tests.test_product_collector import FakeSession, listing
from test_gallery_assets import gallery


def product(code, *urls, category='1'):
    return {'source': 'danawa', 'product_id': code, 'categories': [{'category_id': category}],
            'images': [{'url': url, 'role': 'gallery', 'order': i} for i, url in enumerate(urls)]}


def digest(products):
    hasher = hashlib.sha256()
    for row in products:
        hasher.update((json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8'))
    return hasher.hexdigest()


def blocked(code):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(str(code), response=response)


def without_run_ids(path):
    rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
    for row in rows:
        for image in row['images']:
            image.pop('run_id', None)
            for key in [k for k in image if k.endswith('_at')]:  # wall-clock times differ when a second ticks over
                image.pop(key)
    return rows


class StepTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def overlap(self, name, products, fetcher, **options):
        """Hand products over one at a time, as the collector does, then export and finish."""
        run = self.root / name
        step = ImageStep(run / 'media', fetcher, **options).open()
        try:
            step.begin()
            for row in products:
                step.plan(row)
                step.drain()
            write_jsonl(run / 'products.jsonl', products)
            return run, step.finish(run / 'products.jsonl', digest(products))
        finally:
            step.close(completed=True)

    def batch(self, name, products, fetcher, **options):
        run = self.root / name
        run.mkdir()
        write_jsonl(run / 'products.jsonl', products)
        return run, localize_images(run / 'products.jsonl', run / 'media', fetcher, **options)

    def test_overlap_publishes_the_same_manifest_as_the_batch_step(self):
        products = [product('11', 'https://img.example/a.png', 'https://img.example/b.png'),
                    product('12', 'https://img.example/b.png', 'https://img.example/c.png', category='2'),
                    product('13')]
        values = {'https://img.example/a.png': png('red'), 'https://img.example/b.png': png('blue'),
                  'https://img.example/c.png': blocked(404)}
        first, report = self.batch('batch', products, FakeFetcher(values))
        fetcher = FakeFetcher(values)
        second, overlapped = self.overlap('overlap', products, fetcher)
        self.assertEqual(fetcher.calls, ['https://img.example/a.png', 'https://img.example/b.png',
                                         'https://img.example/c.png'])
        self.assertEqual(without_run_ids(second / 'media/products_with_local_images.jsonl'),
                         without_run_ids(first / 'media/products_with_local_images.jsonl'))
        for key in ('result', 'product_count', 'unique_urls', 'downloaded', 'failed', 'products_complete',
                    'products_without_gallery', 'unique_files'):
            self.assertEqual(overlapped[key], report[key], key)
        self.assertEqual(overlapped['input_sha256'], hashlib.sha256((second / 'products.jsonl').read_bytes()).hexdigest())

    def test_a_progress_report_without_finished_at_replaces_the_last_one_at_once(self):
        media = self.root / 'run/media'
        media.mkdir(parents=True)
        (media / 'download_report.json').write_text(json.dumps({'finished_at': 'earlier', 'result': 'completed'}),
                                                    encoding='utf-8')
        step = ImageStep(media, FakeFetcher({})).open()
        try:
            step.begin()
            report = json.loads((media / 'download_report.json').read_text(encoding='utf-8'))
            self.assertNotIn('finished_at', report)
            self.assertEqual(report['phase'], 'overlap')
            step.abort()
            self.assertNotIn('finished_at', json.loads((media / 'download_report.json').read_text(encoding='utf-8')))
            self.assertFalse((media / 'products_with_local_images.jsonl').exists())
        finally:
            step.close()

    def test_finish_publishes_only_the_products_that_were_handed_over(self):
        products = [product('11', 'https://img.example/a.png'), product('12', 'https://img.example/b.png')]
        values = {'https://img.example/a.png': png('red'), 'https://img.example/b.png': png('blue')}
        for changed in ([products[1], products[0]],                                      # order
                        [products[0], {**products[1], 'categories': [{'category_id': '9'}]}],  # category
                        [products[0], {**products[1], 'name': '다른 이름'}]):               # attribute
            with self.subTest(changed=changed):
                run = self.root / f'changed{len(list(self.root.iterdir()))}'
                step = ImageStep(run / 'media', FakeFetcher(values)).open()
                try:
                    step.begin()
                    for row in products:
                        step.plan(row)
                        step.drain()
                    write_jsonl(run / 'products.jsonl', changed)
                    with self.assertRaisesRegex(ValueError, 'changed during the image step'):
                        step.finish(run / 'products.jsonl', digest(products))
                finally:
                    step.close()
                self.assertFalse((run / 'media/products_with_local_images.jsonl').exists())
                self.assertFalse((run / 'media/products_with_local_images.jsonl.tmp').exists())

    def test_after_a_block_urls_are_still_judged_and_only_downloads_are_marked(self):
        products = [product('11', 'https://img.example/a.png'), product('12', 'https://img.example/b.png'),
                    product('13', 'https://img.example/a.png', 'https://img.example/c.png')]
        values = {'https://img.example/a.png': png('red'), 'https://img.example/b.png': blocked(429)}
        fetcher = FakeFetcher(values)
        run, report = self.overlap('blocked', products, fetcher)
        self.assertEqual(fetcher.calls, ['https://img.example/a.png', 'https://img.example/b.png'])
        self.assertEqual((report['stop_reason'], report['unique_urls'], report['result']), ('http_429', 3, 'partial'))
        rows = [json.loads(line) for line in (run / 'media/products_with_local_images.jsonl').read_text(encoding='utf-8').splitlines()]
        self.assertEqual([[i['status'] for i in r['images']] for r in rows],
                         [['downloaded'], ['failed'], ['downloaded', 'stopped_after_block']])

    def test_the_image_budget_marks_the_rest(self):
        products = [product('11', 'https://img.example/a.png'), product('12', 'https://img.example/b.png')]
        values = {'https://img.example/a.png': png('red'), 'https://img.example/b.png': png('blue')}
        fetcher = FakeFetcher(values)
        _, report = self.overlap('budget', products, fetcher, max_requests=1)
        self.assertEqual((fetcher.calls, report['budget_exhausted']), (['https://img.example/a.png'], 1))

    def test_two_urls_of_the_same_bytes_may_need_fewer_downloads_than_the_batch(self):
        # Limit 5 of the design: the same run's history knows both URLs but the file is gone.
        # The batch judges both as missing and fetches both; overlap fetches the first, the
        # file is back, and the second is judged afterwards and found. The result is the same.
        products = [product('11', 'https://img.example/u1.png'), product('12', 'https://img.example/u2.png')]
        values = {'https://img.example/u1.png': png('red'), 'https://img.example/u2.png': png('red')}
        runs = {}
        for mode in ('batch', 'overlap'):
            first, _ = self.batch(f'{mode}-first', products, FakeFetcher(values))
            for path in (first / 'media/images').iterdir():
                path.unlink()
            fetcher = FakeFetcher(values)
            if mode == 'batch':
                localize_images(first / 'products.jsonl', first / 'media', fetcher)
            else:
                step = ImageStep(first / 'media', fetcher).open()
                try:
                    step.begin()
                    for row in products:
                        step.plan(row)
                        step.drain()
                    step.finish(first / 'products.jsonl', digest(products))
                finally:
                    step.close(completed=True)
            runs[mode] = (fetcher.calls, first)
        self.assertEqual(runs['batch'][0], ['https://img.example/u1.png', 'https://img.example/u2.png'])
        self.assertEqual(runs['overlap'][0], ['https://img.example/u1.png'])
        statuses = {mode: [[i['status'] for i in r['images']] for r in without_run_ids(
            run / 'media/products_with_local_images.jsonl')] for mode, (_, run) in runs.items()}
        self.assertEqual(statuses['batch'], statuses['overlap'])

    def test_without_storage_settings_an_earlier_stored_answer_is_downloaded_again(self):
        media = self.root / 'run/media'
        media.mkdir(parents=True)
        meta = {'sha256': 'a' * 64, 'bytes': 10, 'width': 1, 'height': 1, 'format': 'PNG', 'extension': 'png'}
        (media / 'image_downloads.jsonl').write_text(
            json.dumps({'url': 'https://img.example/a.png', 'status': 'stored', **meta, 'local_path': None}) + '\n',
            encoding='utf-8')
        fetcher = FakeFetcher({'https://img.example/a.png': png('red')})
        step = ImageStep(media, fetcher).open()
        try:
            step.begin()
            step.plan(product('11', 'https://img.example/a.png'))
            step.drain()
            self.assertEqual((fetcher.calls, step.report['stored']), (['https://img.example/a.png'], 0))
        finally:
            step.close()

    def test_a_cancel_stops_after_the_request_in_flight(self):
        products = [product('11', 'https://img.example/a.png', 'https://img.example/b.png')]
        values = {'https://img.example/a.png': png('red'), 'https://img.example/b.png': png('blue')}
        media = self.root / 'run/media'
        step = ImageStep(media, None).open()

        class Interrupting(FakeFetcher):
            def fetch(self, url):
                step.cancel.set()  # Ctrl+C arrives while this request is answered
                return super().fetch(url)

        step.fetcher = fetcher = Interrupting(values)
        try:
            step.begin()
            step.plan(products[0])
            with self.assertRaises(StepCancelled):
                step.drain()
            step.abort()
        finally:
            step.close()
        self.assertEqual(fetcher.calls, ['https://img.example/a.png'])
        self.assertEqual(set(download_history(media / 'image_downloads.jsonl')), {'https://img.example/a.png'})
        self.assertNotIn('finished_at', json.loads((media / 'download_report.json').read_text(encoding='utf-8')))

    def test_a_stop_while_the_step_begins_is_cleaned_up_without_masking_it(self):
        from unittest.mock import patch
        from product_collector.__main__ import ImageResources
        images = ImageResources()
        images.step = ImageStep(self.root / 'run/media', FakeFetcher({})).open()
        with self.assertRaises(KeyboardInterrupt):
            with patch('product_collector.media.download_history', side_effect=KeyboardInterrupt):
                images.step.begin()
        images.close()  # no AttributeError, the locks are released
        self.assertIsNone(images.step)
        self.assertFalse((self.root / 'run/media/download_report.json').exists())
        ImageStep(self.root / 'run/media', FakeFetcher({})).open().close()

    def test_the_request_interval_ends_early_on_cancel_and_sends_nothing(self):
        class Session:
            headers, calls = {}, []

            def mount(self, *_):
                pass

            def get(self, url, **_):
                self.calls.append(url)
                raise AssertionError('no request after a cancel')

        session = Session()
        fetcher = ImageFetcher(interval=30, session=session)
        fetcher.cancel, fetcher.last_start = threading.Event(), fetcher.clock()
        threading.Timer(0.05, fetcher.cancel.set).start()
        started = time.monotonic()
        with self.assertRaises(StepCancelled):
            fetcher.fetch('https://img.example/a.png')
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(session.calls, [])


class FakeStep:
    def __init__(self, plan=None):
        self.cancel = threading.Event()
        self.planned, self._plan = [], plan

    def plan(self, item):
        if self._plan:
            self._plan(item)
        self.planned.append(item['product_id'])

    def drain(self):
        pass


class WorkerTests(unittest.TestCase):
    def test_products_are_planned_in_order_and_hashed_as_exported(self):
        step = FakeStep()
        worker = ImageWorker(step, capacity=2, poll=0.01)
        rows = [{**product(str(code), f'https://img.example/{code}.png'), 'name': f'상품 {code}'} for code in range(11, 16)]
        for row in rows:
            worker.offer(row)
        worker.close()
        self.assertEqual(step.planned, ['11', '12', '13', '14', '15'])
        self.assertTrue(worker.planned_all)
        self.assertEqual(worker.digest.hexdigest(), digest(rows))

    def test_a_failing_worker_with_a_full_queue_does_not_hold_the_collector(self):
        release = threading.Event()
        error = RuntimeError('image step failed')

        def plan(item):
            release.wait(5)
            raise error

        worker = ImageWorker(FakeStep(plan), capacity=1, poll=0.01)
        worker.offer(product('11'))  # taken by the worker, which waits
        while not worker.queue.empty():
            time.sleep(0.01)
        worker.offer(product('12'))  # fills the queue
        outcome = {}

        def collector():
            try:
                worker.offer(product('13'))  # would wait forever on a plain put
            except RuntimeError as exc:
                outcome['error'] = exc

        thread = threading.Thread(target=collector)
        thread.start()
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome['error'], error)
        with self.assertRaises(RuntimeError):
            worker.close()

    def test_stop_ends_the_thread_and_leaves_the_rest_unplanned(self):
        started, release = threading.Event(), threading.Event()

        def plan(item):
            started.set()
            release.wait(5)

        step = FakeStep(plan)
        worker = ImageWorker(step, capacity=10, poll=0.01)
        for code in ('11', '12', '13'):
            worker.offer(product(code))
        started.wait(5)
        threading.Timer(0.05, release.set).start()
        worker.stop()
        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(step.planned, ['11'])
        self.assertFalse(worker.planned_all)


class OnProductTests(unittest.TestCase):
    def test_every_product_is_handed_over_once_in_order_also_when_resumed(self):
        class Interrupted(FakeSession):
            def get(self, url, **kwargs):
                if url.endswith('pcode=13'):
                    raise KeyboardInterrupt()
                return super().get(url, **kwargs)

        adapter, categories = DanawaAdapter(), [Category('1', limit=None)]
        pages = {adapter.category_url('1'): listing('11', '12', '13', '14'),
                 'https://prod.danawa.com/info/?pcode=11': gallery('11', 1)[0],
                 'https://prod.danawa.com/info/?pcode=12': (500, 'error'),
                 'https://prod.danawa.com/info/?pcode=13': gallery('13', 1)[0],
                 'https://prod.danawa.com/info/?pcode=14': gallery('14', 1)[0]}
        with tempfile.TemporaryDirectory() as root:
            first = []
            with open_collection(Path(root), adapter, {}) as store:
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter, RequestClient(None, interval=0, session=Interrupted(pages)), categories,
                            total_limit=None, checkpoint=store, on_product=first.append)
            self.assertEqual([key[1] for key in first], ['11', '12'])
            second = []
            with open_collection(Path(root), adapter, {}) as store:
                raw, _, _ = collect(adapter, RequestClient(None, interval=0, session=FakeSession(pages)), categories,
                                    total_limit=None, checkpoint=store,
                                    on_product=lambda key: second.append((key[1], store.detail_status(key))))
            # 11 was skipped as finished, 12 failed before and is asked again; every product once, in order.
            self.assertEqual(second, [('11', 'ok'), ('12', 'request_failed'), ('13', 'ok'), ('14', 'ok')])
            self.assertEqual([r['product_id'] for r in raw], ['11', '12', '13', '14'])

    def test_listing_only_products_are_handed_over_too(self):
        adapter = DanawaAdapter()
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                seen = []
                collect(adapter, RequestClient(None, interval=0, session=FakeSession(
                    {adapter.category_url('1'): listing('11', '12')})), [Category('1', limit=None)],
                    total_limit=None, collect_details=False, checkpoint=store, on_product=seen.append)
        self.assertEqual([key[1] for key in seen], ['11', '12'])


if __name__ == '__main__':
    unittest.main()
