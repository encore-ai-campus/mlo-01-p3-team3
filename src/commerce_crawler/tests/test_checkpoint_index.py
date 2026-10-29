"""Collector memory: the checkpoint keeps a position index, the image step and reports stream.

Synthetic pages only; no network."""
import copy
import gc
import json
from pathlib import Path
import random
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from product_collector import quality
from product_collector.checkpoint import CollectionCheckpoint, encoded, open_collection
from product_collector.core import AttemptLog, Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.media import localize_images
from test_gallery_assets import gallery
from test_media import FakeFetcher, png
from test_pagination_gallery import PagingSession, product, templates
from test_product_collector import FakeSession, detail, listing

CATEGORY = '1523647'


def listing_page(products):
    return f'0:{{"a":"$@1","q":"?cate={CATEGORY}"}}\n1:' + json.dumps(
        {'success': True, 'data': {'products': products}}, ensure_ascii=False)


def big(code, size):
    value = product(code)
    value['description'] = {'pc': {'raw': f'{code} ' + 'x' * size}}
    return value


def listed(root, count, page_size=50, size=0, collect_details=False):
    """A listing-only collection of count products in pages of page_size, saved and exported."""
    adapter = DanawaAdapter(templates())
    codes = [str(10000 + i) for i in range(count)]

    class Pages:
        """Built when requested, so the fixture itself does not hold every page."""
        def __getitem__(self, key):
            start = (key[1] - 1) * page_size
            return listing_page([big(c, size) for c in codes[start:start + page_size]])
    pages = Pages()
    with open_collection(Path(root), adapter, {}) as store:
        client = RequestClient(None, interval=0, session=PagingSession(pages))
        raw, normalized, report = collect(adapter, client, [Category(CATEGORY, limit=None, max_pages=None)],
                                          total_limit=None, collect_details=collect_details, checkpoint=store)
        store.save([], report, client.attempts)
        store.export()
    return store.directory, raw, normalized, report


class IndexTests(unittest.TestCase):
    def test_records_are_read_from_their_line_and_one_event_is_cached(self):
        with tempfile.TemporaryDirectory() as root:
            directory, raw, _, _ = listed(root, 6, page_size=3)
            store = CollectionCheckpoint(directory, DanawaAdapter(), {})
            self.assertEqual(len(store), 6)
            opened = []
            original = Path.open
            def counting(path, *args, **kwargs):
                if path.name == 'checkpoint_events.jsonl':
                    opened.append(path)
                return original(path, *args, **kwargs)
            with patch.object(Path, 'open', counting):
                loaded = [store.load(key) for key in store.keys()]
            # Two listing pages: two events read, each product once.
            self.assertEqual(len(opened), 2)
            self.assertEqual(loaded, list(raw))
            self.assertEqual(loaded, [json.loads(line) for line in
                                      (directory / 'product_raw.jsonl').read_text(encoding='utf-8').splitlines()])

    def test_an_index_that_does_not_match_the_journal_is_an_error(self):
        with tempfile.TemporaryDirectory() as root:
            directory, _, _, _ = listed(root, 2, page_size=1)
            store = CollectionCheckpoint(directory, DanawaAdapter(), {})
            first, second = store.keys()
            store.index[first] = store.index[second][:2] + store.index[first][2:]
            store._cached = (None, None)
            with self.assertRaisesRegex(ValueError, '색인'):
                store.load(first)

    def test_a_save_after_an_interrupted_tail_points_past_it(self):
        with tempfile.TemporaryDirectory() as root:
            directory, _, _, _ = listed(root, 1)
            with (directory / 'checkpoint_events.jsonl').open('ab') as stream:
                stream.write(b'{"event": {"seq"')
            store = CollectionCheckpoint(directory, DanawaAdapter(), {})
            key = store.keys()[0]
            record = store.load(key)
            record['detail_status'] = 'request_failed'
            store.save([record], store.current_report(), [])
            store._cached = (None, None)
            self.assertEqual(store.load(key)['detail_status'], 'request_failed')
            reopened = CollectionCheckpoint(directory, DanawaAdapter(), {})
            self.assertEqual(reopened.load(key)['detail_status'], 'request_failed')

    def test_resumed_details_read_only_the_products_they_change(self):
        class Interrupted(FakeSession):
            def get(self, url, **kwargs):
                if url.endswith('pcode=12'):
                    raise KeyboardInterrupt()
                return super().get(url, **kwargs)
        adapter = DanawaAdapter()
        pages = {adapter.category_url('1'): listing('11', '12', '13'),
                 **{f'https://prod.danawa.com/info/?pcode={c}': gallery(c, 1)[0] for c in ('11', '12', '13')}}
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter, RequestClient(None, interval=0, session=Interrupted(pages)),
                            [Category('1', limit=None)], total_limit=None, checkpoint=store)
            with open_collection(Path(root), adapter, {}) as store:
                loaded = []
                original = store.load
                with patch.object(store, 'load', side_effect=lambda key: loaded.append(key[1]) or original(key)):
                    raw, _, report = collect(adapter, RequestClient(None, interval=0, session=FakeSession(pages)),
                                             [Category('1', limit=None)], total_limit=None, checkpoint=store)
                self.assertEqual(loaded, ['12', '13'])  # 11 was already ok: skipped from the index
                self.assertEqual([r['detail_status'] for r in raw], ['ok'] * 3)
                self.assertEqual(report['result'], 'completed')


class NormalizationTests(unittest.TestCase):
    def test_a_journal_round_trip_does_not_change_the_normalized_record(self):
        """The report is computed from the normalization saved with each product, which for a resumed
        run was made from the record before it was written; the old code renormalized the read-back copy."""
        adapter = DanawaAdapter()
        extra = {'image': ['https://img.example/a.png'], 'keywords': 'a, b',
                 'additionalProperty': [{'name': '크기', 'value': '10', 'unitText': 'cm'}],
                 'offers': {'@type': 'AggregateOffer', 'lowPrice': '1', 'offers': [
                     {'@type': 'Offer', 'price': '2', 'seller': {'@id': 's1', 'name': '판매처'}},
                     {'@id': 's1', 'name': '판매처', 'url': 'https://seller.example'}]}}
        pages = {adapter.category_url('1'): listing('11', '12'),
                 'https://prod.danawa.com/info/?pcode=11': gallery('11', 2)[0],
                 'https://prod.danawa.com/info/?pcode=12': detail('12', extra)}
        raw, _, _ = collect(adapter, RequestClient(None, interval=0, session=FakeSession(pages)),
                            [Category('1', limit=None)], total_limit=None)
        for record in raw:
            self.assertEqual(adapter.normalize(record), adapter.normalize(json.loads(encoded(record))))


def reference_requests(attempts, versions, base):
    """The request entries of an event as the collector computed them before (every attempt encoded)."""
    current = {index: encoded(attempt) for index, attempt in enumerate(list(attempts))}
    return ([{'id': base + index + 1, 'data': json.loads(data)} for index, data in current.items()
             if versions.get(index) != data], current)


class RequestLogTests(unittest.TestCase):
    def test_incremental_request_log_matches_full_comparison(self):
        """Random finishes, late field changes and saves: every event carries the same requests."""
        with tempfile.TemporaryDirectory() as root:
            store = CollectionCheckpoint(Path(root) / 'run', DanawaAdapter(), {})
            attempts, versions, rng = AttemptLog(), {}, random.Random(5)
            open_attempts = []
            for step in range(300):
                action = rng.random()
                if action < 0.35:
                    attempts.append({'url': f'u{len(attempts)}', 'status_code': None, 'error': None})
                    open_attempts.append(len(attempts) - 1)
                elif action < 0.6 and open_attempts:
                    index = open_attempts.pop(rng.randrange(len(open_attempts)))
                    attempts[index]['status_code'] = rng.choice([200, 500])
                    attempts.mark_finished(index)
                elif action < 0.7 and open_attempts:
                    attempts[rng.choice(open_attempts)]['error'] = f'retrying {step}'
                else:
                    expected, versions = reference_requests(attempts, versions, store.request_base)
                    with patch('product_collector.checkpoint.os.fsync'):
                        store.save([], {'categories': []}, attempts)
                    event = json.loads(store.journal.read_text(encoding='utf-8').splitlines()[-1])['event']
                    self.assertEqual(event['requests'], expected, step)
            self.assertGreater(store._stable_attempts, 0)
            self.assertLess(len(store.attempt_versions), len(attempts))

    def test_a_finished_attempt_is_not_encoded_again(self):
        with tempfile.TemporaryDirectory() as root:
            store = CollectionCheckpoint(Path(root) / 'run', DanawaAdapter(), {})
            attempts = AttemptLog()
            for index in range(50):
                attempts.append({'url': f'u{index}'})
                attempts.mark_finished(index)
            store.save([], {'categories': []}, attempts)
            attempts.append({'url': 'u50'})
            with patch('product_collector.checkpoint.encoded', wraps=encoded) as encoding:
                store.save([], {'categories': []}, attempts)
            self.assertEqual([c.args[0] for c in encoding.call_args_list if 'url' in c.args[0]], [{'url': 'u50'}])

    def test_the_request_log_keeps_the_payload_that_was_sent(self):
        payload = [{'page': 1}]
        client = RequestClient(None, interval=0, session=PagingSession({('https://prod.danawa.com/list/?cate=1', 1): listing_page([])}))
        client.post('https://prod.danawa.com/list/?cate=1', payload, {})
        payload[0]['page'] = 2
        self.assertEqual(client.attempts[0]['payload'], [{'page': 1}])
        self.assertTrue(client.attempts.finished(0))


class ProjectionTests(unittest.TestCase):
    def test_the_gate_judges_projections_like_rows(self):
        rows = []
        for i in range(120):
            rows.append({'product_id': str(i), 'detail_status': ['ok', 'unrecognized', 'identity_mismatch'][i % 3 and i % 7 == 0],
                         'categories': [{'category_id': str(i % 4)}], 'images': [{'url': 'x'}] * (i % 3),
                         'specifications': [{'name': 'a', 'value': '1'}] if i % 2 else [{'name': 'a', 'value': ''}],
                         'offers': [{'price': 1}] if i % 5 else [{'low_price': None}]})
        report = {'categories': []}
        reused = {'3', '4'}
        self.assertEqual(quality.evaluate(rows, report, 'warn', reused),
                         quality.evaluate([quality.project(r) for r in rows], report, 'warn', reused))


class ManifestTests(unittest.TestCase):
    def test_products_changed_during_the_image_step_are_not_published(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            products = Path(root) / 'products.jsonl'
            row = {'source': 'danawa', 'product_id': '11', 'images': [{'url': url}]}
            products.write_text(json.dumps(row) + '\n', encoding='utf-8')
            output = Path(root) / 'media'
            localize_images(products, output, FakeFetcher({url: png()}))
            published = (output / 'products_with_local_images.jsonl').read_bytes()

            class Rewriting(FakeFetcher):
                def fetch(self, address):
                    products.write_text(json.dumps({**row, 'product_id': '12'}) + '\n', encoding='utf-8')
                    return super().fetch(address)
            products.write_text(json.dumps({**row, 'images': [{'url': url}, {'url': url + '?2'}]}) + '\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'changed during the image step'):
                localize_images(products, output, Rewriting({url + '?2': png('blue')}))
            self.assertEqual((output / 'products_with_local_images.jsonl').read_bytes(), published)
            self.assertFalse((output / 'products_with_local_images.jsonl.tmp').exists())


def peak(callback):
    gc.collect()
    tracemalloc.start()
    try:
        callback()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


class MemoryTests(unittest.TestCase):
    def test_peak_memory_does_not_grow_with_the_records(self):
        """Doubling the products (20 KB each) adds index entries, not records."""
        size, results = 20_000, {}
        for count in (400, 800):
            with tempfile.TemporaryDirectory() as root:
                def run():
                    directory, raw, normalized, report = listed(root, count, page_size=40, size=size)
                    localize_images(directory / 'products.jsonl', directory / 'media', FakeFetcher({}))
                    quality.apply(report, normalized.projection, 'warn')
                results[count] = peak(run)
        added = results[800] - results[400]
        # Holding the records would add at least 400 x 20 KB = 8 MB (several copies in the old collector).
        self.assertLess(added, 400 * size * 0.25, results)


if __name__ == '__main__':
    unittest.main()
