"""Offline regression tests for file persistence, parallel lists and category images."""
import json
from pathlib import Path
import tempfile
import threading
import unittest

from product_collector.checkpoint import open_collection, CollectionCheckpoint
from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.media import localize_images
from test_product_collector import FakeSession, listing
from test_media import FakeFetcher, png


class FileParallelTests(unittest.TestCase):
    def test_parallel_overlap_single_writer_and_duplicate_category_merge(self):
        barrier = threading.Barrier(2)
        owners = {}
        lock = threading.Lock()

        class Session(FakeSession):
            def get(self, url, **kwargs):
                with lock:
                    owners.setdefault(id(self), set()).add(threading.get_ident())
                barrier.wait(timeout=5)
                return super().get(url, **kwargs)

        adapter = DanawaAdapter()
        pages = {adapter.category_url(code): listing('11') for code in ('1', '2')}
        factory = lambda: Session(pages)
        client = RequestClient(None, interval=0, session=factory(), session_factory=factory)
        main_thread = threading.get_ident()
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                save = store.save
                def main_save(*args):
                    self.assertEqual(threading.get_ident(), main_thread)
                    return save(*args)
                store.save = main_save
                raw, normal, report = collect(adapter, client, [Category('1'), Category('2')],
                    total_limit=None, collect_details=False, checkpoint=store, workers=2)
                store.save([], report, client.attempts)
                self.assertEqual(len(raw), 1)
                self.assertEqual([c['category_id'] for c in normal[0]['categories']], ['1', '2'])
                self.assertEqual(len(owners), 2)
                self.assertTrue(all(len(v) == 1 for v in owners.values()))
                self.assertTrue(all(r['status_code'] == 200 for r in store.current_report()['requests']))
                product = store.directory / 'products/danawa_11.json'
                self.assertEqual(json.loads(product.read_text(encoding='utf-8'))['categories'], normal[0]['categories'])
                self.assertFalse(list(store.directory.glob('*.sqlite*')))
        client.close()

    def test_parallel_global_request_budget(self):
        adapter = DanawaAdapter()
        pages = {adapter.category_url(str(code)): listing(str(code)) for code in range(1, 9)}
        factory = lambda: FakeSession(pages)
        client = RequestClient(3, interval=0, session=factory(), session_factory=factory)
        raw, _, report = collect(adapter, client, [Category(str(code)) for code in range(1, 9)],
                                total_limit=None, collect_details=False, workers=2)
        self.assertEqual(len(client.attempts), 3)
        self.assertEqual(len(raw), 3)
        client.close()

    def test_incomplete_tail_and_missing_product_mirror_recover_from_jsonl(self):
        adapter = DanawaAdapter()
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                client = RequestClient(None, interval=0, session=FakeSession({adapter.category_url('1'): listing('11')}))
                collect(adapter, client, [Category('1')], total_limit=None, collect_details=False, checkpoint=store)
                directory = store.directory
            (directory / 'products/danawa_11.json').unlink()
            with (directory / 'checkpoint_events.jsonl').open('ab') as stream:
                stream.write(b'{"event":')
            restored = CollectionCheckpoint(directory, adapter, {}, repair_files=True)
            self.assertEqual(len(restored), 1)
            self.assertTrue((directory / 'products/danawa_11.json').is_file())
            restored.save([], restored.current_report(), [])
            self.assertEqual(len(CollectionCheckpoint(directory, readonly=True)), 1)

    def test_category_image_links_share_one_download_and_resume_without_requests(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            products = root / 'products.jsonl'
            row = {'source': 'danawa', 'product_id': '11',
                   'categories': [{'category_id': '1'}, {'category_id': '2'}],
                   'images': [{'url': url, 'role': 'representative'}]}
            products.write_text(json.dumps(row) + '\n', encoding='utf-8')
            output = root / 'media'
            fetcher = FakeFetcher({url: png()})
            localize_images(products, output, fetcher)
            self.assertEqual(fetcher.calls, [url])
            stored = json.loads((output / 'products_with_local_images.jsonl').read_text(encoding='utf-8'))
            image = stored['images'][0]
            self.assertEqual(set(image['category_paths']), {'1', '2'})
            for code, path in image['category_paths'].items():
                self.assertTrue(path.startswith(f'categories/{code}/danawa/11/'))
                self.assertEqual((output / path).read_bytes(), png())
            row['categories'].append({'category_id': '3'})
            products.write_text(json.dumps(row) + '\n', encoding='utf-8')
            resumed = FakeFetcher({})
            localize_images(products, output, resumed)
            self.assertEqual(resumed.calls, [])
            stored = json.loads((output / 'products_with_local_images.jsonl').read_text(encoding='utf-8'))
            self.assertEqual(set(stored['images'][0]['category_paths']), {'1', '2', '3'})


if __name__ == '__main__':
    unittest.main()
