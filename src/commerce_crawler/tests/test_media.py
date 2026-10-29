"""Image localization tests use generated tiny test images; no network."""
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests
from PIL import Image

from product_collector.media import ImageFetcher, localize_images, product_images


def png(color='red'):
    stream = io.BytesIO()
    Image.new('RGB', (3, 2), color).save(stream, format='PNG')
    return stream.getvalue()


class FakeFetcher:
    def __init__(self, values):
        self.values, self.calls = values, []

    def fetch(self, url):
        self.calls.append(url)
        value = self.values[url]
        if isinstance(value, Exception):
            raise value
        return value


class MediaTests(unittest.TestCase):
    def products(self, root, rows):
        # These fixtures represent a gallery URL; listing-only is tested separately.
        for row in rows:
            if row.get('listing_image_url') and not row.get('images'):
                row['images'] = [{'url': row['listing_image_url'], 'role': 'representative', 'order': 0}]
        path = Path(root) / 'products.jsonl'
        path.write_text('\n'.join(json.dumps(row) for row in rows) + '\n', encoding='utf-8')
        return path

    def test_product_linkage_hash_bytes_and_dimensions(self):
        data = png()
        url = 'https://img.example/a.png?shrink=130:130'
        with tempfile.TemporaryDirectory() as root:
            products = self.products(root, [{'source': 'danawa', 'product_id': '11', 'listing_image_url': url}])
            output = Path(root) / 'media'
            report = localize_images(products, output, FakeFetcher({url: data}))
            record = json.loads((output / 'products_with_local_images.jsonl').read_text(encoding='utf-8'))
            entry = record['images'][0]
            self.assertEqual(record['product_id'], '11')
            self.assertEqual(entry['url'], url)
            self.assertTrue(entry['filename'].startswith('danawa_11_001_'))
            self.assertEqual(entry['filename'], Path(entry['local_path']).name)
            self.assertNotIn('local_images', record)
            self.assertNotIn('image_urls', record)
            self.assertNotIn('storage_path', entry)
            self.assertEqual((entry['width'], entry['height']), (3, 2))
            self.assertEqual(entry['sha256'], hashlib.sha256(data).hexdigest())
            self.assertEqual((output / entry['local_path']).read_bytes(), data)
            self.assertEqual(report['products_with_local_images'], 1)
            self.assertEqual(report['result'], 'completed')

    def test_duplicate_url_one_request_and_identical_bytes_one_file(self):
        a, b = 'https://img.example/a.png', 'https://img.example/b.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': str(i), 'listing_image_url': url}
                                        for i, url in enumerate((a, a, b))])
            fetcher = FakeFetcher({a: png(), b: png()})
            report = localize_images(path, Path(root) / 'media', fetcher)
            self.assertEqual(len(fetcher.calls), 2)
            self.assertEqual(report['unique_files'], 1)
            self.assertEqual(report['products_with_local_images'], 3)

    def test_resume_skips_verified_files_and_redownloads_corruption(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'listing_image_url': url}])
            output = Path(root) / 'media'
            localize_images(path, output, FakeFetcher({url: png()}))
            fetcher = FakeFetcher({})
            report = localize_images(path, output, fetcher)
            self.assertEqual((report['requests'], report['cached']), (0, 1))
            self.assertEqual(fetcher.calls, [])
            next(output.glob('images/*')).write_bytes(b'corrupted')
            report = localize_images(path, output, FakeFetcher({url: png()}))
            self.assertEqual(report['downloaded'], 1)

    def test_bad_response_failure_keeps_other_product(self):
        a, b = 'https://img.example/a.png', 'https://img.example/b.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'listing_image_url': a},
                                       {'source': 'danawa', 'product_id': '12', 'listing_image_url': b}])
            output = Path(root) / 'media'
            report = localize_images(path, output, FakeFetcher({a: b'<html>not an image</html>', b: png()}))
            self.assertEqual((report['failed'], report['downloaded']), (1, 1))
            # Not an image is the site's answer: left out of the failure rate, the step completes.
            self.assertEqual((report['result'], report['failures']['by_kind']['external']), ('completed', 1))
            self.assertEqual(len(list(output.glob('images/*'))), 1)

    def test_request_limit_preserves_pending_links(self):
        a, b = 'https://img.example/a.png', 'https://img.example/b.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11',
                                        'images': [{'url': a, 'role': 'representative'}, {'url': b, 'role': 'gallery'}]}])
            output = Path(root) / 'media'
            report = localize_images(path, output, FakeFetcher({a: png()}), max_requests=1)
            row = json.loads((output / 'products_with_local_images.jsonl').read_text())
            self.assertEqual(row['images'][1]['status'], 'budget_exhausted')
            self.assertEqual(report['requests'], 1)
            self.assertEqual(report['budget_exhausted'], 1)

    def test_interrupted_journal_can_resume(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'listing_image_url': url}])
            output = Path(root) / 'media'; output.mkdir()
            (output / 'image_downloads.jsonl').write_text('{"unfinished":', encoding='utf-8')
            localize_images(path, output, FakeFetcher({url: png()}))
            report = localize_images(path, output, FakeFetcher({}))
            self.assertEqual(report['cached'], 1)

    def test_gallery_listing_url_dedup_and_no_url_rewriting(self):
        url = 'https://img.example/a.png?shrink=53:53'
        entries = product_images({'images': [{'url': url, 'role': 'representative', 'order': 0}], 'listing_image_url': url})
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['url'], url)

    def test_fetcher_rejects_redirects_and_enforces_size(self):
        class Session:
            headers = {}
            def mount(self, *_): pass
            def get(self, url, **kwargs):
                response = requests.Response()
                response.status_code = 302 if 'redirect' in url else 200
                response.raw = io.BytesIO(b'123456')
                return response
        fetcher = ImageFetcher(interval=0, max_bytes=3, session=Session())
        with self.assertRaises(requests.HTTPError):
            fetcher.fetch('https://img.example/redirect')
        with self.assertRaises(ValueError):
            fetcher.fetch('https://img.example/large')

    def test_legacy_urls_are_read_but_only_images_are_written(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'image_urls': [url]}])
            output = Path(root) / 'media'
            localize_images(path, output, FakeFetcher({url: png()}))
            row = json.loads((output / 'products_with_local_images.jsonl').read_text())
            self.assertNotIn('image_urls', row)
            self.assertEqual(row['images'][0]['url'], url)

    def test_snapshot_is_not_rewritten_per_image(self):
        from product_collector.storage import write_jsonl
        urls = [f'https://img.example/{i}.png' for i in range(6)]
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'images': [{'url': u} for u in urls]}])
            with patch('product_collector.media.write_jsonl', wraps=write_jsonl) as writer:
                localize_images(path, Path(root) / 'media', FakeFetcher({u: png() for u in urls}))
            self.assertEqual(writer.call_count, 1)

    def test_old_local_history_migrates_into_shared_store_without_requests(self):
        url = 'https://img.example/a.png'
        with tempfile.TemporaryDirectory() as root:
            path = self.products(root, [{'source': 'danawa', 'product_id': '11', 'images': [{'url': url}]}])
            output, shared = Path(root) / 'media', Path(root) / 'shared'
            localize_images(path, output, FakeFetcher({url: png()}))
            report = localize_images(path, output, FakeFetcher({}), cache_dir=shared)
            self.assertEqual(report['requests'], 0)
            # Both logical paths refer to the same bytes on supported filesystems.
            self.assertEqual(len(list(shared.glob('images/*'))), 1)


if __name__ == '__main__':
    unittest.main()
