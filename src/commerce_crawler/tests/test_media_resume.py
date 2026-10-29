"""File-only resume regression. No requests to Danawa or image servers."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import requests

from product_collector.checkpoint import writer_lock
from product_collector.media import localize_images
from product_collector.media_resume import ValidationMemo
from product_collector.storage import write_jsonl
from product_collector.validation import image_errors
from test_media import FakeFetcher, png


class FastResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.products = self.root / 'products.jsonl'
        self.media = self.root / 'media'
        self.urls = ['https://img.example/a.png', 'https://img.example/b.png']
        self.write_products(self.urls)

    def write_products(self, urls):
        write_jsonl(self.products, [dict(source='danawa', product_id='11',
            categories=[{'category_id': '1523647'}, {'category_id': '15241240'}],
            images=[{'url': u, 'role': 'gallery', 'order': i} for i, u in enumerate(urls)])])

    def run_images(self, values=None, **kwargs):
        return localize_images(self.products, self.media,
                               FakeFetcher(values or {}), **kwargs)

    def test_warm_resume_no_hash_no_journal_growth_one_export(self):
        self.write_products([f'https://img.example/{i}.png' for i in range(100)])
        urls = [f'https://img.example/{i}.png' for i in range(100)]
        self.run_images({u: png() for u in urls})
        journal = self.media / 'image_downloads.jsonl'
        before = journal.read_bytes()
        with patch('product_collector.media.write_jsonl', wraps=write_jsonl) as writer:
            result = self.run_images()
        self.assertEqual((result['requests'], result['cached']), (0, 100))
        self.assertEqual(result['validation_full_hashes'], 0)
        self.assertEqual(result['full_exports'], 1)
        writer.assert_called_once()
        self.assertEqual(before, journal.read_bytes())
        row = json.loads((self.media / 'products_with_local_images.jsonl').read_text())
        self.assertFalse(image_errors(row, self.media))

    def test_missing_memo_falls_back_to_actual_content_check(self):
        self.run_images({u: png() for u in self.urls})
        (self.media / 'image_validation.jsonl').unlink()
        result = self.run_images()
        self.assertEqual((result['requests'], result['cached']), (0, 2))
        self.assertEqual(result['validation_full_hashes'], 1)  # one shared blob

    def test_corrupt_memo_is_not_treated_as_verified(self):
        self.run_images({u: png() for u in self.urls})
        (self.media / 'image_validation.jsonl').write_text('{"broken":\n')
        self.assertEqual(self.run_images()['validation_full_hashes'], 1)

    def test_force_verification_hashes_even_with_valid_memo(self):
        self.run_images({u: png() for u in self.urls})
        self.assertEqual(self.run_images(verify_cache=True)['validation_full_hashes'], 1)

    def test_changed_same_length_file_with_restored_mtime_is_not_trusted(self):
        self.run_images({u: png() for u in self.urls})
        blob = next((self.media / 'images').iterdir())
        before = blob.stat()
        blob.write_bytes(b'x' * before.st_size)
        os.utime(blob, ns=(before.st_atime_ns, before.st_mtime_ns))
        result = self.run_images({u: png() for u in self.urls})
        self.assertGreater(result['requests'], 0)
        self.assertEqual(hashlib.sha256(blob.read_bytes()).hexdigest(), blob.stem)

    def test_failures_are_preserved_and_retry_is_explicit_with_attempt_metadata(self):
        a, b = self.urls
        response = requests.Response(); response.status_code = 404
        error = requests.HTTPError('404 Not Found', response=response)
        result = self.run_images({a: error, b: png()})
        self.assertEqual(result['failed'], 1)
        result = self.run_images()
        self.assertEqual((result['requests'], result['skipped_failed']), (0, 1))
        result = self.run_images({a: png()}, retry_failed=True)
        self.assertEqual(result['downloaded'], 1)
        records = [json.loads(s) for s in (self.media / 'image_downloads.jsonl').read_text().splitlines() if s.strip()]
        failed = next(r for r in records if r['status'] == 'failed')
        self.assertEqual(failed['http_status'], 404)
        self.assertTrue(failed['run_id'] and failed['failed_at'] and failed['attempted_at'])
        self.assertEqual(records[-1]['attempts'], 2)

    def test_graceful_interruption_reuses_success_and_preserves_category_links(self):
        a, b = self.urls
        class Fetcher:
            def fetch(self, url):
                if url == b:
                    raise KeyboardInterrupt()
                return png()
        with self.assertRaises(KeyboardInterrupt):
            localize_images(self.products, self.media, Fetcher())
        result = self.run_images({b: png('blue')})
        self.assertEqual((result['cached'], result['downloaded']), (1, 1))
        row = json.loads((self.media / 'products_with_local_images.jsonl').read_text())
        self.assertFalse(image_errors(row, self.media))

    def test_hard_process_exit_recovers_committed_url_without_export(self):
        code = """import sys
from pathlib import Path
from product_collector.media import localize_images
from test_media import png
class Fetcher:
    def fetch(self, url):
        if url.endswith('b.png'):
            import os
            os._exit(17)
        return png()
localize_images(Path(sys.argv[1]), Path(sys.argv[2]), Fetcher())
"""
        env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(Path.cwd()), str(Path.cwd() / 'tests')]))
        process = subprocess.run([sys.executable, '-c', code, str(self.products), str(self.media)],
                                 env=env, capture_output=True)
        self.assertEqual(process.returncode, 17)
        self.assertFalse((self.media / 'products_with_local_images.jsonl').exists())
        result = self.run_images({self.urls[1]: png('blue')})
        self.assertEqual((result['cached'], result['requests']), (1, 1))

    def test_cache_preparation_interrupt_does_not_replace_existing_mapping(self):
        self.run_images({u: png() for u in self.urls})
        path = self.media / 'products_with_local_images.jsonl'
        original = path.read_bytes()
        with patch.object(ValidationMemo, 'valid', side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt):
                self.run_images()
        self.assertEqual(path.read_bytes(), original)

    def test_existing_success_outside_request_budget_is_preserved(self):
        a, b = self.urls
        self.run_images({a: png()}, max_requests=1)
        result = self.run_images({b: png('blue')}, max_requests=1)
        self.assertEqual((result['cached'], result['downloaded']), (1, 1))
        self.assertEqual(result['products_complete'], 1)

    def test_same_source_extended_input_does_not_trust_a_stale_cursor(self):
        a, b = self.urls
        self.write_products([a]);self.run_images({a: png()})
        old = json.loads((self.media / 'image_resume.json').read_text())['input_sha256']
        self.write_products([a, b]);result = self.run_images({b: png('blue')})
        self.assertEqual((result['cached'], result['requests']), (1, 1))
        self.assertNotEqual(result['input_sha256'], old)

    def test_concurrent_writer_is_rejected_without_requests(self):
        with writer_lock(self.media / '.image-download.lock'):
            with self.assertRaises((OSError, ValueError)):
                self.run_images()

    def test_403_stops_before_any_following_request(self):
        a, b = self.urls
        response = requests.Response();response.status_code = 403
        fetcher = FakeFetcher({a: requests.HTTPError('blocked', response=response)})
        result = localize_images(self.products, self.media, fetcher)
        self.assertEqual(fetcher.calls, [a])
        self.assertEqual(result['stop_reason'], 'http_403')

    def test_memo_rejects_path_escape(self):
        outside = self.root / 'outside';outside.write_bytes(png())
        self.media.mkdir()
        with ValidationMemo(self.media) as memo:
            self.assertFalse(memo.valid(dict(status='downloaded', local_path='../outside',
                sha256=hashlib.sha256(outside.read_bytes()).hexdigest())))

    def test_timed_exports_keep_live_mapping_without_per_url_exports(self):
        with patch('product_collector.media.time.monotonic', side_effect=[0, 301, 302]):
            result = self.run_images({u: png() for u in self.urls})
        self.assertEqual(result['full_exports'], 2)  # one timed + one final


if __name__ == '__main__':
    unittest.main()
