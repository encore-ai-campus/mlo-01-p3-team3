"""End-to-end gallery, resume, request deduplication and product/file identity."""
import copy
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import requests

from product_collector.core import Category, RequestClient, collect, save_run
from product_collector.danawa import DanawaAdapter
from product_collector.gallery_assets import enrich_saved_run
from product_collector.danawa import validate_identity
from product_collector.validation import verify_assets
from product_collector.media import localize_images, product_images
from test_media import FakeFetcher, png
from test_product_collector import FakeSession, detail, listing


def gallery(code, count=6):
    urls = [f'https://img.example/{code}/{i}.png' for i in range(count)]
    html = detail(code, {'image': urls})
    html += '<img id="baseImage" src="' + urls[0] + '">'
    html += '<ul id="basicThumbSlideArea">' + ''.join(f'<li><img src="{u}"></li>' for u in urls) + '</ul>'
    html += '<div id="productDescription"><img src="https://img.example/description.png"></div>'
    return html, urls


class GalleryAssetsTests(unittest.TestCase):
    def saved(self, root):
        adapter = DanawaAdapter()
        session = FakeSession({adapter.category_url('1'): listing('11', '12')})
        raw, normal, report = collect(adapter, RequestClient(interval=0, session=session),
                                      [Category('1')], collect_details=False)
        return save_run(Path(root), raw, normal, report)

    def test_full_pipeline_all_gallery_excludes_description_and_listing_and_resumes_without_requests(self):
        with tempfile.TemporaryDirectory() as root:
            run = self.saved(root)
            pages, urls = {}, []
            for code in ('11', '12'):
                html, observed = gallery(code)
                pages[f'https://prod.danawa.com/info/?pcode={code}'] = html
                urls.extend(observed)
            client = RequestClient(interval=0, session=FakeSession(pages))
            output = run / 'gallery_assets'
            report = enrich_saved_run(run, output, client)
            self.assertEqual((report['detail_requests'], report['with_gallery']), (2, 2))
            fetcher = FakeFetcher({u: png() for u in urls})
            localize_images(output / 'products.jsonl', output / 'media', fetcher)
            self.assertEqual(fetcher.calls, urls)
            result = verify_assets(output / 'media/products_with_local_images.jsonl', output / 'media')
            self.assertEqual((result['result'], result['image_links']), ('completed', 12))
            rows = [json.loads(l) for l in (output / 'media/products_with_local_images.jsonl').read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(rows[0]['images']), 6)  # never cap to 1--4
            self.assertNotEqual(rows[0]['images'][0]['filename'], rows[1]['images'][0]['filename'])
            empty = RequestClient(interval=0, session=FakeSession({}))
            enrich_saved_run(run, output, empty)
            self.assertEqual(empty.attempts, [])
            report = localize_images(output / 'products.jsonl', output / 'media', FakeFetcher({}))
            self.assertEqual(report['requests'], 0)
            # The verifier catches a swapped product association even with valid bytes.
            rows[0]['images'][0]['product_id'] = '12'
            from product_collector.storage import write_jsonl
            write_jsonl(output / 'media/products_with_local_images.jsonl', rows)
            self.assertEqual(verify_assets(output / 'media/products_with_local_images.jsonl', output / 'media')['result'], 'partial')

    def test_listing_only_is_not_silently_downloaded(self):
        self.assertEqual(product_images({'listing_image_url': 'https://img.example/thumb.png'}), [])
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'products.jsonl'
            path.write_text(json.dumps({'source': 'danawa', 'product_id': '1', 'listing_image_url': 'https://img.example/thumb.png'}))
            report = localize_images(path, Path(root) / 'media', FakeFetcher({}))
            self.assertEqual((report['requests'], report['result']), (0, 'partial'))

    def test_identity_mismatch_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_identity('<link rel="canonical" href="https://prod.danawa.com/info/?pcode=12">', '11')
        with self.assertRaises(ValueError):
            validate_identity(detail('12'), '11')

    def test_block_stops_later_image_requests_and_keeps_pending_links(self):
        response = requests.Response(); response.status_code = 429
        error = requests.HTTPError('rate limited', response=response)
        urls = ['https://img.example/a.png', 'https://img.example/b.png']
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'products.jsonl'
            path.write_text(json.dumps({'source': 'danawa', 'product_id': '1', 'images': [{'url': u} for u in urls]}))
            fetcher = FakeFetcher({urls[0]: error})
            report = localize_images(path, Path(root) / 'media', fetcher)
            self.assertEqual(fetcher.calls, urls[:1])
            self.assertEqual(report['stop_reason'], 'http_429')
            row = json.loads((Path(root) / 'media/products_with_local_images.jsonl').read_text())
            self.assertEqual(row['images'][1]['status'], 'stopped_after_block')

    def test_interruption_preserves_successful_linkage(self):
        urls = ['https://img.example/a.png', 'https://img.example/b.png']
        class Interrupted(FakeFetcher):
            def fetch(self, url):
                if url == urls[1]:
                    raise KeyboardInterrupt()
                return super().fetch(url)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'products.jsonl'
            path.write_text(json.dumps({'source': 'danawa', 'product_id': '1', 'images': [{'url': u} for u in urls]}))
            output = Path(root) / 'media'
            with self.assertRaises(KeyboardInterrupt):
                localize_images(path, output, Interrupted({urls[0]: png()}))
            row = json.loads((output / 'products_with_local_images.jsonl').read_text())
            self.assertEqual([e['status'] for e in row['images']], ['downloaded', 'pending'])
            self.assertTrue((output / row['images'][0]['local_path']).is_file())

    def test_duplicate_product_keeps_memberships_one_detail(self):
        with tempfile.TemporaryDirectory() as root:
            run = self.saved(root)
            records = [json.loads(l) for l in (run / 'product_raw.jsonl').read_text(encoding='utf-8').splitlines()]
            repeated = copy.deepcopy(records[0]); repeated['categories'] = [{'category_id': '2'}]
            from product_collector.storage import write_jsonl
            write_jsonl(run / 'product_raw.jsonl', [records[0], repeated])
            client = RequestClient(interval=0, session=FakeSession({'https://prod.danawa.com/info/?pcode=11': gallery('11')[0]}))
            report = enrich_saved_run(run, run / 'out', client)
            self.assertEqual((report['products'], report['detail_requests']), (1, 1))
            row = json.loads((run / 'out/products.jsonl').read_text(encoding='utf-8'))
            self.assertEqual(len(row['categories']), 2)

    def test_shared_cache_across_different_outputs_makes_no_detail_or_image_requests(self):
        with tempfile.TemporaryDirectory() as root:
            run = self.saved(root)
            cache = Path(root) / 'shared'
            pages, urls = {}, []
            for code in ('11', '12'):
                html, observed = gallery(code, 2)
                pages[f'https://prod.danawa.com/info/?pcode={code}'] = html
                urls.extend(observed)
            first, second = Path(root) / 'first', Path(root) / 'second'
            enrich_saved_run(run, first, RequestClient(interval=0, session=FakeSession(pages)), cache_dir=cache)
            localize_images(first / 'products.jsonl', first / 'media', FakeFetcher({u: png() for u in urls}), cache_dir=cache / 'media')
            empty = RequestClient(interval=0, session=FakeSession({}))
            report = enrich_saved_run(run, second, empty, cache_dir=cache)
            self.assertEqual(report['detail_requests'], 0)
            fetcher = FakeFetcher({})
            report = localize_images(second / 'products.jsonl', second / 'media', fetcher, cache_dir=cache / 'media')
            self.assertEqual(report['requests'], 0)
            self.assertEqual(fetcher.calls, [])
            self.assertEqual(verify_assets(second / 'media/products_with_local_images.jsonl', second / 'media')['result'], 'completed')

    def test_offline_audit_never_requests_and_detects_cache_identity_mismatch(self):
        from product_collector.audit_assets import audit_saved_assets
        with tempfile.TemporaryDirectory() as root:
            run = self.saved(root)
            output = run / 'gallery_assets'
            pages = {f'https://prod.danawa.com/info/?pcode={code}': gallery(code)[0] for code in ('11', '12')}
            enrich_saved_run(run, output, RequestClient(interval=0, session=FakeSession(pages)))
            with patch('requests.Session.request', side_effect=AssertionError('network forbidden')):
                report = audit_saved_assets(run, output)
            self.assertEqual((report['network_requests'], report['products_with_gallery'], report['gallery_image_links']), (0, 2, 12))
            self.assertEqual(report['saved_evidence_result'], 'passed')
            cached = output / 'details/11.json'
            entry = json.loads(cached.read_text(encoding='utf-8'))
            entry['product_id'] = '12'
            cached.write_text(json.dumps(entry), encoding='utf-8')
            self.assertEqual(audit_saved_assets(run, output)['saved_evidence_result'], 'failed')
            # Right file identity, but the page itself is another product's.
            entry.update(product_id='11', html=gallery('12')[0])
            cached.write_text(json.dumps(entry), encoding='utf-8')
            audit = audit_saved_assets(run, output)
            self.assertEqual(audit['saved_evidence_result'], 'failed')
            self.assertIn('identity mismatch', audit['errors'][0]['reason'])

    def test_audit_limit_writes_report_only(self):
        from product_collector.audit_assets import audit_saved_assets
        with tempfile.TemporaryDirectory() as root:
            run = self.saved(root)
            output = run / 'gallery_assets'
            pages = {f'https://prod.danawa.com/info/?pcode={code}': gallery(code)[0] for code in ('11', '12')}
            enrich_saved_run(run, output, RequestClient(interval=0, session=FakeSession(pages)))
            report = audit_saved_assets(run, output, limit=1)
            self.assertEqual((report['checked_products'], report['gallery_image_links']), (1, 6))
            self.assertEqual([p.name for p in (output / 'audit').iterdir()], ['verification_report.json'])


if __name__ == '__main__':
    unittest.main()
