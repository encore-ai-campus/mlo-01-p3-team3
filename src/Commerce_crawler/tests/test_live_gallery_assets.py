"""Opt-in: saved detail evidence, three products, at most ten image requests."""
import json
import os
import unittest
from pathlib import Path
from uuid import uuid4

from product_collector.danawa import DanawaAdapter
from product_collector.media import ImageFetcher, localize_images
from product_collector.reprocess import apply_detail
from product_collector.storage import atomic_json, read_jsonl, write_jsonl
from product_collector.validation import verify_assets


@unittest.skipUnless(os.environ.get('DANAWA_IMAGE_LIVE') == '1', 'opt-in: DANAWA_IMAGE_LIVE=1')
class LiveGalleryAssetsTests(unittest.TestCase):
    def test_three_saved_products_download_verify_and_zero_request_resume(self):
        run = Path(os.environ['DANAWA_ASSET_RUN'])
        original = read_jsonl(run / 'gallery_assets/audit/products_verified.jsonl')
        selected = [next(row for row in original if len(row.get('images') or []) == count) for count in (4, 5, 1)]
        source = {r['product_id']: r for r in read_jsonl(run / 'product_raw.jsonl')}
        output = run / f'refactor_smoke_{uuid4().hex[:8]}'
        output.mkdir()
        adapter, products = DanawaAdapter(), []
        for previous in selected:
            code = previous['product_id']
            saved = json.loads((run / 'gallery_assets/details' / f'{code}.json').read_text(encoding='utf-8'))
            record = source[code]
            apply_detail(record, adapter, saved['html'], saved['collected_at'])
            product = adapter.normalize(record)
            self.assertEqual([i['url'] for i in product['images']], [i['url'] for i in previous['images']])
            products.append(product)
        write_jsonl(output / 'products.jsonl', products)
        self.assertEqual(sum(len(p['images']) for p in products), 10)
        fetcher = ImageFetcher(interval=1, timeout=20)
        try:
            first = localize_images(output / 'products.jsonl', output / 'media', fetcher, max_requests=10,
                                    cache_dir=run.parent / '_asset_cache/media')
        finally:
            fetcher.close()
        verified = verify_assets(output / 'media/products_with_local_images.jsonl', output / 'media')
        self.assertEqual((first['result'], verified['result']), ('completed', 'completed'))
        class NoNetwork:
            def fetch(self, url):
                raise AssertionError('resume must not request any URL')
        resumed = localize_images(output / 'products.jsonl', output / 'media', NoNetwork(),
                                  cache_dir=run.parent / '_asset_cache/media')
        self.assertEqual(resumed['requests'], 0)
        atomic_json(output / 'smoke_report.json', {'products': len(products), 'gallery_images': 10,
                    'detail_requests': 0, 'first_download': first, 'resume': resumed, 'verification': verified})
        print(json.dumps({'smoke_output': str(output.resolve()), 'image_requests': first['requests'],
                          'image_links': verified['image_links'], 'resume_requests': resumed['requests']}, ensure_ascii=False))


if __name__ == '__main__':
    unittest.main()
