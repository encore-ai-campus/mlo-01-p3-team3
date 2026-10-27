"""Opt-in real-site tests for the five configured categories.

Default discovery does NOT access the network. Set DANAWA_LIVE_TEST=1 to run.
Fresh execution: 5 category GETs + at most 10 page POSTs, no detail requests.
Optionally use previously captured HTML via DANAWA_DISCOVERY_DIR.
"""
import copy
import json
import os
import unittest
from dataclasses import replace
from pathlib import Path

from product_collector.core import RequestClient, collect, load_categories, save_run
from product_collector.danawa import DanawaAdapter
from product_collector.pagination import PaginationTemplates, pagination_metadata

ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(os.environ.get("DANAWA_LIVE_TEST") == "1", "opt-in: DANAWA_LIVE_TEST=1")
class LiveFiveCategoryPaginationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(os.environ.get("DANAWA_TEMPLATE_FILE", str(ROOT / 'config/danawa_pagination.local.json')))
        captured = json.loads(source.read_text(encoding='utf-8'))
        seed = captured['categories']['1523647']['payload']
        headers = captured.get('headers', {})
        if not any(key.lower() == 'next-action' and value for key, value in headers.items()):
            raise ValueError('live validation requires the captured Next-Action header')
        categories = load_categories(ROOT / 'config/danawa_categories.json')
        expected = {'15241194', '15240504', '1523647', '15241240', '15227279'}
        if {c.category_id for c in categories} != expected:
            raise ValueError('this live test targets the original five categories exactly')
        cls.client = RequestClient(max_requests=15, interval=1, timeout=20)
        cls.addClassCleanup(cls.client.close)
        cls.metadata = {}
        templates = {'headers': headers, 'categories': {}}
        html_pages = {}
        discovery = os.environ.get('DANAWA_DISCOVERY_DIR')
        for category in categories:
            url = DanawaAdapter().category_url(category.category_id)
            html = (Path(discovery) / (category.category_id + '.html')).read_text(encoding='utf-8') if discovery else cls.client.get(url)
            html_pages[category.category_id] = html
            meta = pagination_metadata(html, category.category_id)
            cls.metadata[category.category_id] = meta
            payload = copy.deepcopy(seed)
            for key in ('categoryCode', 'physicsCate1', 'physicsCate2', 'physicsCate3', 'physicsCate4',
                        'group', 'depth', 'bSearchOptionMultiCategoryYN'):
                payload[0][key] = meta[key]
            payload[0].update(page=1, listCount=30)
            templates['categories'][category.category_id] = {
                'payload': payload, 'headers': {'Referer': url}, 'metadata': meta}
        cls.templates = PaginationTemplates(templates)
        cls.raw, cls.normal, cls.report = collect(DanawaAdapter(cls.templates), cls.client,
                                                 [replace(c, limit=60, start_page=1, max_pages=2) for c in categories],
                                                 total_limit=300, collect_details=False)
        cls.report['validation'] = {'target': 'five_categories_pages_1_and_2',
                                    'metadata_source': 'saved_category_html' if discovery else 'fresh_category_html',
                                    'metadata': cls.metadata, 'discovery_directory': discovery,
                                    'detail_requests': 0}
        cls.output = save_run(ROOT / 'data/danawa', cls.raw, cls.normal, cls.report)
        for category_id, html in html_pages.items():
            (cls.output / (category_id + '.html')).write_text(html, encoding='utf-8')
        (cls.output / 'validated_templates.json').write_text(json.dumps(templates, ensure_ascii=False, indent=2), encoding='utf-8')
        # Reusable templates keep real headers local; public fixture contains body only.
        cls.generated_config = templates
        print('LIVE_VALIDATION_OUTPUT=' + str(cls.output), flush=True)

    def check_category(self, category_id):
        outcome = next(c for c in self.report['categories'] if c['category_id'] == category_id)
        self.assertEqual(outcome['status'], 'ok', outcome)
        self.assertEqual([p['page'] for p in outcome['pages']], [1, 2], outcome)
        self.assertTrue(all(p['status'] == 'ok' for p in outcome['pages']), outcome)
        self.assertTrue(all(p['diagnostics']['response_product_count'] == 30 for p in outcome['pages']), outcome)
        selected = {page: set() for page in (1, 2)}
        physical = self.metadata[category_id]
        expected = [physical[key] for key in ('physicsCate1', 'physicsCate2', 'physicsCate3') if physical[key]]
        for record in self.raw:
            for observation in record['payload']['listings']:
                if observation['category']['category_id'] != category_id:
                    continue
                selected[observation['page']].add(record['product_id'])
                actual = [str(code) for code in observation['api_product']['category']['codes']]
                self.assertEqual(actual[:len(expected)], expected, (category_id, record['product_id'], actual))
        self.assertTrue(selected[1], outcome)
        self.assertTrue(selected[2] - selected[1], outcome)
        requests = [r for r in self.report['requests'] if r['method'] == 'POST' and r['url'].endswith('cate=' + category_id)]
        self.assertEqual(len(requests), 2)
        self.assertTrue(all(r['status_code'] == 200 and r['error'] is None for r in requests))
        first, second = (copy.deepcopy(r['payload']) for r in requests)
        self.assertEqual((first[0].pop('page'), second[0].pop('page')), (1, 2))
        self.assertEqual(first, second)  # only page changes within one category

    def test_bed_pages_1_and_2(self):
        self.check_category('15241194')

    def test_desk_pages_1_and_2(self):
        self.check_category('15240504')

    def test_chair_pages_1_and_2(self):
        self.check_category('1523647')

    def test_dining_table_pages_1_and_2(self):
        self.check_category('15241240')

    def test_shelf_pages_1_and_2(self):
        self.check_category('15227279')

    def test_overall_budget_identity_and_storage(self):
        self.assertEqual(self.report['result'], 'completed', self.report['errors'])
        self.assertLessEqual(self.report['request_count'], 15)
        self.assertEqual(sum(r['method'] == 'POST' for r in self.report['requests']), 10)
        self.assertEqual(len({(r['source'], r['product_id']) for r in self.raw}), len(self.raw))
        self.assertTrue(all(r['detail_status'] == 'disabled' for r in self.raw))
        self.assertTrue(all(r['payload_hash'] == n['raw_payload_hash'] for r, n in zip(self.raw, self.normal)))
        self.assertTrue(all((self.output / name).is_file() for name in ('product_raw.jsonl', 'products.jsonl', 'run_report.json')))


if __name__ == '__main__':
    unittest.main()
