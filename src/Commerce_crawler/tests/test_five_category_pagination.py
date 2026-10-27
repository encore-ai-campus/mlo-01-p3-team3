"""Network-free regression tests using retained samples of five real categories."""
import copy
import json
import unittest
from pathlib import Path

from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.pagination import PaginationTemplates, pagination_metadata
from test_pagination_gallery import PagingSession

ROOT = Path(__file__).resolve().parent.parent


class FiveCategoryPaginationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads((ROOT / 'tests/fixtures/five_category_pagination.json').read_text(encoding='utf-8'))
        cls.config = json.loads((ROOT / 'config/danawa_pagination_5.json').read_text(encoding='utf-8'))

    def test_five_real_category_samples_use_ten_post_requests(self):
        pages, categories, expected = {}, [], {}
        for category in self.fixture['categories']:
            code = category['category_id']
            categories.append(Category(code, limit=4, max_pages=2))
            expected[code] = {str(p['productCode']) for page in category['pages'] for p in page['products']}
            for page in category['pages']:
                pages[(DanawaAdapter().category_url(code), page['page'])] = '1:' + json.dumps(
                    {'success': True, 'data': {'products': page['products']}}, ensure_ascii=False)
        session = PagingSession(pages)
        raw, normalized, report = collect(DanawaAdapter(PaginationTemplates(self.config)),
                                          RequestClient(max_requests=10, interval=0, session=session),
                                          categories, total_limit=20, collect_details=False)
        self.assertEqual(len(raw), 20)
        self.assertEqual(report['request_count'], 10)
        self.assertEqual(report['result'], 'completed')
        self.assertEqual(len(normalized), len(raw))
        for category in categories:
            with self.subTest(category=category.category_id):
                actual = {r['product_id'] for r in raw if any(c['category_id'] == category.category_id for c in r['categories'])}
                self.assertEqual(actual, expected[category.category_id])
                outcome = next(c for c in report['categories'] if c['category_id'] == category.category_id)
                self.assertEqual([p['page'] for p in outcome['pages']], [1, 2])
                calls = [kw for url, kw in session.calls if url.endswith('cate=' + category.category_id)]
                self.assertEqual([json.loads(kw['data'])[0]['page'] for kw in calls], [1, 2])

    def test_all_five_templates_preserve_correct_physical_category(self):
        templates = PaginationTemplates(self.config)
        self.assertEqual(len(templates.categories), 5)
        for category in self.fixture['categories']:
            with self.subTest(category=category['category_id']):
                code, meta = category['category_id'], category['metadata']
                for page in (1, 2):
                    payload, _ = templates.request(code, page)
                    self.assertEqual(payload[0]['page'], page)
                    self.assertEqual(payload[0]['categoryCode'], meta['categoryCode'])
                    for key in ('physicsCate1', 'physicsCate2', 'physicsCate3', 'physicsCate4'):
                        self.assertEqual(payload[0][key], meta[key])
                    self.assertEqual(payload[0]['searchMaker'], '$undefined')

    def make_html(self, public_id='15299999', missing=False):
        model = {'currentCategory': {'code': public_id, 'name': '테스트', 'group': 15, 'depth': 2},
                 'uiCategoryHierarchyCodes': ['477', '41194'],
                 'physicsCategory': {'physicsCate1': '1826', 'physicsCate2': '11241',
                                     'physicsCate3': '11242', 'physicsCate4': ''}, 'multiCategoryYN': 'N'}
        if missing:
            del model['physicsCategory']['physicsCate3']
        text = 'x:' + json.dumps({'viewModel': model}, ensure_ascii=False)
        # Real pages may split metadata between script chunks.
        cut = len(text) // 2
        html = ''.join('<script>self.__next_f.push(' + json.dumps([1, chunk], ensure_ascii=False) + ')</script>'
                       for chunk in (text[:cut], text[cut:]))
        return html

    def test_discovery_joins_chunks_and_does_not_slice_url_category(self):
        meta = pagination_metadata(self.make_html(), '15299999')
        self.assertEqual(meta['categoryCode'], '41194')
        self.assertNotEqual(meta['categoryCode'], '99999')
        self.assertEqual(meta['physicsCate3'], '11242')

    def test_discovery_rejects_mismatched_or_incomplete_metadata(self):
        for html, code in ((self.make_html(), '1523647'), (self.make_html(missing=True), '15299999'), ('<html/>', '15299999')):
            with self.subTest(category=code), self.assertRaises(ValueError):
                pagination_metadata(html, code)


if __name__ == '__main__':
    unittest.main()
