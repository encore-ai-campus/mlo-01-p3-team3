"""Unlimited defaults terminate on real empty/repeated pages; no HTTP."""
import tempfile
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from product_collector.core import Category, RequestClient, collect, load_categories, parse_limit
from product_collector.danawa import DanawaAdapter
from product_collector.pagination import PaginationTemplates
from test_pagination_gallery import PagingSession, response, templates
from test_product_collector import FakeSession, listing, detail


class UnlimitedCollectionTests(unittest.TestCase):
    def test_unlimited_products_pages_and_requests_stop_on_empty_page(self):
        adapter = DanawaAdapter(templates())
        url = adapter.category_url('1523647')
        session = PagingSession({(url, 1): response('11', '12'), (url, 2): response('13'), (url, 3): response()})
        raw, _, report = collect(adapter, RequestClient(None, interval=0, session=session),
                                 [Category('1523647', limit=None, max_pages=None)], total_limit=None,
                                 collect_details=False)
        self.assertEqual([r['product_id'] for r in raw], ['11', '12', '13'])
        self.assertEqual((report['request_count'], report['categories'][0]['reason']), (3, 'empty_page'))
        self.assertIsNone(report['limits']['total_products'])
        self.assertIsNone(report['limits']['requests'])

    def test_unlimited_pages_stop_on_repeated_products(self):
        adapter = DanawaAdapter(templates())
        url = adapter.category_url('1523647')
        session = PagingSession({(url, 1): response('11'), (url, 2): response('11')})
        raw, _, report = collect(adapter, RequestClient(None, interval=0, session=session),
                                 [Category('1523647', limit=None, max_pages=None)], total_limit=None,
                                 collect_details=False)
        self.assertEqual(len(raw), 1)
        self.assertEqual(report['categories'][0]['reason'], 'repeated_products')

    def test_unlimited_requests_stop_after_server_block(self):
        for status in (403, 429):
            with self.subTest(status=status):
                adapter = DanawaAdapter()
                session = FakeSession({adapter.category_url('1'): (status, 'blocked')})
                client = RequestClient(None, interval=0, session=session)
                _, _, report = collect(adapter, client, [Category('1'), Category('2')], total_limit=None)
                self.assertEqual(len(session.calls), 1)
                self.assertEqual(report['stop_reason'], f'http_{status}')

    def test_progress_is_emitted_before_network_wait_and_after_parsing(self):
        events = []
        adapter = DanawaAdapter()
        case = self
        class ObservedSession(FakeSession):
            def get(self, url, **kwargs):
                case.assertEqual(events[-1]['stage'], 'request')
                case.assertEqual(events[-1]['url'], url)
                return super().get(url, **kwargs)
        session = ObservedSession({adapter.category_url('1'): listing('11'),
                                   'https://prod.danawa.com/info/?pcode=11': detail('11')})
        _, _, report = collect(adapter, RequestClient(None, interval=0, session=session, progress=events.append),
                                 [Category('1')], total_limit=None, progress=events.append)
        self.assertEqual([event['stage'] for event in events],
                         ['category', 'request', 'page', 'details_start', 'request', 'detail'])
        self.assertEqual(report['request_count'], 2)

    def test_only_listing_requests_use_zero_interval(self):
        adapter = DanawaAdapter(templates())
        url = adapter.category_url('1523647')
        session = PagingSession({(url, 1): response('11'), (url, 2): response(),
                                'https://prod.danawa.com/info/?pcode=11': detail('11')})
        observed, sleeps = [], []
        clock = iter([0.0, 0.0, 0.0, 0.0, 0.0])
        client = RequestClient(None, interval=1, session=session, clock=lambda: next(clock), sleep=sleeps.append)
        def progress(event):
            if event['stage'] == 'request':
                observed.append(client.interval)
        client.progress = progress
        _, _, report = collect(adapter, client, [Category('1523647', limit=None, max_pages=None)],
                                 total_limit=None, listing_interval=0)
        self.assertEqual(observed, [0, 0, 1])
        self.assertEqual(sleeps, [0.0, 1.0])
        self.assertEqual(client.interval, 1)
        self.assertEqual(report['limits']['listing_interval_seconds'], 0)
        self.assertEqual(report['limits']['interval_seconds'], 1)

    def test_default_categories_are_unlimited_and_have_templates(self):
        root = Path(__file__).resolve().parent.parent
        categories = load_categories(root / 'config/danawa_categories_71.json')
        self.assertEqual(len(categories), 71)
        self.assertTrue(all(c.limit is None and c.max_pages is None and c.start_page == 1 for c in categories))
        DanawaAdapter(PaginationTemplates.load(root / 'config/danawa_pagination_5.json'), discover_categories=True).validate_categories(categories)

    def test_all_71_categories_discover_their_own_payload_and_terminate(self):
        root = Path(__file__).resolve().parent.parent
        categories = load_categories(root / 'config/danawa_categories_71.json')
        seed = templates()
        seed.categories['0'] = seed.categories.pop('1523647')  # common arguments only
        adapter = DanawaAdapter(seed, discover_categories=True)
        pages, expected = {}, {}
        for index, category in enumerate(categories):
            url = adapter.category_url(category.category_id)
            model = {'currentCategory': {'code': category.category_id, 'name': category.category_name, 'group': 15, 'depth': 2},
                     'uiCategoryHierarchyCodes': ['477', str(700000 + index)],
                     'physicsCategory': {'physicsCate1': '1826', 'physicsCate2': str(800000 + index),
                                         'physicsCate3': str(900000 + index), 'physicsCate4': ''}, 'multiCategoryYN': 'N'}
            text = 'x:' + json.dumps({'viewModel': model})
            pages[url] = '<script>self.__next_f.push(' + json.dumps([1, text]) + ')</script>'
            pages[(url, 1)] = response(str(100000 + index))
            pages[(url, 2)] = response()
            expected[url] = model
        session = PagingSession(pages)
        raw, _, report = collect(adapter, RequestClient(None, interval=0, session=session), categories,
                                 total_limit=None, collect_details=False)
        self.assertEqual((len(raw), len(report['categories']), report['request_count']), (71, 71, 213))
        self.assertTrue(all(c['reason'] == 'empty_page' for c in report['categories']))
        self.assertEqual(report['result'], 'completed')
        for url, kwargs in session.calls:
            if 'data' in kwargs:
                payload = json.loads(kwargs['data'])[0]
                self.assertEqual(payload['categoryCode'], expected[url]['uiCategoryHierarchyCodes'][-1])
                for key, value in expected[url]['physicsCategory'].items():
                    self.assertEqual(payload[key], value)
                self.assertEqual(kwargs['headers']['Referer'], url)

    def test_missing_category_metadata_never_posts_with_foreign_payload(self):
        seed = templates()
        adapter = DanawaAdapter(seed, discover_categories=True)
        url = adapter.category_url('15299999')
        session = PagingSession({url: '<html>no category metadata</html>'})
        raw, _, report = collect(adapter, RequestClient(None, interval=0, session=session),
                                 [Category('15299999', limit=None, max_pages=None)],
                                 total_limit=None, collect_details=False)
        self.assertEqual(raw, [])
        self.assertEqual(len(session.calls), 1)
        self.assertNotIn('data', session.calls[0][1])
        self.assertEqual(report['categories'][0]['status'], 'parse_failed')

    def test_limit_arguments_support_zero_and_reject_negative(self):
        self.assertIsNone(parse_limit('0'))
        self.assertEqual(parse_limit('5'), 5)
        for value in ('-1', 'bad'):
            with self.assertRaises(ValueError):
                parse_limit(value)

    def test_cli_defaults_and_zero_page_override_without_network(self):
        from product_collector.__main__ import main
        report = {'result': 'completed', 'request_count': 0}
        with tempfile.TemporaryDirectory() as temporary:
            with patch('sys.argv', ['product_collector', '--max-pages', '0', '--output', temporary]), \
                 patch('product_collector.__main__.PaginationTemplates.load', return_value=templates()), \
                 patch('product_collector.__main__.DanawaAdapter.validate_categories'), \
                 patch('product_collector.__main__.collect', return_value=([], [], report)) as collected, \
                 patch('product_collector.__main__.save_run', return_value=Path(temporary)), \
                 patch('requests.Session.request', side_effect=AssertionError('network forbidden')):
                self.assertEqual(main(), 0)
            args, kwargs = collected.call_args
            self.assertIsNone(args[1].max_requests)
            self.assertIsNone(args[3])
            self.assertEqual(len(args[2]), 71)
            self.assertTrue(all(c.max_pages is None for c in args[2]))
            self.assertTrue(kwargs['collect_details'])
            self.assertEqual(kwargs['listing_interval'], 0)


if __name__ == '__main__':
    unittest.main()
