"""Disk durability, live export and resume tests; all responses are synthetic."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from product_collector.checkpoint import open_collection, export_checkpoint
from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.storage import read_jsonl
from test_pagination_gallery import PagingSession, response, templates
from test_product_collector import FakeSession, listing
from test_gallery_assets import gallery


class CheckpointTests(unittest.TestCase):
    def test_page_is_exportable_before_completion_and_resume_skips_saved_page(self):
        class Interrupted(PagingSession):
            def post(self, url, **kwargs):
                if json.loads(kwargs['data'])[0]['page'] == 2:
                    raise KeyboardInterrupt()
                return super().post(url, **kwargs)
        adapter = DanawaAdapter(templates())
        url = adapter.category_url('1523647')
        category = Category('1523647', limit=None, max_pages=None)
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                directory = store.directory
                client = RequestClient(None, interval=0, session=Interrupted({(url, 1): response('11', '12')}))
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter, client, [category], total_limit=None, collect_details=False, checkpoint=store)
                snapshot = export_checkpoint(directory)  # reader while writer is still open
                products = read_jsonl(snapshot / 'products.jsonl')
                self.assertEqual([p['product_id'] for p in products], ['11', '12'])
                self.assertEqual(products[0]['categories'][0]['category_id'], '1523647')
            with open_collection(Path(root), DanawaAdapter(templates()), {}) as store:
                self.assertEqual(store.directory, directory)
                session = PagingSession({(url, 2): response('12', '13'), (url, 3): response()})
                raw, normalized, report = collect(DanawaAdapter(templates()), RequestClient(None, interval=0, session=session),
                    [category], total_limit=None, collect_details=False, checkpoint=store)
                self.assertEqual([json.loads(kw['data'])[0]['page'] for _, kw in session.calls], [2, 3])
                self.assertEqual([r['product_id'] for r in raw], ['11', '12', '13'])
                self.assertEqual(report['request_count'], 3)
                self.assertTrue(all(n['raw_payload_hash'] == r['payload_hash'] for r, n in zip(raw, normalized)))

    def test_completed_detail_and_categories_are_not_requested_again(self):
        class Interrupted(FakeSession):
            def get(self, url, **kwargs):
                if url.endswith('pcode=12'):
                    raise KeyboardInterrupt()
                return super().get(url, **kwargs)
        adapter, categories = DanawaAdapter(), [Category('1')]
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                client = RequestClient(None, interval=0, session=Interrupted({adapter.category_url('1'): listing('11', '12'),
                    'https://prod.danawa.com/info/?pcode=11': gallery('11', 2)[0]}))
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter, client, categories, total_limit=None, checkpoint=store)
                snapshot = export_checkpoint(store.directory)
                self.assertEqual(len(read_jsonl(snapshot / 'products.jsonl')[0]['images']), 2)
            with open_collection(Path(root), adapter, {}) as store:
                session = FakeSession({'https://prod.danawa.com/info/?pcode=12': gallery('12', 1)[0]})
                raw, _, _ = collect(adapter, RequestClient(None, interval=0, session=session), categories,
                                     total_limit=None, checkpoint=store)
                self.assertEqual(len(session.calls), 1)
                self.assertTrue(session.calls[0][0].endswith('pcode=12'))
                self.assertTrue(all(r['detail_status'] == 'ok' for r in raw))

    def test_half_page_transaction_rolls_back_without_advancing_cursor(self):
        class Interrupted(DanawaAdapter):
            def normalize(self, raw):
                if raw['product_id'] == '12':
                    raise KeyboardInterrupt()
                return super().normalize(raw)
        with tempfile.TemporaryDirectory() as root:
            adapter = Interrupted(templates())
            url = adapter.category_url('1523647')
            category = Category('1523647', limit=None, max_pages=None)
            with open_collection(Path(root), adapter, {}) as store:
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter, RequestClient(None, interval=0, session=PagingSession({(url, 1): response('11', '12')})),
                            [category], total_limit=None, collect_details=False, checkpoint=store)
                records, report = store.restore()
                self.assertEqual(records, {})
                self.assertEqual(report['categories'][0]['pages'], [])
            with open_collection(Path(root), DanawaAdapter(templates()), {}) as store:
                session = PagingSession({(url, 1): response('11', '12'), (url, 2): response()})
                raw, _, _ = collect(DanawaAdapter(templates()), RequestClient(None, interval=0, session=session),
                                    [category], total_limit=None, collect_details=False, checkpoint=store)
                self.assertEqual(len(raw), 2)
                self.assertEqual(json.loads(session.calls[0][1]['data'])[0]['page'], 1)

    def test_forced_process_exit_keeps_committed_page_and_releases_writer_lock(self):
        script = '''
import os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_pagination_gallery import PagingSession, response, templates
from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter
from product_collector.checkpoint import open_collection
class Crash(PagingSession):
    def post(self, url, **kwargs):
        if json.loads(kwargs['data'])[0]['page'] == 2:
            os._exit(23)
        return super().post(url, **kwargs)
adapter = DanawaAdapter(templates())
url = adapter.category_url('1523647')
with open_collection(Path(sys.argv[1]), adapter, {}) as store:
    collect(adapter, RequestClient(None, interval=0, session=Crash({(url, 1): response('11')})),
            [Category('1523647', limit=None, max_pages=None)], total_limit=None, collect_details=False, checkpoint=store)
'''
        with tempfile.TemporaryDirectory() as root:
            result = subprocess.run([sys.executable, '-c', script, root], cwd=Path(__file__).resolve().parent.parent, timeout=20)
            self.assertEqual(result.returncode, 23)
            adapter = DanawaAdapter(templates())
            url = adapter.category_url('1523647')
            with open_collection(Path(root), adapter, {}) as store:
                self.assertTrue((store.directory / 'checkpoint_events.jsonl').is_file())
                self.assertEqual(len(store.restore()[0]), 1)
                session = PagingSession({(url, 2): response()})
                raw, _, _ = collect(adapter, RequestClient(None, interval=0, session=session),
                    [Category('1523647', limit=None, max_pages=None)], total_limit=None, collect_details=False, checkpoint=store)
                self.assertEqual([r['product_id'] for r in raw], ['11'])
                self.assertEqual(len(session.calls), 1)

    def test_config_changes_and_concurrent_writer_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), DanawaAdapter(), {'a': 1}):
                with self.assertRaises((ValueError, BlockingIOError)):
                    with open_collection(Path(root), DanawaAdapter(), {'a': 1}):
                        self.fail('second writer entered')
            with self.assertRaises(ValueError):
                with open_collection(Path(root), DanawaAdapter(), {'a': 2}):
                    self.fail('configuration mismatch accepted')

    def test_completed_run_does_not_auto_resume(self):
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), DanawaAdapter(), {}) as store:
                previous = store.directory
                store.state('completed')
            self.assertFalse((Path(root) / 'active_run.json').exists())
            with open_collection(Path(root), DanawaAdapter(), {}) as store:
                self.assertNotEqual(store.directory, previous)

    def test_cli_interrupt_exports_and_next_execution_resumes_automatically(self):
        from product_collector.__main__ import main
        class Interrupted(FakeSession):
            def get(self, url, **kwargs):
                if url.endswith('pcode=12'):
                    raise KeyboardInterrupt()
                return super().get(url, **kwargs)
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            config = root / 'categories.json'
            config.write_text('[{"category_id":"1","limit":2,"max_pages":1}]')
            output = root / 'data'
            argv = ['product_collector', '--categories', str(config), '--output', str(output), '--interval', '0']
            session = Interrupted({'https://prod.danawa.com/list/?cate=1': listing('11', '12'),
                                   'https://prod.danawa.com/info/?pcode=11': gallery('11', 2)[0]})
            with patch('sys.argv', argv), patch('requests.Session', return_value=session), \
                 patch('product_collector.__main__.PaginationTemplates.load', return_value=None):
                self.assertEqual(main(), 130)
            directory = Path(json.loads((output / 'active_run.json').read_text())['directory'])
            state = json.loads((directory / 'checkpoint_state.json').read_text(encoding='utf-8'))
            self.assertEqual((state['status'], state['product_count']), ('interrupted', 2))
            self.assertEqual(len(read_jsonl(directory / 'products.jsonl')[0]['images']), 2)
            resumed = FakeSession({'https://prod.danawa.com/info/?pcode=12': gallery('12', 1)[0]})
            with patch('sys.argv', argv), patch('requests.Session', return_value=resumed), \
                 patch('product_collector.__main__.PaginationTemplates.load', return_value=None):
                self.assertEqual(main(), 0)
            self.assertEqual(len(resumed.calls), 1)
            self.assertFalse((output / 'active_run.json').exists())
            self.assertEqual(len(read_jsonl(directory / 'products.jsonl')), 2)

    def test_discovered_category_arguments_can_be_restored_without_another_get(self):
        adapter = DanawaAdapter(templates(), discover_categories=True)
        seed = adapter.pagination_templates.categories['1523647']['payload']
        payload = json.loads(json.dumps(seed))
        payload[0].update(categoryCode='700001', physicsCate2='800001')
        url = adapter.category_url('15299999')
        adapter.restore_pagination({'requests': [{'method': 'POST', 'url': url, 'payload': payload}]})
        session = PagingSession({(url, 2): response()})
        adapter.fetch_listing_page(RequestClient(None, interval=0, session=session),
                                   Category('15299999', limit=None, max_pages=None), 2)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(json.loads(session.calls[0][1]['data'])[0]['categoryCode'], '700001')


if __name__ == '__main__':
    unittest.main()
