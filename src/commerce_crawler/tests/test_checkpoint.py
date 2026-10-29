"""Disk durability, live export and resume tests; all responses are synthetic."""
import gzip
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
from product_collector.storage import detail_html_path, load_detail_html, read_jsonl, save_detail_html
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

    def test_detail_html_is_kept_beside_the_run_and_reprocess_reads_it(self):
        from product_collector.reprocess import reprocess_run
        adapter, categories = DanawaAdapter(), [Category('1')]
        pages = {adapter.category_url('1'): listing('11', '12'),
                 'https://prod.danawa.com/info/?pcode=11': gallery('11', 2)[0],
                 'https://prod.danawa.com/info/?pcode=12': gallery('12', 1)[0]}
        with tempfile.TemporaryDirectory() as root:
            with open_collection(Path(root), adapter, {}) as store:
                raw, normalized, _ = collect(adapter, RequestClient(None, interval=0, session=FakeSession(pages)),
                                             categories, total_limit=None, checkpoint=store)
                store.export()
                directory = store.directory
            for record in raw:
                code, detail = record['product_id'], record['payload']['detail']
                path = detail_html_path(directory, code, detail['collected_at'])
                saved = json.loads(gzip.decompress(path.read_bytes()).decode('utf-8'))
                self.assertEqual(saved['html'], pages[f'https://prod.danawa.com/info/?pcode={code}'])
                self.assertEqual(saved['collected_at'], detail['collected_at'])
                self.assertNotIn(saved['html'], json.dumps(record, ensure_ascii=False))
                self.assertEqual(record['extractor_version'], '0.4')
            snapshot = export_checkpoint(directory)
            for run in (directory, snapshot):
                with patch('requests.Session.get', side_effect=AssertionError('network forbidden')):
                    reparsed, renormalized, _ = reprocess_run(run)
                self.assertEqual([r['payload_hash'] for r in reparsed], [r['payload_hash'] for r in raw])
                self.assertEqual([len(n['images']) for n in renormalized], [len(n['images']) for n in normalized])
            # Gallery enrichment of the exported snapshot follows its evidence pointer instead of refetching.
            from product_collector.gallery_assets import enrich_saved_run
            with patch('requests.Session.request', side_effect=AssertionError('network forbidden')):
                report = enrich_saved_run(snapshot, Path(root) / 'gallery', RequestClient(None, interval=0))
            self.assertEqual((report['detail_cached'], report['detail_requests'], report['with_gallery']), (2, 0, 2))
            from product_collector.audit_assets import audit_saved_assets
            audit = audit_saved_assets(snapshot, Path(root) / 'audit')
            self.assertEqual((audit['missing_details'], audit['products_with_gallery'], audit['saved_evidence_result']),
                             ([], 2, 'passed'))
            # A truncated page is that product's error; the audit still checks the rest and writes its report.
            first = raw[0]
            path = detail_html_path(directory, first['product_id'], first['payload']['detail']['collected_at'])
            path.write_bytes(path.read_bytes()[:20])
            audit = audit_saved_assets(snapshot, Path(root) / 'audit-damaged')
            self.assertEqual([e['product_id'] for e in audit['errors']], [first['product_id']])
            self.assertEqual((audit['products_with_gallery'], audit['saved_evidence_result']), (1, 'failed'))
            self.assertTrue((Path(root) / 'audit-damaged' / 'audit' / 'verification_report.json').is_file())

    def test_retried_detail_keeps_the_page_an_earlier_snapshot_used(self):
        url = 'https://prod.danawa.com/info/?pcode=11'
        with tempfile.TemporaryDirectory() as root:
            save_detail_html(Path(root), '11', url, '<p>first</p>', '2026-10-29T00:00:00Z')
            save_detail_html(Path(root), '11', url, '<p>retry</p>', '2026-10-29T00:05:00Z')
            self.assertEqual(load_detail_html([root], '11', url, '2026-10-29T00:00:00Z')['html'], '<p>first</p>')
            self.assertEqual(load_detail_html([root], '11', url, '2026-10-29T00:05:00Z')['html'], '<p>retry</p>')
            self.assertEqual(load_detail_html([root], '11', url)['html'], '<p>retry</p>')
            self.assertIsNone(load_detail_html([root], '11', url, '2026-10-29T00:09:00Z'))
            with self.assertRaisesRegex(ValueError, 'identity mismatch'):
                load_detail_html([root], '11', 'https://prod.danawa.com/info/?pcode=12', '2026-10-29T00:00:00Z')
            path = detail_html_path(Path(root), '11', '2026-10-29T00:00:00Z')
            intact = path.read_bytes()
            for damaged in (b'not gzip', intact[:20]):  # BadGzipFile, then EOFError
                path.write_bytes(damaged)
                with self.assertRaisesRegex(ValueError, 'unreadable'):
                    load_detail_html([root], '11', url, '2026-10-29T00:00:00Z')


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
                self.assertEqual(store.keys(), [])
                self.assertEqual(store.current_report()['categories'][0]['pages'], [])
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
                self.assertEqual(len(store), 1)
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
