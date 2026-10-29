"""Detail refresh "changed": reuse unchanged details of the previous completed run; no network."""
import copy
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from product_collector import quality
from product_collector.checkpoint import open_collection
from product_collector.core import Category, RequestClient, collect
from product_collector.danawa import DanawaAdapter, listing_fingerprint
from product_collector.detail_reuse import REUSE_FILE, DetailReuse, find_base
from product_collector.reprocess import reprocess_run
from product_collector.storage import detail_html_path
from test_gallery_assets import gallery
from test_pagination_gallery import PagingSession, product, templates
from test_product_collector import detail

CATEGORY = Category('1523647', limit=None, max_pages=None)
URL = DanawaAdapter(templates()).category_url('1523647')
CONFIG = {'source': 'danawa', 'collect_details': True}


def api(code, **changes):
    value = product(code)
    value.update(image={'url': f'https://img.example/{code}/thumb.jpg?_v=1'}, price={'min': 1000},
                 stats={'shopCount': 3}, popularRank=1)
    value.update(changes)
    return value


def listing_page(products):
    return '0:{"a":"$@1","q":"?cate=1523647"}\n1:' + json.dumps(
        {'success': True, 'data': {'products': products}}, ensure_ascii=False)


def page(code):
    return f'https://prod.danawa.com/info/?pcode={code}'


class Interrupted(PagingSession):
    def __init__(self, pages, stop):
        super().__init__(pages)
        self.stop = stop

    def get(self, url, **kwargs):
        if url == page(self.stop):
            raise KeyboardInterrupt()
        return super().get(url, **kwargs)


class DetailReuseTests(unittest.TestCase):
    def run_once(self, root, products, details, sample=None, resume_from=None, stop=None):
        """One collection as __main__ runs it; sample=None is the "all" mode."""
        adapter = DanawaAdapter(templates())
        config = dict(CONFIG)
        if sample is not None:
            config['detail_refresh'] = {'mode': 'changed', 'sample': sample, 'fingerprint_version': 1}
        pages = {(URL, 1): listing_page(products), (URL, 2): listing_page([]), **details}
        session = Interrupted(pages, stop) if stop else PagingSession(pages)
        client = RequestClient(None, interval=0, session=session)
        with open_collection(Path(root), adapter, config, resume_from, new_run=resume_from is None) as store:
            reuse = (DetailReuse.open(store.directory, Path(root), 'danawa', store.job_id, sample)
                     if sample is not None else None)
            raw, normalized, report = collect(adapter, client, [CATEGORY], total_limit=None, checkpoint=store,
                                              detail_reuse=reuse)
            quality.apply(report, normalized, 'warn', {str(r['product_id']) for r in raw if r.get('detail_reused_from')})
            store.save([], report, client.attempts)
            store.state(report['result'])
            store.export()
        requested = sorted(u.split('pcode=')[1] for u, _ in session.calls if '/info/' in u)
        return store.directory, {r['product_id']: r for r in raw}, report, requested

    def first(self, root, codes=('11', '12', '13', '14', '15'), plain=('13',)):
        details = {page(c): detail(c) if c in plain else gallery(c, 2)[0] for c in codes}
        return self.run_once(root, [api(c) for c in codes], details)

    def test_fingerprint_ignores_prices_and_ranks_but_sees_names_images_and_other_listings(self):
        base = [{'api_product': api('11')}]
        same = [{'api_product': api('11', price={'min': 900}, stats={'shopCount': 9}, popularRank=7)}]
        self.assertEqual(listing_fingerprint(base), listing_fingerprint(same))
        self.assertTrue(listing_fingerprint(base).startswith('v1:'))
        renamed = [{'api_product': api('11', name={'full': '새 이름', 'maker': '제조사'})}]
        reimaged = [{'api_product': api('11', image={'url': 'https://img.example/11/thumb.jpg?_v=2'})}]
        other = copy.deepcopy(base) + [{'api_product': api('11', description={'pc': {'raw': '바뀐 규격'}})}]
        for changed in (renamed, reimaged, other):
            self.assertNotEqual(listing_fingerprint(base), listing_fingerprint(changed))
        self.assertEqual(listing_fingerprint(base + base), listing_fingerprint(base))
        self.assertIsNone(listing_fingerprint([{'html': '<div></div>'}]))

    def test_unchanged_products_reuse_the_detail_and_the_run_keeps_its_own_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            first, before, _, _ = self.first(root)
            products = [api('11', price={'min': 500}, popularRank=9), api('12', name={'full': '바뀐 이름'}),
                        api('13'), api('14'), api('15'), api('16')]
            details = {page(c): gallery(c, 2)[0] for c in ('12', '13', '16')}
            second, raw, report, requested = self.run_once(root, products, details, sample=0)
            # Changed name, a base detail without gallery, and a new product; nothing else.
            self.assertEqual(requested, ['12', '13', '16'])
            reused = {c for c, r in raw.items() if r.get('detail_reused_from')}
            self.assertEqual(reused, {'11', '14', '15'})
            self.assertTrue(all(r['listing_fingerprint'] for r in raw.values()))
            at = before['11']['payload']['detail']['collected_at']
            self.assertEqual(raw['11']['payload']['detail'], before['11']['payload']['detail'])
            self.assertEqual(raw['11']['detail_reused_from']['collected_at'], at)
            self.assertTrue(os.path.samefile(detail_html_path(first, '11', at), detail_html_path(second, '11', at)))
            self.assertEqual((report['detail_reuse']['reused'], report['detail_failures']['requested']), (3, 3))
            self.assertEqual(report['quality']['reused_details'], 3)
            self.assertEqual(report['result'], 'completed')
            # The earlier run may be pruned: this run alone reprocesses every detail, reused ones included.
            shutil.rmtree(first)
            again, _, replay = reprocess_run(second)
            self.assertTrue(all(r['detail_status'] == 'ok' for r in again))
            self.assertEqual(replay['detail_failures']['requested'], 6)

    def test_a_damaged_or_missing_saved_page_requests_only_that_product(self):
        with tempfile.TemporaryDirectory() as root:
            first, before, _, _ = self.first(root)
            path = detail_html_path(first, '11', before['11']['payload']['detail']['collected_at'])
            data = bytearray(path.read_bytes())
            data[12:40] = bytes(28)  # valid gzip header, corrupt DEFLATE data (zlib.error)
            path.write_bytes(bytes(data))
            truncated = detail_html_path(first, '14', before['14']['payload']['detail']['collected_at'])
            truncated.write_bytes(truncated.read_bytes()[:20])
            missing = detail_html_path(first, '12', before['12']['payload']['detail']['collected_at'])
            missing.unlink()
            # A per-product legacy cache file is not evidence of the reused response.
            (first / 'details' / '12.json').write_text(json.dumps(
                {'product_id': '12', 'url': page('12'), 'html': gallery('12', 2)[0]}), encoding='utf-8')
            _, raw, _, requested = self.run_once(root, [api(c) for c in ('11', '12', '14')],
                                                 {page(c): gallery(c, 2)[0] for c in ('11', '12', '14')}, sample=0)
            self.assertEqual(requested, ['11', '12', '14'])
            self.assertIsNone(raw['11'].get('detail_reused_from'))

    def test_the_sample_is_requested_even_when_unchanged_and_is_stable_per_run(self):
        with tempfile.TemporaryDirectory() as root:
            first, *_ = self.first(root, plain=())
            reuse = DetailReuse(first, 'job-a', 2)
            draws = [reuse.sampled(str(c)) for c in range(1000)]
            self.assertEqual(draws, [reuse.sampled(str(c)) for c in range(1000)])
            self.assertTrue(200 < sum(draws) < 600)  # about 2/5 of a five-product base
            codes = ('11', '12', '13', '14', '15')
            _, _, report, requested = self.run_once(root, [api(c) for c in codes],
                                                    {page(c): gallery(c, 2)[0] for c in codes}, sample=5)
            self.assertEqual(requested, list(codes))  # a sample as large as the base asks for everything
            self.assertEqual((report['detail_reuse']['reused'], report['detail_reuse']['sampled']), (0, 5))

    def test_a_resumed_run_keeps_its_base_even_after_a_newer_run_completes(self):
        codes = ('11', '12', '13')
        with tempfile.TemporaryDirectory() as root:
            first, *_ = self.first(root, codes, plain=())
            products, details = [api(c) for c in codes], {page(c): gallery(c, 2)[0] for c in codes}
            changed = [api('11'), api('12', name={'full': '바뀐 이름'}), api('13', name={'full': '또 바뀜'})]
            with self.assertRaises(KeyboardInterrupt):
                self.run_once(root, changed, details, sample=0, stop='12')
            second = next(p for p in Path(root).iterdir() if p.is_dir() and (p / REUSE_FILE).is_file())
            third, *_ = self.run_once(root, products, details)  # a newer completed run with --new-run
            self.assertEqual(find_base(Path(root), second, 'danawa'), third)
            _, raw, report, requested = self.run_once(root, changed, details, sample=0, resume_from=second)
            self.assertEqual(report['detail_reuse']['base_job_id'], first.name)
            self.assertEqual(requested, ['12', '13'])
            self.assertEqual(raw['11']['detail_reused_from']['job_id'], first.name)

    def test_a_base_pruned_during_an_interruption_requests_the_rest(self):
        codes = ('11', '12', '13')
        with tempfile.TemporaryDirectory() as root:
            first, *_ = self.first(root, codes, plain=())
            products, details = [api(c) for c in codes], {page(c): gallery(c, 2)[0] for c in codes}
            with self.assertRaises(KeyboardInterrupt):
                self.run_once(root, [api('11'), api('12', name={'full': '바뀐 이름'}), api('13')], details,
                              sample=0, stop='12')
            second = next(p for p in Path(root).iterdir() if p.is_dir() and (p / REUSE_FILE).is_file())
            shutil.rmtree(first)
            _, raw, report, requested = self.run_once(root, [api('11'), api('12', name={'full': '바뀐 이름'}),
                                                             api('13')], details, sample=0, resume_from=second)
            self.assertEqual(requested, ['12', '13'])
            self.assertEqual(raw['11']['detail_reused_from']['job_id'], first.name)  # kept, with its own evidence
            self.assertEqual(report['detail_reuse']['base_missing'], str(first.resolve()))
            self.assertEqual(report['result'], 'completed')

    def test_base_is_the_newest_completed_detail_run_of_the_source(self):
        with tempfile.TemporaryDirectory() as root:
            def run(name, status, updated, **config):
                directory = Path(root) / name
                directory.mkdir()
                (directory / 'product_raw.jsonl').write_text('', encoding='utf-8')
                (directory / 'checkpoint_state.json').write_text(json.dumps({'status': status, 'updated_at': updated}))
                (directory / 'run_config.json').write_text(json.dumps({'config': {**CONFIG, **config}}))
                return directory
            older = run('a', 'completed', '2026-10-01T00:00:00Z')
            run('b', 'partial', '2026-10-09T00:00:00Z')
            run('c', 'completed', '2026-10-09T00:00:00Z', collect_details=False)
            run('d', 'completed', '2026-10-09T00:00:00Z', source='other')
            current = run('e', 'completed', '2026-10-10T00:00:00Z')
            self.assertEqual(find_base(Path(root), current, 'danawa'), older)
            self.assertIsNone(DetailReuse(None, 'job', 10).reuse({'product_id': '1'}, 'v1:x', None))


if __name__ == '__main__':
    unittest.main()
