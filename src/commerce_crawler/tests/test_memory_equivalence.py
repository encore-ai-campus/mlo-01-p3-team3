"""Characterization: whole CLI runs keep their output bytes; no network, fixed clock and ids.

The expected hashes were recorded with the collector that kept every record in memory
(before logic 5). Set UPDATE_MEMORY_BASELINE=1 to record them again after an intended change.
"""
import hashlib
import itertools
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

import requests

from product_collector.__main__ import main
from product_collector.checkpoint import encoded
from product_collector.danawa import DanawaAdapter
from product_collector.image_store import object_key
from product_collector.media_resume import download_history
from test_gallery_assets import gallery
from test_media import png
from test_pagination_gallery import product
from test_product_collector import detail

BASELINE = Path(__file__).with_name('fixtures') / 'memory_baseline.json'
NOW = '2026-10-29T00:00:00+00:00'
CATEGORIES = ('1523647', '1523648')
# Report fields that depend on file system timing rather than on the collected data.
VOLATILE = {'validation_full_hashes', 'validation_stat_reuses'}
COLORS = ('red', 'blue', 'green', 'white', 'black', 'yellow', 'gray', 'purple')


def api(code, **changes):
    value = product(code)
    value.update(image={'url': f'https://img.example/{code}/thumb.jpg?_v=1'}, price={'min': 1000})
    value.update(changes)
    return value


def listing_page(category, products):
    return f'0:{{"a":"$@1","q":"?cate={category}"}}\n1:' + json.dumps(
        {'success': True, 'data': {'products': products}}, ensure_ascii=False)


def page(code):
    return f'https://prod.danawa.com/info/?pcode={code}'


def category_url(code):
    return DanawaAdapter().category_url(code)


def images(codes, count=2):
    """Gallery pages and distinct image bytes for each product."""
    pages, files = {}, {}
    for index, code in enumerate(codes):
        html, urls = gallery(code, count)
        pages[page(code)] = html
        for position, url in enumerate(urls):
            files[url] = png(COLORS[(index + position) % len(COLORS)])
    return pages, files


class Session:
    """requests.Session stand-in for listings (POST), details and images (GET)."""

    def __init__(self, posts, gets, stop=None):
        self.posts, self.gets, self.stop = posts, gets, stop
        self.headers, self.calls = {}, []

    def mount(self, *_):
        pass

    def respond(self, url, value):
        if isinstance(value, Exception):
            raise value
        response = requests.Response()
        status, body = value if isinstance(value, tuple) else (200, value)
        response.status_code = status
        response._content = body if isinstance(body, bytes) else body.encode('utf-8')
        response._content_consumed = True
        response.encoding = 'utf-8'
        response.url = url
        return response

    def post(self, url, data=None, **_):
        number = json.loads(data)[0]['page']
        self.calls.append(('POST', url, number))
        if self.stop == ('POST', url, number):
            raise KeyboardInterrupt()
        return self.respond(url, self.posts.get((url, number), listing_page(url.split('cate=')[1], [])))

    def get(self, url, **_):
        self.calls.append(('GET', url))
        if self.stop == ('GET', url):
            raise KeyboardInterrupt()
        return self.respond(url, self.gets[url])

    def close(self):
        pass


class Store:
    def __init__(self, objects):
        self.objects = objects

    def head(self, key):
        return self.objects.get(key)

    def close(self):
        pass


def uploaded(run):
    """What the pipeline would hold after loading this run's downloads."""
    return {object_key(e): {'bytes': e['bytes'], 'sha256': e['sha256']}
            for e in download_history(run / 'media' / 'image_downloads.jsonl').values()
            if e.get('status') == 'downloaded'}


class Harness:
    def __init__(self, root: Path):
        self.root = root
        self.ids = itertools.count(1)
        self.media_ids = itertools.count(1)
        template = {'payload': [{'categoryCode': '3647', 'page': 1, 'listCount': 30, 'physicsCate1': '1826',
                                 'physicsCate2': '32303', 'physicsCate3': '32346', 'searchMaker': '$undefined',
                                 'sortMethod': 'BEST'}]}
        self.templates = root / 'templates.json'
        self.templates.write_text(json.dumps({'headers': {'Next-Action': 'TEST_ONLY_ACTION'},
                                              'categories': {c: template for c in CATEGORIES}}), encoding='utf-8')
        self.categories = root / 'categories.json'
        self.categories.write_text(json.dumps([{'category_id': c, 'limit': None, 'max_pages': None}
                                               for c in CATEGORIES]), encoding='utf-8')
        self.store_config = root / 'store.properties'
        self.store_config.write_text('endpoint=https://store.example\nbucket=pj3-data\naccessKey=a\nsecretKey=b\n',
                                     encoding='utf-8')

    def run(self, output, listings, gets, *options, stop=None, objects=None):
        posts = {(category_url(c), 1): listing_page(c, products) for c, products in listings.items()}
        session = Session(posts, gets, stop)
        argv = ['product_collector', '--categories', str(self.categories), '--output', str(self.root / output),
                '--interval', '0', '--workers', '1', '--pagination-template', str(self.templates),
                '--download-images', *options]
        if objects is not None:
            argv += ['--image-store-config', str(self.store_config)]
        with patch('sys.argv', argv), patch('requests.Session', return_value=session), \
                patch('product_collector.core.utc_now', return_value=NOW), \
                patch('product_collector.media.utc_now', return_value=NOW), \
                patch('product_collector.validation.utc_now', return_value=NOW), \
                patch('product_collector.checkpoint.uuid4', side_effect=lambda: uuid.UUID(int=next(self.ids))), \
                patch('product_collector.media.uuid4', side_effect=lambda: uuid.UUID(int=10 ** 6 + next(self.media_ids))), \
                patch('product_collector.image_store.S3ImageStore', side_effect=lambda config: Store(objects or {})), \
                patch.dict(sys.modules, {'boto3': types.ModuleType('boto3')}), \
                patch('builtins.print'):
            code = main()
        return code, session

    def reprocess(self, run, output):
        argv = ['product_collector', '--reprocess-from', str(run), '--output', str(self.root / output)]
        with patch('sys.argv', argv), patch('requests.Session', side_effect=AssertionError('network forbidden')), \
                patch('product_collector.core.utc_now', return_value=NOW), \
                patch('product_collector.reprocess.utc_now', return_value=NOW), \
                patch('product_collector.reprocess.uuid4', side_effect=lambda: uuid.UUID(int=next(self.ids))), \
                patch('builtins.print'):
            return main()

    def runs(self, output):
        return sorted(p for p in (self.root / output).iterdir() if (p / 'run_config.json').is_file())


def fingerprint(root: Path, output: str) -> dict:
    """Hashes of every output of the runs under output, with paths and line endings made portable."""
    base = root / output
    names = {}
    for index, run in enumerate(sorted(p for p in base.iterdir() if (p / 'run_config.json').is_file())):
        names[run.name] = f'run{index}'
    replacements = []
    for path in (base.resolve(), base, root.resolve(), root):
        for form in (str(path), json.dumps(str(path))[1:-1]):
            replacements.append(form)
    result = {}
    for path in sorted(base.rglob('*')):
        relative = path.relative_to(base).as_posix()
        if (not path.is_file() or '/details/' in f'/{relative}' or relative.endswith(('.lock', '.tmp'))
                or path.name in {'image_validation.jsonl', 'active_run.json'} or '/media/images/' in f'/{relative}'
                or '/categories/' in f'/{relative}' or path.name == 'run_config.json'):
            continue
        text = path.read_bytes().decode('utf-8').replace('\r\n', '\n')
        if path.name == 'checkpoint_events.jsonl':
            # Each envelope's sha256 covers temporary paths; check it here and compare the events.
            events = []
            for line in text.split('\n'):
                if line.strip():
                    envelope = json.loads(line)
                    assert hashlib.sha256(encoded(envelope['event']).encode()).hexdigest() == envelope['sha256']
                    events.append(encoded(envelope['event']))
            text = '\n'.join(events)
        for form in sorted(set(replacements), key=len, reverse=True):
            text = text.replace(form, '<ROOT>')
        for name, label in names.items():
            text = text.replace(name, label)
            relative = relative.replace(name, label)
        text = text.replace('\\\\', '/')  # a JSON-escaped Windows path separator
        if path.suffix == '.json':
            value = json.loads(text)
            for key in VOLATILE & set(value if isinstance(value, dict) else ()):
                value.pop(key)
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        result[relative] = hashlib.sha256(text.encode('utf-8')).hexdigest()
    return result


FIRST = {CATEGORIES[0]: [api('11'), api('12'), api('13')], CATEGORIES[1]: [api('12'), api('14')]}


def first_pages():
    details, files = images(('11', '12', '13', '14'))
    return {**details, **files}


def scenarios(root: Path) -> dict:
    harness = Harness(root)
    results = {}

    # 1. Complete run, then a changed-only run that reuses details and finds images in storage.
    code, _ = harness.run('reuse', FIRST, first_pages())
    assert code == 0, code
    first = harness.runs('reuse')[0]
    renamed = {CATEGORIES[0]: [api('11'), api('12', name={'full': '바뀐 이름', 'maker': '제조사'}), api('13')],
               CATEGORIES[1]: [api('12', name={'full': '바뀐 이름', 'maker': '제조사'}), api('14'), api('15')]}
    details, files = images(('12', '15'), 3)
    code, session = harness.run('reuse', renamed, {**first_pages(), **details, **files}, '--new-run',
                                '--detail-refresh', 'changed', '--refresh-sample', '0', objects=uploaded(first))
    assert code == 0, code
    results['reuse'] = (fingerprint(root, 'reuse'), [c[1] for c in session.calls if c[0] == 'GET'])

    # 2. Failed detail, another product's page, a missing image and a listing-only product.
    gets = first_pages()
    gets[page('12')] = (500, 'error')
    gets[page('13')] = detail('99')
    gets['https://img.example/14/1.png'] = (404, b'missing')
    code, _ = harness.run('failures', FIRST, gets)
    assert code == 2, code
    results['failures'] = (fingerprint(root, 'failures'), None)

    # 3. Offline re-extraction of saved runs: one with reused details, one with failures.
    for name, source in (('reprocess_reuse', harness.runs('reuse')[1]),
                         ('reprocess_failures', harness.runs('failures')[0])):
        code = harness.reprocess(source, name)
        assert code in (0, 2), (name, code)
        results[name] = (fingerprint(root, name), None)

    # 4. Interrupted while listing, while fetching details and while downloading images; then resumed.
    for name, stop in (('resume_listing', ('POST', category_url(CATEGORIES[1]), 1)),
                       ('resume_detail', ('GET', page('13'))),
                       ('resume_images', ('GET', 'https://img.example/12/1.png'))):
        code, _ = harness.run(name, FIRST, first_pages(), stop=stop)
        assert code == 130, (name, code)
        code, _ = harness.run(name, FIRST, first_pages())
        assert code == 0, (name, code)
        results[name] = (fingerprint(root, name), None)
    return results


class MemoryEquivalenceTests(unittest.TestCase):
    def test_outputs_match_the_recorded_collector(self):
        with tempfile.TemporaryDirectory() as directory:
            results = scenarios(Path(directory))
        recorded = {name: {'files': files, 'gets': gets} for name, (files, gets) in results.items()}
        if os.environ.get('UPDATE_MEMORY_BASELINE'):
            BASELINE.parent.mkdir(exist_ok=True)
            BASELINE.write_text(json.dumps(recorded, indent=1, sort_keys=True) + '\n', encoding='utf-8')
        expected = json.loads(BASELINE.read_text(encoding='utf-8'))
        for name in expected:
            with self.subTest(scenario=name):
                self.assertEqual(sorted(recorded[name]['files']), sorted(expected[name]['files']))
                for path, digest in expected[name]['files'].items():
                    self.assertEqual(recorded[name]['files'][path], digest, path)
                self.assertEqual(recorded[name]['gets'], expected[name]['gets'])


if __name__ == '__main__':
    unittest.main()
