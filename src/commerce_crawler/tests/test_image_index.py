"""Shared download history and the object-storage check: fake storage, no network or boto3."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from product_collector.__main__ import image_index
from product_collector.checkpoint import writer_lock
from product_collector.image_store import S3ImageStore, describe, object_key, read_store_config
from product_collector.media import GIVE_UP_AFTER, localize_images
from product_collector.media_resume import download_history
from product_collector.prune import prune_runs
from product_collector.validation import stored_metadata, verify_assets
from tests.test_media import FakeFetcher, png
from tests.test_prune import run as finished_run

URL = 'https://img.example/a.png?_v=1'


class FakeStore:
    """Answers HEAD from what the pipeline would have uploaded."""
    def __init__(self, objects=None, error=None):
        self.objects, self.error, self.keys = objects or {}, error, []

    def upload(self, entry):
        self.objects[object_key(entry)] = {'bytes': entry['bytes'], 'sha256': entry['sha256']}

    def head(self, key):
        self.keys.append(key)
        if self.error:
            raise self.error
        return self.objects.get(key)


def not_found():
    response = requests.Response()
    response.status_code = 404
    return requests.HTTPError('404', response=response)


class IndexTests(unittest.TestCase):
    def setUp(self):
        waits = patch('product_collector.media.RETRY_WAITS', (0.0, 0.0))  # the retry passes, without waiting
        waits.start()
        self.addCleanup(waits.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.index = self.root / '_image_index'
        self.runs = 0

    def tearDown(self):
        self.directory.cleanup()

    def collect(self, values, store=None, urls=(URL,), **options):
        self.runs += 1
        run = self.root / f'run{self.runs}'
        run.mkdir()
        product = {'source': 'danawa', 'product_id': '11', 'categories': [{'category_id': '1'}],
                   'images': [{'url': url, 'role': 'gallery', 'order': i} for i, url in enumerate(urls)]}
        (run / 'products.jsonl').write_text(json.dumps(product) + '\n', encoding='utf-8')
        fetcher = FakeFetcher(values)
        report = localize_images(run / 'products.jsonl', run / 'media', fetcher, index_dir=self.index,
                                 object_store=store, retry_transient=True, **options)
        return run, fetcher, report

    def rows(self, run):
        return [json.loads(line) for line in (run / 'media/products_with_local_images.jsonl').read_text(encoding='utf-8').splitlines()]

    def test_an_image_already_in_storage_is_not_downloaded_again(self):
        store = FakeStore()
        first, fetcher, report = self.collect({URL: png()}, store)
        self.assertEqual((fetcher.calls, report['downloaded'], report['store_checks']), ([URL], 1, 0))
        downloaded = self.rows(first)[0]['images'][0]
        store.upload(downloaded)  # the pipeline loads the first run

        second, fetcher, report = self.collect({}, store)
        self.assertEqual((fetcher.calls, report['requests'], report['stored']), ([], 0, 1))
        row = self.rows(second)[0]
        image = row['images'][0]
        self.assertEqual(image['status'], 'stored')
        self.assertEqual(stored_metadata(image), stored_metadata(downloaded))
        self.assertNotIn('object_key', image)
        self.assertEqual((image['local_path'], image['filename'], image['category_paths']), (None, None, {}))
        self.assertEqual(row['image_download_status'], 'completed')
        self.assertEqual(report['result'], 'completed')
        self.assertEqual(store.keys, [f"images/sha256/{downloaded['sha256'][:2]}/{downloaded['sha256']}.png"])
        self.assertFalse(any((second / 'media/images').iterdir()))  # no bytes kept for stored images
        self.assertEqual(verify_assets(second / 'media/products_with_local_images.jsonl', second / 'media')['result'],
                         'completed')

    def test_missing_mismatched_or_unreachable_objects_are_downloaded(self):
        store = FakeStore()
        first, _, _ = self.collect({URL: png()}, store)
        entry = self.rows(first)[0]['images'][0]
        cases = {'missing': {},
                 'size': {object_key(entry): {'bytes': entry['bytes'] + 1, 'sha256': entry['sha256']}},
                 'sha': {object_key(entry): {'bytes': entry['bytes'], 'sha256': '0' * 64}}}
        for name, objects in cases.items():
            with self.subTest(name):
                _, fetcher, report = self.collect({URL: png()}, FakeStore(objects))
                self.assertEqual((fetcher.calls, report['stored'], report['downloaded']), ([URL], 0, 1))
        _, fetcher, report = self.collect({URL: png()}, FakeStore(error=OSError('timeout')))
        self.assertEqual((fetcher.calls, report['store_errors']), ([URL], 1))

    def test_without_storage_settings_everything_downloads_but_history_is_kept(self):
        # Until the server loads through v2 ingest-*, a stored image would be loaded as no image.
        self.collect({URL: png()})
        second, fetcher, report = self.collect({URL: png()})
        self.assertEqual((fetcher.calls, report['store_checks'], report['object_store'], report['stored']),
                         ([URL], 0, False, 0))
        self.assertFalse([i for r in self.rows(second) for i in r['images'] if i['status'] == 'stored'])
        self.assertEqual(download_history(self.index / 'image_downloads.jsonl')[URL]['status'], 'downloaded')
        # Resuming a run whose journal already says "stored" (made with settings) downloads without them.
        store = FakeStore()
        first, _, _ = self.collect({URL: png()}, store)
        store.upload(self.rows(first)[0]['images'][0])
        third, _, report = self.collect({}, store)
        self.assertEqual(report['stored'], 1)
        fetcher = FakeFetcher({URL: png()})
        report = localize_images(third / 'products.jsonl', third / 'media', fetcher, index_dir=self.index,
                                 retry_transient=True)
        self.assertEqual((fetcher.calls, report['stored'], report['downloaded']), ([URL], 0, 1))
        self.assertFalse([i for r in self.rows(third) for i in r['images'] if i['status'] == 'stored'])

    def test_repeated_storage_errors_stop_asking(self):
        urls = [f'https://img.example/{i}.png' for i in range(25)]
        store = FakeStore()
        self.collect({u: png() for u in urls}, store, urls)
        store.error = OSError('denied')
        _, fetcher, report = self.collect({u: png() for u in urls}, store, urls)
        self.assertEqual((report['store_checks'], report.get('store_disabled'), len(fetcher.calls)),
                         (20, 'consecutive_errors', 25))

    def test_a_new_index_imports_successful_downloads_of_earlier_runs(self):
        old = self.root / 'old' / 'media'
        old.mkdir(parents=True)
        legacy_product = self.root / 'old' / 'products.jsonl'
        legacy_product.write_text(json.dumps({'source': 'danawa', 'product_id': '11',
                                              'images': [{'url': URL}, {'url': URL + 'x'}]}) + '\n', encoding='utf-8')
        localize_images(legacy_product, old, FakeFetcher({URL: png(), URL + 'x': not_found()}))  # a partial run
        entry = download_history(old / 'image_downloads.jsonl')[URL]
        store = FakeStore()
        store.upload(entry)
        _, fetcher, report = self.collect({URL + 'x': png()}, store, (URL, URL + 'x'), import_from=[old])
        self.assertEqual((fetcher.calls, report['stored']), ([URL + 'x'], 1))
        seeded = download_history(self.index / 'image_downloads.jsonl')[URL]
        self.assertNotIn('local_path', seeded)
        self.assertEqual(seeded['imported_from'], str(old))

    def test_resuming_a_run_from_before_the_index_publishes_its_downloads(self):
        run = self.root / 'legacy'
        run.mkdir()
        (run / 'products.jsonl').write_text(json.dumps({'source': 'danawa', 'product_id': '11',
                                                        'images': [{'url': URL}]}) + '\n', encoding='utf-8')
        localize_images(run / 'products.jsonl', run / 'media', FakeFetcher({URL: png()}))
        self.index.mkdir()
        (self.index / 'image_downloads.jsonl').write_text('', encoding='utf-8')  # index already seeded
        report = localize_images(run / 'products.jsonl', run / 'media', FakeFetcher({}), index_dir=self.index,
                                 retry_transient=True)
        self.assertEqual(report['cached'], 1)
        entry = download_history(self.index / 'image_downloads.jsonl')[URL]
        self.assertEqual(entry['status'], 'downloaded')
        self.assertNotIn('local_path', entry)

    def test_not_found_gives_up_after_three_runs_and_transient_errors_keep_retrying(self):
        store = FakeStore()
        for attempt in range(1, GIVE_UP_AFTER + 1):
            _, fetcher, _ = self.collect({URL: not_found()}, store)
            self.assertEqual(fetcher.calls, [URL])
        self.assertTrue(download_history(self.index / 'image_downloads.jsonl')[URL]['gave_up'])
        _, fetcher, report = self.collect({URL: png()}, store)
        self.assertEqual((fetcher.calls, report['gave_up']), ([], 1))
        _, fetcher, _ = self.collect({URL: png()}, store, retry_failed=True)
        self.assertEqual(fetcher.calls, [URL])

        other = 'https://img.example/b.png'
        for _ in range(GIVE_UP_AFTER + 1):
            # Never given up; each run tries it once more in each retry pass.
            _, fetcher, _ = self.collect({other: requests.ConnectionError('reset')}, store, (other,))
            self.assertEqual(fetcher.calls, [other] * 3)

    def test_a_non_image_response_counts_as_a_permanent_failure(self):
        for _ in range(GIVE_UP_AFTER):
            self.collect({URL: b'<html>not an image</html>'})
        self.assertTrue(download_history(self.index / 'image_downloads.jsonl')[URL]['gave_up'])

    def test_a_failure_keeps_the_last_success_usable(self):
        store = FakeStore()
        first, _, _ = self.collect({URL: png()}, store)
        self.collect({URL: requests.Timeout('slow')}, store)  # not uploaded yet: downloads and fails
        latest = download_history(self.index / 'image_downloads.jsonl')[URL]
        self.assertEqual((latest['status'], latest['last_success']['sha256']),
                         ('failed', self.rows(first)[0]['images'][0]['sha256']))
        store.upload(self.rows(first)[0]['images'][0])
        _, fetcher, report = self.collect({}, store)
        self.assertEqual((fetcher.calls, report['stored']), ([], 1))

    def test_resume_reuses_this_runs_answers(self):
        store = FakeStore()
        first, _, _ = self.collect({URL: png()}, store)
        store.upload(self.rows(first)[0]['images'][0])
        second, _, _ = self.collect({}, store)
        store.keys.clear()
        report = localize_images(second / 'products.jsonl', second / 'media', FakeFetcher({}), index_dir=self.index,
                                 object_store=store, retry_transient=True)
        self.assertEqual((report['stored'], store.keys), (1, []))

    def test_the_index_is_compacted(self):
        # Rewritten once it holds more than twice the URLs, so the history stays bounded.
        for _ in range(6):
            self.collect({URL: requests.Timeout('slow')})
            lines = [l for l in (self.index / 'image_downloads.jsonl').read_text(encoding='utf-8').splitlines() if l.strip()]
            self.assertLessEqual(len(lines), 2)

    def test_stored_entries_with_bad_metadata_fail_verification(self):
        media = self.root / 'media'
        media.mkdir()
        image = {'url': URL, 'status': 'stored', 'sha256': 'a' * 64, 'bytes': 10, 'width': 1, 'height': 1,
                 'format': 'JPEG', 'extension': 'jpeg'}
        (media / 'rows.jsonl').write_text(json.dumps({'source': 'danawa', 'product_id': '11', 'images': [image]}) + '\n',
                                          encoding='utf-8')
        report = verify_assets(media / 'rows.jsonl', media)
        self.assertEqual([e['reason'] for e in report['errors']], ['stored_image_invalid'])

    def test_stored_metadata_requires_a_known_format(self):
        good = {'sha256': 'a' * 64, 'bytes': 10, 'width': 1, 'height': 1, 'format': 'JPEG', 'extension': 'jpg'}
        self.assertIsNotNone(stored_metadata(good))
        for fmt, ext in ((None, None), ('jpeg', 'jpg'), (['JPEG'], 'jpg'), ('ICO', 'ico')):
            self.assertIsNone(stored_metadata({**good, 'format': fmt, 'extension': ext}), fmt)


class SettingsTests(unittest.TestCase):
    def test_a_resumed_run_keeps_its_image_settings(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            run = output / 'run1'
            run.mkdir()
            config = {'endpoint': 'https://s3.example:8443', 'bucket': 'images', 'accessKey': 'k', 'secretKey': 's'}
            index, _ = image_index(output, run, 1, config)
            self.assertEqual(index, (output / '_image_index').resolve())
            saved = json.loads((run / 'media/image_settings.json').read_text(encoding='utf-8'))
            self.assertEqual(saved['object_store'], describe(config))
            self.assertFalse({'accessKey', 'secretKey'} & set(saved['object_store']))  # no key pair
            self.assertEqual(image_index(output, run, 1, config)[0], index)
            with self.assertRaisesRegex(ValueError, '이미지 단계 설정'):
                image_index(output, run, 1, None)
            with self.assertRaisesRegex(ValueError, '이미지 단계 설정'):
                image_index(output, run, 2, config)

    def test_store_settings_file(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'crawler.properties'
            path.write_text('# image bucket, read only\nendpoint=https://s3.example:8443\nbucket=images\n'
                            'accessKey=k\nsecretKey=s\ncaBundle=/etc/ca.pem\nother=ignored\n', encoding='utf-8')
            if os.name == 'posix':
                path.chmod(0o600)
            self.assertEqual(read_store_config(path)['caBundle'], '/etc/ca.pem')
            path.write_text('endpoint=http://s3.example\nbucket=b\naccessKey=k\nsecretKey=s\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'https'):
                read_store_config(path)
            path.write_text('endpoint=https://s3.example\nbucket=b\naccessKey=k\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'secretKey'):
                read_store_config(path)

    def test_head_answers_none_only_for_a_missing_object(self):
        class Error(Exception):
            def __init__(self, code):
                self.response = {'Error': {'Code': code}}

        class Client:
            def __init__(self, error=None, name='sha256'):
                self.error, self.name = error, name

            def head_object(self, Bucket, Key):
                if self.error:
                    raise self.error
                return {'ContentLength': 10, 'Metadata': {self.name: 'a' * 64}}

        config = {'bucket': 'images'}
        self.assertEqual(S3ImageStore(config, Client()).head('k'), {'bytes': 10, 'sha256': 'a' * 64})
        # Garage capitalises the metadata key.
        self.assertEqual(S3ImageStore(config, Client(name='Sha256')).head('k'), {'bytes': 10, 'sha256': 'a' * 64})
        self.assertIsNone(S3ImageStore(config, Client(Error('404'))).head('k'))
        with self.assertRaises(Error):
            S3ImageStore(config, Client(Error('403'))).head('k')


class PruneTests(unittest.TestCase):
    def test_prune_keeps_a_run_whose_image_step_is_working_and_never_the_index(self):
        with tempfile.TemporaryDirectory() as root:
            old = finished_run(root, 'w1', '2026-10-01T00:00:00+00:00')
            finished_run(root, 'w2', '2026-10-02T00:00:00+00:00')
            (old / 'media').mkdir()
            (Path(root) / '_image_index').mkdir()
            with writer_lock(old / 'media' / '.image-download.lock'):
                report = prune_runs(root, keep=1, apply=True)
            self.assertEqual((report['deleted'], report['kept']['w1']), ([], 'in_use'))
            self.assertEqual(prune_runs(root, keep=1, apply=True)['deleted'], ['w1'])
            self.assertTrue((Path(root) / '_image_index').is_dir())


if __name__ == '__main__':
    unittest.main()
