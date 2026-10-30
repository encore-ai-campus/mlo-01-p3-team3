"""Catalog images matched without _v and the replacement check: fake storage and fetcher, no network."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from product_collector.__main__ import image_index
from product_collector.image_store import object_key
from product_collector.media import image_identity, localize_images, same_picture
from product_collector.media_resume import download_history
from tests.test_image_index import FakeStore, not_found
from tests.test_media import FakeFetcher

PATH = 'https://img.danuri.io/catalog-image/264/512/081/99046c300b584abb98a39c4be8c8346f.jpg'


def url(version, shrink='500:500'):
    return f'{PATH}?shrink={shrink}&_v={version}'


def jpeg(quality=90, color=(200, 40, 40), size=(40, 30)):
    image = Image.new('RGB', size, color)
    for x in range(size[0]):  # a gradient, so another quality gives other bytes
        image.putpixel((x, x % size[1]), (x * 6 % 256, 90, 160))
    stream = io.BytesIO()
    image.save(stream, format='JPEG', quality=quality)
    return stream.getvalue()


class BytesStore(FakeStore):
    """Also keeps the bytes the pipeline uploaded, for the replacement check's GET."""
    def __init__(self):
        super().__init__()
        self.blobs, self.gets = {}, []

    def put(self, entry, data):
        self.upload(entry)
        self.blobs[object_key(entry)] = data

    def get(self, key):
        self.gets.append(key)
        return self.blobs[key]


class IdentityTests(unittest.TestCase):
    def test_only_catalog_images_lose_their_v(self):
        self.assertEqual(image_identity(url(1)), image_identity(url(2)))
        self.assertNotEqual(image_identity(url(1)), image_identity(url(1, '330:*')))
        self.assertEqual(image_identity(f'{PATH}?_v=1'), image_identity(PATH))
        for other in ('https://img.danawa.com/prod_img/500000/264/512/img/81512264_1.jpg?_v=1',
                      'https://img.example/catalog-image/264/512/081/99046c300b584abb98a39c4be8c8346f.jpg?_v=1',
                      'https://img.danuri.io/catalog-image/264/512/081/not-a-hash.jpg?_v=1'):
            self.assertIsNone(image_identity(other), other)

    def test_a_reencoding_is_the_same_picture_and_another_picture_is_not(self):
        self.assertNotEqual(jpeg(90), jpeg(80))
        self.assertTrue(same_picture(jpeg(90), jpeg(80)))
        self.assertFalse(same_picture(jpeg(), jpeg(color=(30, 200, 30))))
        self.assertFalse(same_picture(jpeg(), jpeg(size=(30, 40))))


class MatchTests(unittest.TestCase):
    def setUp(self):
        waits = patch('product_collector.media.RETRY_WAITS', (0.0, 0.0))
        waits.start()
        self.addCleanup(waits.stop)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.store, self.runs = BytesStore(), 0

    def collect(self, values, image_url, run=None, sample=0, identity_keys=True):
        if run is None:
            self.runs += 1
            run = self.root / f'run{self.runs}'
            run.mkdir()
            product = {'source': 'danawa', 'product_id': '11', 'categories': [{'category_id': '1'}],
                       'images': [{'url': image_url, 'role': 'gallery', 'order': 0}]}
            (run / 'products.jsonl').write_text(json.dumps(product) + '\n', encoding='utf-8')
        fetcher = FakeFetcher(values)
        report = localize_images(run / 'products.jsonl', run / 'media', fetcher, index_dir=self.root / '_image_index',
                                 object_store=self.store, retry_transient=True, identity_keys=identity_keys,
                                 replacement_sample=sample)
        image = json.loads((run / 'media/products_with_local_images.jsonl').read_text(encoding='utf-8'))['images'][0]
        return run, fetcher, report, image

    def first(self):
        data = jpeg(90)
        _, _, _, image = self.collect({url(1): data}, url(1))
        self.store.put(image, data)  # the pipeline loads the first run
        return image

    def shared(self):
        return download_history(self.root / '_image_index/image_downloads.jsonl')

    def test_another_v_of_a_stored_image_is_not_downloaded_and_keeps_its_observed_url(self):
        first = self.first()
        run, fetcher, report, image = self.collect({}, url(2))
        self.assertEqual((fetcher.calls, report['stored'], report['stored_by_identity']), ([], 1, 1))
        self.assertEqual((image['url'], image['status'], image['sha256']), (url(2), 'stored', first['sha256']))
        self.assertFalse({'matched_url', 'identity'} & set(image))  # not handed to the pipeline
        journal = download_history(run / 'media/image_downloads.jsonl')[url(2)]
        self.assertEqual((journal['matched_url'], journal['identity']), (url(1), image_identity(url(2))))
        self.assertNotIn(url(2), self.shared())  # the shared history keeps real downloads only
        self.assertEqual(report['result'], 'completed')
        self.assertFalse((run / 'media/image_replacement.json').exists())  # no sample asked

    def test_without_identity_keys_or_with_another_shrink_the_image_is_downloaded(self):
        self.first()
        _, fetcher, report, _ = self.collect({url(2): jpeg(80)}, url(2), identity_keys=False)
        self.assertEqual((fetcher.calls, report['stored_by_identity']), ([url(2)], 0))
        _, fetcher, report, _ = self.collect({url(3, '330:*'): jpeg(80)}, url(3, '330:*'))
        self.assertEqual((fetcher.calls, report['stored_by_identity']), ([url(3, '330:*')], 0))

    def test_a_sampled_reencoding_keeps_the_earlier_bytes_and_confirms_them(self):
        first = self.first()
        run, fetcher, report, image = self.collect({url(2): jpeg(80)}, url(2), sample=5)
        self.assertEqual(fetcher.calls, [url(2)])
        self.assertEqual((image['status'], image['sha256']), ('stored', first['sha256']))
        check = report['replacement_check']
        self.assertEqual((check['targets'], check['reencoded'], check['changed'], check['rate']), (1, 1, 0, 0.0))
        self.assertEqual(self.store.gets, [object_key(first)])
        self.assertIn('confirmed_at', self.shared()[url(1)])
        self.assertEqual(report['alerts'], [])
        # A resume asks nothing again: the sample and its verdicts are kept in the run.
        _, fetcher, report, _ = self.collect({}, url(2), run=run, sample=5)
        self.assertEqual((fetcher.calls, report['replacement_check']['reencoded']), ([], 1))

    def test_another_picture_at_the_same_path_is_downloaded_with_an_alert_and_becomes_the_match(self):
        first = self.first()
        other = jpeg(90, color=(30, 200, 30))
        run, _, report, image = self.collect({url(2): other}, url(2), sample=5)
        self.assertEqual(image['status'], 'downloaded')
        self.assertNotEqual(image['sha256'], first['sha256'])
        journal = download_history(run / 'media/image_downloads.jsonl')[url(2)]
        self.assertEqual(journal['replaced_sha256'], first['sha256'])
        self.assertEqual((report['downloaded'], report['stored'], report['stored_by_identity']), (1, 0, 0))
        self.assertEqual([a['signal'] for a in report['alerts']], ['image_replaced_at_same_path'])
        # A stop after the journal but before the verdict was saved: the resume still counts it.
        state_path = run / 'media/image_replacement.json'
        state = json.loads(state_path.read_text(encoding='utf-8'))
        state['done'] = {}
        state_path.write_text(json.dumps(state), encoding='utf-8')
        _, fetcher, report, _ = self.collect({}, url(2), run=run, sample=5)
        self.assertEqual((fetcher.calls, report['replacement_check']['changed']), ([], 1))
        self.store.put(image, other)
        run, fetcher, _, image = self.collect({}, url(3))
        self.assertEqual(fetcher.calls, [])
        self.assertEqual(image['sha256'], self.shared()[url(2)]['sha256'])  # the newest real download
        self.assertEqual(download_history(run / 'media/image_downloads.jsonl')[url(3)]['matched_url'], url(2))

    def test_a_failed_check_leaves_the_image_stored_and_is_not_an_image_failure(self):
        self.first()
        _, fetcher, report, image = self.collect({url(2): not_found()}, url(2), sample=5)
        self.assertEqual((fetcher.calls, image['status']), ([url(2)], 'stored'))
        self.assertEqual((report['replacement_check']['failed'], report['failures']['failed']), (1, 0))
        self.assertEqual(report['result'], 'completed')

    def test_a_blocked_check_stops_the_step_and_is_asked_again_on_resume(self):
        self.first()
        blocked = not_found()
        blocked.response.status_code = 429
        run, _, report, _ = self.collect({url(2): blocked}, url(2), sample=5)
        self.assertEqual((report['result'], report['replacement_check']['stopped']), ('partial', 'http_429'))
        _, fetcher, report, _ = self.collect({url(2): jpeg(80)}, url(2), run=run, sample=5)
        self.assertEqual((fetcher.calls, report['replacement_check']['reencoded'], report['result']),
                         ([url(2)], 1, 'completed'))

    def test_the_key_rules_and_the_sample_are_fixed_per_run(self):
        run = self.root / 'run'
        run.mkdir()
        config = {'endpoint': 'https://s3.example:8443', 'bucket': 'images', 'accessKey': 'k', 'secretKey': 's'}
        image_index(self.root, run, 2, config, 1000)
        saved = json.loads((run / 'media/image_settings.json').read_text(encoding='utf-8'))
        self.assertEqual((saved['image_key_version'], saved['replacement_sample']), (2, 1000))
        with self.assertRaisesRegex(ValueError, '이미지 단계 설정'):
            image_index(self.root, run, 2, config, 200)


if __name__ == '__main__':
    unittest.main()
