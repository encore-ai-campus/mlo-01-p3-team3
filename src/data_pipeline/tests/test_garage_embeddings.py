import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from product_pipeline import garage_embeddings as worker
from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json, read_json


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.key = self.work/'api-key'
        self.key.write_text('test-secret')
        self.key.chmod(0o600)
        self.cfg = dict(model=worker.MODEL, preprocessing=worker.PREPROCESSING,
                        dimension=768, bucket='pj3-data', api_key_file=str(self.key))
        self.payload = dict(embedding=dict(values=[1.0]*768))

    def child(self, cfg, work, action, input_path=None):
        if action == 'inventory':
            atomic_json(work/'delta-inventory.json', dict(version=0, records={}))
            return dict(status='INVENTORY_READY')
        rows = [json.loads(line) for line in Path(input_path).read_text().splitlines()]
        self.committed.extend(rows)
        return dict(status='IMPORTED', inserted=len(rows), reused=0)

    def test_request_has_dimension_and_no_key_in_url(self):
        def opener(request, timeout):
            self.assertEqual(timeout, 120)
            self.assertNotIn('test-secret', request.full_url)
            self.assertEqual(request.get_header('X-goog-api-key'), 'test-secret')
            body = json.loads(request.data)
            self.assertEqual(body['outputDimensionality'], 768)
            self.assertEqual(body['content']['parts'][0]['inlineData']['mimeType'], 'image/png')
            return io.BytesIO(json.dumps(self.payload).encode())
        self.assertEqual(worker.embed(b'png', self.cfg, 'test-secret', opener=opener), self.payload)

    def test_429_is_not_retried(self):
        calls = []
        def opener(request, timeout):
            calls.append(request)
            raise HTTPError(request.full_url, 429, 'limit', {'Retry-After': '17'}, io.BytesIO())
        with self.assertRaises(worker.RateLimited) as error:
            worker.embed(b'png', self.cfg, 'test-secret', opener=opener)
        self.assertEqual(error.exception.seconds, 17)
        self.assertEqual(len(calls), 1)

    def test_result_normalized_and_stable(self):
        value = worker.result(self.payload, 'a'*64, 'b'*64, self.cfg)
        self.assertAlmostEqual(sum(x*x for x in value['vector']), 1, places=5)
        self.assertEqual(value['record_hash'], worker.result(self.payload, 'a'*64, 'b'*64, self.cfg)['record_hash'])

    def exercise(self, effects, *, reuse_file=None, plan=False):
        self.committed = []
        items = [dict(checksum=x*64, key=x, bytes=3) for x in 'abc']
        with patch.object(worker, 'delta_child', side_effect=self.child), patch.object(worker, 'storage'), \
                patch.object(worker, 'images', return_value=items), patch.object(worker, 'prepare', return_value=b'png'), \
                patch.object(worker, 'embed', side_effect=effects) as api, patch.object(worker.time, 'sleep'):
            result = worker.run(self.cfg, self.work, reuse_file=reuse_file, plan=plan)
        return result, api

    def test_successes_flushed_on_first_429(self):
        report, api = self.exercise([self.payload, worker.RateLimited(60)])
        self.assertEqual(report['status'], 'RATE_LIMIT_429')
        self.assertEqual(api.call_count, 2)
        self.assertEqual(len(self.committed), 1)
        self.assertEqual(read_json(self.work/'state.json')['delta_inserted'], 1)
        self.assertEqual(len(list((self.work/'receipts').glob('*.json'))), 1)

    def test_receipt_recovers_without_another_api_call(self):
        checksum = 'a'*64
        identity = worker.digest(dict(checksum=checksum, model=worker.MODEL,
            preprocessing=worker.PREPROCESSING, dimension=768))
        atomic_json(self.work/'receipts'/(identity+'.json'), dict(image_checksum=checksum,
            preprocessed_checksum=worker.hashlib.sha256(b'png').hexdigest(), model=worker.MODEL,
            preprocessing=worker.PREPROCESSING, dimension=768, payload=self.payload))
        report, api = self.exercise([worker.RateLimited(60)])
        self.assertEqual(report['reused'], 1)
        self.assertEqual(api.call_count, 1)
        self.assertEqual(self.committed[0]['image_checksum'], checksum)

    def test_plan_does_not_require_key(self):
        self.key.unlink()
        report, api = self.exercise([], plan=True)
        self.assertEqual(report['status'], 'PLANNED')
        self.assertEqual(report['new_images'], 3)
        api.assert_not_called()

    def test_reuse_checks_preprocessing_without_paid_call(self):
        reuse = self.work/'reuse.jsonl'
        value = worker.result(self.payload, 'a'*64, 'b'*64, self.cfg)
        reuse.write_text(canonical(worker.serializable(value))+'\n')
        with self.assertRaisesRegex(Exception, 'REUSE_PREPROCESSING_CHECKSUM_MISMATCH'):
            self.exercise([], reuse_file=reuse)


if __name__ == '__main__':
    unittest.main()
