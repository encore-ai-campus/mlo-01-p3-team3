"""Garage deliberately overwrites ordinary PUTs; safety must hold without CAS."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from product_pipeline.config import Settings
from product_pipeline.contracts import canonical, digest
from product_pipeline.errors import Conflict, PipelineError
from product_pipeline.objects import S3Objects


class Missing(Exception):
    response = {'Error': {'Code': 'NoSuchKey'}}


class Garage:
    def __init__(self, barrier=None):
        self.values = {}; self.calls = []; self.barrier = barrier
        self.local = threading.local(); self.lock = threading.Lock()
        self.lost = False; self.fail_before_write = False

    def get_object(self, Bucket, Key):
        self.calls.append(('GET', Key))
        if self.barrier and not getattr(self.local, 'started', False):
            self.local.started = True
            self.barrier.wait(timeout=5)
            raise Missing()
        with self.lock:
            if Key not in self.values:
                raise Missing()
            return {'Body': io.BytesIO(self.values[Key])}

    def put_object(self, **request):
        assert 'IfNoneMatch' not in request
        self.calls.append(('PUT', request['Key']))
        if self.fail_before_write:
            raise TimeoutError()
        with self.lock:
            self.values[request['Key']] = request['Body']
        if self.lost:
            self.lost = False
            raise TimeoutError()


class GarageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name)/'data'; self.data = b'gallery bytes'
        self.source.write_bytes(self.data)
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.key = f'images/sha256/{self.sha[:2]}/{self.sha}.png'
        self.client = Garage()
        self.store = S3Objects('pj3-data', client=self.client, profile='garage_v241')

    def test_image_is_created_and_reused_without_conditional_header(self):
        self.assertTrue(self.store.ensure(self.key, self.source, self.sha, len(self.data)))
        self.assertFalse(self.store.ensure(self.key, self.source, self.sha, len(self.data)))
        self.assertEqual(sum(method=='PUT' for method,_ in self.client.calls), 1)

    def test_wrong_key_and_mutable_delta_keys_are_rejected_without_requests(self):
        for key in ['image.png', 'lake/_delta_log/00000000000000000001.json',
                    f'images/sha256/ff/{self.sha}.png', '../'+self.key,
                    'handoff/chunks/sha256/'+'0'*64]:
            with self.subTest(key=key), self.assertRaisesRegex(Conflict, 'OBJECT_KEY_CONTENT_MISMATCH'):
                self.store.ensure(key, self.source, self.sha, len(self.data))
        self.assertEqual(self.client.calls, [])

    def test_other_bytes_cannot_overwrite_same_key_before_any_request(self):
        self.store.ensure(self.key, self.source, self.sha, len(self.data))
        before = list(self.client.calls)
        self.source.write_bytes(b'other')
        with self.assertRaisesRegex(Conflict, 'OBJECT_KEY_CONTENT_MISMATCH'):
            self.store.ensure(self.key, self.source, hashlib.sha256(b'other').hexdigest(), 5)
        self.assertEqual(self.client.calls, before)
        self.assertEqual(self.client.values[self.key], self.data)

    def test_external_existing_corruption_is_detected_without_repair_put(self):
        self.client.values[self.key] = b'foreign'
        with self.assertRaisesRegex(Conflict, 'OBJECT_CONTENT_CONFLICT'):
            self.store.ensure(self.key, self.source, self.sha, len(self.data))
        self.assertEqual([method for method,_ in self.client.calls], ['GET'])

    def test_source_changes_cannot_reach_network(self):
        self.source.write_bytes(b'changed')
        with self.assertRaisesRegex(Conflict, 'IMAGE_SOURCE_CHANGED'):
            self.store.ensure(self.key, self.source, self.sha, len(self.data))
        self.assertFalse(self.client.calls)

    def test_lost_ack_is_reconciled_by_sha256_readback_without_second_put(self):
        self.client.lost = True
        self.assertTrue(self.store.ensure(self.key, self.source, self.sha, len(self.data)))
        self.assertEqual([m for m,_ in self.client.calls], ['GET','PUT','GET'])

    def test_failed_put_does_not_report_success(self):
        self.client.fail_before_write = True
        with self.assertRaisesRegex(PipelineError, 'OBJECT_WRITE_UNCONFIRMED'):
            self.store.ensure(self.key, self.source, self.sha, len(self.data))

    def test_two_writers_both_observe_absence_but_can_only_write_same_bytes(self):
        self.client.barrier = threading.Barrier(2)
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: self.store.ensure(self.key,self.source,self.sha,len(self.data)), range(2)))
        self.assertEqual(results, [True, True])
        self.assertEqual(self.client.values[self.key], self.data)

    def test_handoff_chunk_uses_full_payload_hash(self):
        self.assertTrue(self.store.ensure('handoff/chunks/sha256/'+self.sha,self.source,self.sha,len(self.data)))

    def ready(self):
        value = dict(transport_schema_version=1, snapshot_id='snapshot', files={})
        value['transport_id'] = digest(value)
        return value

    def test_ready_manifest_key_is_stable_and_canonical(self):
        value = self.ready(); data = canonical(value).encode()
        self.source.write_bytes(data)
        key = f"handoff/ready/{value['transport_id']}.ready.json"
        self.assertTrue(self.store.ensure(key,self.source,hashlib.sha256(data).hexdigest(),len(data)))

    def test_ready_rejects_whitespace_alternate_bytes_and_changed_content(self):
        value = self.ready(); key = f"handoff/ready/{value['transport_id']}.ready.json"
        for data in [json.dumps(value).encode(), canonical({**value, 'snapshot_id':'other'}).encode(), b'[]']:
            self.source.write_bytes(data)
            with self.assertRaisesRegex(Conflict, 'OBJECT_KEY_CONTENT_MISMATCH'):
                self.store.ensure(key,self.source,hashlib.sha256(data).hexdigest(),len(data))
        self.assertFalse(self.client.calls)

    def test_profile_does_not_change_existing_identity_fingerprint(self):
        normal = Settings('vm','raw','pj3-data')
        garage = Settings('vm','raw','pj3-data',s3_profile='garage_v241')
        self.assertEqual(normal.fingerprint,garage.fingerprint)

    def test_configuration_rejects_unknown_profile_and_missing_credentials_env(self):
        with self.assertRaisesRegex(PipelineError, 'S3_PROFILE_INVALID'):
            Settings('vm','raw','pj3-data',s3_profile='guess')
        settings = Settings('vm','raw','pj3-data',s3_profile='garage_v241',s3_credentials_file_env='TEST_PJ3_NO_SUCH_ENV')
        with patch.dict('os.environ', {}, clear=True), self.assertRaisesRegex(PipelineError,'S3_SERVICE_CREDENTIALS_ENV_MISSING'):
            S3Objects.from_settings(settings,'https://example.invalid')

    def test_factory_is_used_for_handoff_bucket_and_service_file(self):
        settings = Settings('vm','raw','images',handoff_bucket='pj3-data',s3_profile='garage_v241',s3_credentials_file_env='PJ3_TEST_FILE')
        with patch.dict('os.environ',{'PJ3_TEST_FILE':'/private/key.properties'}), patch.object(S3Objects,'__init__',return_value=None) as init:
            S3Objects.from_settings(settings,'https://example.invalid',bucket=settings.handoff_bucket)
        init.assert_called_once_with('pj3-data','https://example.invalid',profile='garage_v241',credentials_file='/private/key.properties')


if __name__ == '__main__':
    unittest.main()
