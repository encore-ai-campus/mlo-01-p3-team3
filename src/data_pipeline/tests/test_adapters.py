import io
from pathlib import Path
import tempfile
import types
import unittest

from helpers import png
from product_pipeline.config import Settings
from product_pipeline.contracts import digest, collector_payload_hash
from product_pipeline.errors import Conflict, PipelineError
from product_pipeline.files import read_json
from product_pipeline.mongo import bson_document, wire_document, validate_size, MongoLedger
from product_pipeline.objects import S3Objects
from product_pipeline.publisher import KafkaPublisher


class StorageError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.values, self.writes, self.fail_response = {}, 0, False

    def get_object(self, Bucket, Key):
        if Key not in self.values:
            raise StorageError("NoSuchKey")
        return {"Body": io.BytesIO(self.values[Key]), "ETag": "not-a-sha256"}

    def put_object(self, Bucket, Key, Body, IfNoneMatch, Metadata):
        assert IfNoneMatch == "*"
        if Key in self.values:
            raise StorageError("PreconditionFailed")
        self.values[Key] = Body
        self.writes += 1
        if self.fail_response:
            self.fail_response = False
            raise StorageError("RequestTimeout")


class FakeProducer:
    def __init__(self, deliver=True, error=None):
        self.deliver, self.error, self.time = deliver, error, 0
        self.messages, self.callback = [], None

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self.callback = on_delivery

    def poll(self, timeout):
        self.time += 1
        if self.deliver and self.callback:
            callback, self.callback = self.callback, None
            callback(self.error, None)

    def flush(self, timeout):
        return 0


class AdapterTests(unittest.TestCase):
    def test_crawler_hash_golden_vectors(self):
        values = read_json(Path(__file__).parent / "fixtures/collector_hash_vectors.json")
        for vector in values["vectors"]:
            self.assertEqual(collector_payload_hash(vector["input"]), vector["sha256"])

    def test_canonical_hash_golden_empty_object(self):
        self.assertEqual(digest({}), "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a")

    def test_s3_reads_hash_not_etag_reuses_without_put(self):
        import hashlib
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "image.png"
            data = png()
            source.write_bytes(data)
            h = hashlib.sha256(data).hexdigest()
            fake = FakeS3()
            store = S3Objects("images", client=fake)
            self.assertTrue(store.ensure("images/hash.png", source, h, len(data)))
            self.assertFalse(store.ensure("images/hash.png", source, h, len(data)))
            self.assertEqual(fake.writes, 1)
            fake.values["images/hash.png"] = b"modified"
            with self.assertRaises(Conflict):
                store.ensure("images/hash.png", source, h, len(data))
            self.assertEqual(fake.writes, 1)

    def test_s3_success_lost_response_recovers_by_get(self):
        import hashlib
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "image.png"
            data = png()
            source.write_bytes(data)
            fake = FakeS3()
            fake.fail_response = True
            store = S3Objects("images", client=fake)
            with self.assertRaisesRegex(PipelineError, "OBJECT_WRITE_UNCONFIRMED"):
                store.ensure("key", source, hashlib.sha256(data).hexdigest(), len(data))
            self.assertFalse(store.ensure("key", source, hashlib.sha256(data).hexdigest(), len(data)))
            self.assertEqual(fake.writes, 1)

    def test_s3_permission_denied_does_not_upload(self):
        import hashlib
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "image.png"
            source.write_bytes(png())
            fake = FakeS3()
            fake.get_object = lambda **kwargs: (_ for _ in ()).throw(StorageError("AccessDenied"))
            with self.assertRaisesRegex(PipelineError, "OBJECT_PERMISSION_DENIED"):
                S3Objects("images", client=fake).ensure("key", source, hashlib.sha256(png()).hexdigest(), len(png()))
            self.assertEqual(fake.writes, 0)

    def test_kafka_waits_for_delivery_and_uses_same_bytes(self):
        producer = FakeProducer()
        sender = KafkaPublisher("product.raw.collected", {"bootstrap.servers": "fake:9092"}, producer=producer, clock=lambda: producer.time)
        sender.send({"event_id": "fixed"}, "danawa:PRODUCT:11")
        sender.send({"event_id": "fixed"}, "danawa:PRODUCT:11")
        self.assertEqual(producer.messages[0], producer.messages[1])
        self.assertEqual(producer.messages[0][1], b"danawa:PRODUCT:11")

    def test_kafka_timeout_or_delivery_error_is_not_success(self):
        for deliver, error in ((False, None), (True, "broker-error")):
            producer = FakeProducer(deliver, error)
            sender = KafkaPublisher("topic", {"bootstrap.servers": "fake"}, producer=producer, clock=lambda: producer.time)
            with self.assertRaisesRegex(PipelineError, "EVENT_DELIVERY_UNCONFIRMED"):
                sender.send({"event_id": "one"}, "key")

    def test_kafka_weak_ack_config_rejected(self):
        for extra in ({"acks": "1"}, {"enable.idempotence": False}):
            with self.assertRaises(PipelineError):
                KafkaPublisher("topic", {"bootstrap.servers": "fake", **extra}, producer=FakeProducer())

    def test_bson_wire_roundtrip_and_size_limit(self):
        value = {"_id": "507f1f77bcf86cd799439011", "collected_at": "2026-10-15T03:00:00.000Z", "payload": {"name": "의자"}}
        self.assertEqual(wire_document(bson_document(value)), value)
        validate_size(value)
        with self.assertRaisesRegex(PipelineError, "RAW_TOO_LARGE"):
            validate_size({**value, "payload": "x" * (16 * 1024 * 1024)})
        with self.assertRaisesRegex(PipelineError, "BSON_UNENCODABLE_INPUT"):
            validate_size({**value, "payload": {"too_big_integer": 2 ** 80}})

    def test_standalone_or_wrong_replica_topology_rejected(self):
        for response in ({}, {"setName": "wrong", "hosts": ["a", "b", "c"]},
                         {"setName": "rs-products", "hosts": ["a", "b", "c"], "arbiters": ["c"]}):
            client = types.SimpleNamespace(admin=types.SimpleNamespace(command=lambda _: response), close=lambda: None)
            with self.assertRaisesRegex(PipelineError, "REPLICA_SET_CONTRACT_REQUIRED"):
                MongoLedger(Settings("test", "test", "images"), "not-used", client=client)

    def test_config_rejects_secrets_and_invalid_contract_values(self):
        with self.assertRaises(TypeError):
            Settings(environment_id="test", mongo_database="test", image_bucket="images", mongo_uri="secret")
        for kwargs in ({"max_attempts": 4}, {"lease_seconds": 2}, {"mongo_uri_env": "mongodb://secret"}):
            with self.assertRaises(PipelineError):
                Settings("test", "test", "images", **kwargs)


if __name__ == "__main__":
    unittest.main()
