"""Durability checks must include effective topic configuration, not only ISR."""
from concurrent.futures import Future
from types import SimpleNamespace
import unittest

from product_pipeline.errors import PipelineError
from product_pipeline.publisher import KafkaPublisher


class Admin:
    def __init__(self, value='2', error=None):
        self.value, self.error, self.calls = value, error, 0

    def describe_configs(self, resources, **kwargs):
        self.calls += 1
        future = Future()
        if self.error:
            future.set_exception(self.error)
        else:
            future.set_result({} if self.value is None else {
                'min.insync.replicas': SimpleNamespace(value=self.value)})
        return {resources[0]: future}


class KafkaPreflightTests(unittest.TestCase):
    def publisher(self, admin, replicas=3, isrs=2):
        partition = SimpleNamespace(replicas=list(range(replicas)), isrs=list(range(isrs)))
        topic = SimpleNamespace(error=None, partitions={0: partition})
        producer = SimpleNamespace(list_topics=lambda **kwargs: SimpleNamespace(topics={'topic': topic}))
        return KafkaPublisher('topic', {'bootstrap.servers': 'unused'}, producer=producer, admin_client=admin)

    def test_effective_min_isr_two_or_three_accepted(self):
        for value in ('2', '3'):
            admin = Admin(value)
            self.publisher(admin).preflight()
            self.assertEqual(admin.calls, 1)

    def test_missing_invalid_or_weak_min_isr_rejected(self):
        for value in (None, '', 'unknown', '1', '0'):
            with self.subTest(value=value), self.assertRaisesRegex(PipelineError, 'KAFKA_MIN_ISR_CONTRACT_REQUIRED'):
                self.publisher(Admin(value)).preflight()

    def test_config_permission_failure_is_not_success(self):
        with self.assertRaisesRegex(PipelineError, 'KAFKA_CONFIG_UNCONFIRMED'):
            self.publisher(Admin(error=RuntimeError('sensitive SDK error'))).preflight()

    def test_replication_and_current_isr_still_required(self):
        for replicas, isrs in ((2, 2), (3, 1)):
            with self.assertRaisesRegex(PipelineError, 'KAFKA_REPLICATION_CONTRACT_REQUIRED'):
                self.publisher(Admin(), replicas, isrs).preflight()


if __name__ == '__main__':
    unittest.main()
