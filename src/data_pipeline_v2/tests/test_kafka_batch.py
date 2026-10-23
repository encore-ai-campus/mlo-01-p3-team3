"""Producer settings and failure codes, against a fake confluent_kafka module (no broker)."""
import json
import os
import sys
import types
import unittest
from unittest.mock import patch

import test_engine  # noqa: F401  (puts the v2 and legacy packages on sys.path)
from pipeline_v2.adapters.kafka_batch import KafkaProducer
from pipeline_v2.domain.batch import BatchLimits
from pipeline_v2.domain.contracts import Invalid


class FakeError:
    def __init__(self, name):
        self._name = name

    def name(self):
        return self._name


class FakeMessage:
    def __init__(self, offset):
        self._offset = offset

    def offset(self):
        return self._offset

    def topic(self):
        return 'topic'

    def partition(self):
        return 0


def fake_module(failures=()):
    """failures: positions whose delivery report carries an error instead of an offset."""
    module = types.ModuleType('confluent_kafka')

    class Producer:
        created = []

        def __init__(self, config):
            self.config, self.pending, self.flushed = config, [], []
            Producer.created.append(self)

        def produce(self, topic, key, value, on_delivery):
            self.pending.append(on_delivery)

        def poll(self, timeout):
            pass

        def flush(self, timeout):
            self.flushed.append(timeout)
            for position, report in enumerate(self.pending):
                if position in failures:
                    report(FakeError('_MSG_TIMED_OUT'), None)
                else:
                    report(None, FakeMessage(position))
            self.pending = []

    module.Producer = Producer
    return module


class KafkaProducerTests(unittest.TestCase):
    def producer(self, options=None, failures=()):
        module = fake_module(failures)
        env = {'TEST_KAFKA_OPTIONS': json.dumps(options or {'bootstrap.servers': 'broker:9092'})}
        with patch.dict(sys.modules, {'confluent_kafka': module}), patch.dict(os.environ, env):
            made = KafkaProducer(dict(topic='topic', producer_options_env='TEST_KAFKA_OPTIONS'))
        return made, module.Producer.created[-1]

    def test_reliability_is_fixed_and_throughput_settings_are_sized_for_the_group(self):
        _, fake = self.producer()
        self.assertEqual((fake.config['acks'], fake.config['enable.idempotence']), ('all', True))
        # Idempotent ordering holds only up to 5 in flight; more would reorder retries.
        self.assertEqual(fake.config['max.in.flight.requests.per.connection'], 5)
        self.assertEqual(fake.config['compression.type'], 'lz4')
        # Queue wait counts toward the delivery timeout; the default group must fit inside it.
        self.assertGreaterEqual(fake.config['delivery.timeout.ms'], 120000)
        self.assertLessEqual(BatchLimits().outbox_chunk_bytes, 8 * 1024 * 1024)
        with self.assertRaisesRegex(Invalid, 'RELIABILITY_OPTIONS_FIXED'):
            self.producer({'bootstrap.servers': 'broker:9092', 'acks': '1'})

    def test_operator_options_may_tune_compression(self):
        _, fake = self.producer({'bootstrap.servers': 'broker:9092', 'compression.type': 'zstd'})
        self.assertEqual(fake.config['compression.type'], 'zstd')

    def test_success_returns_one_ack_per_message(self):
        producer, fake = self.producer()
        acks = producer.send_many([('k%d' % i, {'n': i}) for i in range(3)])
        self.assertEqual([a['offset'] for a in acks], [0, 1, 2])
        self.assertIsNone(producer.failure_code)
        self.assertEqual(fake.flushed, [150])

    def test_failure_keeps_a_symbolic_code_and_no_payload(self):
        producer, _ = self.producer(failures={1})
        acks = producer.send_many([('k%d' % i, {'secret': 'value-%d' % i}) for i in range(3)])
        self.assertEqual([a is None for a in acks], [False, True, False])
        self.assertEqual(producer.failure_code, 'KAFKA_DELIVERY_MSG_TIMED_OUT')
        self.assertNotIn('value', producer.failure_code)
        with self.assertRaisesRegex(Invalid, 'KAFKA_DELIVERY_UNCONFIRMED'):
            self.producer(failures={0})[0].send('k', {})


if __name__ == '__main__':
    unittest.main()
