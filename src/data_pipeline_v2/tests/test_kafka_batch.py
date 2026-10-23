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


class FakeKafkaMessage:
    def __init__(self, offset, key, value, error=None):
        self._offset, self._key, self._value, self._error = offset, key, value, error

    def error(self):
        return self._error

    def topic(self):
        return 'topic'

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value


def fake_consumer_module(log):
    """log: offset -> (key, value) for partition 0; polling past the end yields PARTITION_EOF."""
    module = types.ModuleType('confluent_kafka')

    class KafkaError:
        _PARTITION_EOF = -191

        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

        def name(self):
            return 'EOF' if self._code == -191 else 'OTHER'

    class TopicPartition:
        def __init__(self, topic, partition, offset):
            self.topic, self.partition, self.offset = topic, partition, offset

    class Consumer:
        created = []

        def __init__(self, config):
            # confluent-kafka refuses a consumer without a group id; mirror that.
            if not config.get('group.id'):
                raise ValueError('group.id must be set')
            self.config, self.position, self.polls, self.closed = config, None, 0, False
            Consumer.created.append(self)

        def assign(self, partitions):
            self.position = partitions[0].offset

        def unassign(self):
            self.position = None

        def poll(self, timeout):
            self.polls += 1
            if self.position in log:
                key, value = log[self.position]
                message = FakeKafkaMessage(self.position, key.encode(), value.encode())
                self.position += 1
                return message
            return FakeKafkaMessage(None, None, None, error=KafkaError(KafkaError._PARTITION_EOF))

        def close(self):
            self.closed = True

    module.Consumer, module.TopicPartition, module.KafkaError = Consumer, TopicPartition, KafkaError
    return module


class KafkaRangeReaderTests(unittest.TestCase):
    def reader(self, log, options=None):
        from pipeline_v2.adapters.kafka_batch import KafkaRangeReader
        module = fake_consumer_module(log)
        env = {'TEST_KAFKA_OPTIONS': json.dumps(options or {'bootstrap.servers': 'broker:9092'})}
        with patch.dict(sys.modules, {'confluent_kafka': module}), patch.dict(os.environ, env):
            made = KafkaRangeReader(dict(topic='topic', producer_options_env='TEST_KAFKA_OPTIONS'))
            # Construction alone opens no connection; prepare/submit never touch Kafka.
            self.assertEqual(module.Consumer.created, [])
            consumer = made.consumer
        return made, consumer, module

    def manifest(self, start, end):
        return {'ranges': [dict(topic='topic', partition=0, start_offset=start, end_offset=end)]}

    def test_reads_exactly_the_manifest_range_without_group_commits(self):
        log = {i: ('k%d' % i, '{"n":%d}' % i) for i in range(10)}
        reader, consumer, module = self.reader(log)
        with patch.dict(sys.modules, {'confluent_kafka': module}):
            rows = list(reader.rows(self.manifest(3, 7)))
        self.assertEqual([r['offset'] for r in rows], [3, 4, 5, 6])
        self.assertEqual(rows[0]['key'], 'k3')
        self.assertEqual(consumer.config['enable.auto.commit'], False)
        # A group id is mandatory for the client, but nothing is ever committed to it.
        self.assertTrue(consumer.config['group.id'])
        self.assertEqual(consumer.config['enable.auto.offset.store'], False)
        # The end offset is exclusive: one extra poll at most, never a read to the log's end.
        self.assertLessEqual(consumer.polls, 5)
        self.assertIsNone(consumer.position)

    def test_group_coordinator_errors_do_not_stop_an_assigned_read(self):
        log = {i: ('k%d' % i, 'v%d' % i) for i in range(3)}
        reader, consumer, module = self.reader(log)
        original = consumer.poll
        injected = []

        def poll(timeout):
            if not injected:
                injected.append(1)
                error = module.KafkaError(-1)
                error.name = lambda: 'GROUP_AUTHORIZATION_FAILED'
                return FakeKafkaMessage(None, None, None, error=error)
            return original(timeout)
        consumer.poll = poll
        with patch.dict(sys.modules, {'confluent_kafka': module}):
            rows = list(reader.rows(self.manifest(0, 3)))
        self.assertEqual([r['offset'] for r in rows], [0, 1, 2])
        self.assertEqual(reader.ignored_group_errors, 1)

    def test_other_read_errors_stop_with_a_symbolic_code(self):
        reader, consumer, module = self.reader({0: ('k', 'v')})

        def poll(timeout):
            error = module.KafkaError(-2)
            error.name = lambda: 'TOPIC_AUTHORIZATION_FAILED'
            return FakeKafkaMessage(None, None, None, error=error)
        consumer.poll = poll
        with patch.dict(sys.modules, {'confluent_kafka': module}):
            with self.assertRaisesRegex(Invalid, 'KAFKA_READ_TOPIC_AUTHORIZATION_FAILED'):
                list(reader.rows(self.manifest(0, 1)))

    def test_missing_offsets_are_unavailable_not_skipped(self):
        reader, _, module = self.reader({i: ('k', 'v') for i in range(3)})
        with patch.dict(sys.modules, {'confluent_kafka': module}):
            with self.assertRaisesRegex(Invalid, 'KAFKA_OFFSET_UNAVAILABLE'):
                list(reader.rows(self.manifest(0, 5)))

    def test_reader_options_cannot_override_progress_semantics(self):
        with self.assertRaisesRegex(Invalid, 'READER_OPTIONS_FIXED'):
            self.reader({}, {'bootstrap.servers': 'b', 'group.id': 'custom'})
