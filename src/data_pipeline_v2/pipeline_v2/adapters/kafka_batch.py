"""Kafka producer and bounded Spark source. No consumer commit or streaming query."""
import json
import os
import re
from .files import locked, read, write
from ..domain.batch import offsets
from ..domain.contracts import canonical, digest, seal, verified, Invalid


class FileBroker:
    """Local contract backend with durable partitions; it makes no Kafka performance claim."""
    def __init__(self, root, topic='product-events-v2', partitions=3):
        from pathlib import Path
        self.root, self.topic, self.partitions = Path(root), topic, partitions

    def send(self, key, event):
        partition = int(digest(key), 16) % self.partitions
        path = self.root / self.topic / (str(partition) + '.json')
        with locked(path.with_suffix('.lock')):
            rows = read(path) if path.exists() else []
            offset = len(rows)
            rows.append(dict(topic=self.topic,partition=partition,offset=offset,key=key,value=canonical(event)))
            write(path, rows)
        return dict(topic=self.topic,partition=partition,offset=offset)

    def send_many(self, pairs):
        return [self.send(key, event) for key, event in pairs]

    def rows(self, manifest):
        for position in manifest['ranges']:
            path = self.root / position['topic'] / (str(position['partition']) + '.json')
            rows = read(path) if path.exists() else []
            if len(rows) < position['end_offset']:
                raise Invalid('KAFKA_OFFSET_UNAVAILABLE')
            yield from rows[position['start_offset']:position['end_offset']]

    def close(self):
        pass


class KafkaProducer:
    """Durable acks=all producer. The server venv ships confluent-kafka; kafka-python is a fallback."""
    def __init__(self, config):
        options = json.loads(os.environ[config['producer_options_env']])
        self.topic = config['topic']
        try:
            from confluent_kafka import Producer
        except ImportError:
            Producer = None
        if Producer is not None:
            fixed = {'acks','enable.idempotence','max.in.flight.requests.per.connection'}
            if fixed & set(options):
                raise Invalid('KAFKA_RELIABILITY_OPTIONS_FIXED')
            self.kind = 'confluent'
            # Queue wait counts toward delivery.timeout.ms, so the group size (outbox_chunk_bytes) and this
            # timeout must be sized together against the measured broker throughput. JSON compresses well.
            # Up to 5 requests in flight keeps per-partition order under idempotence (the broker's
            # limit); one in flight paid a full replication round trip per request (~0.27 s/message).
            self.producer = Producer({'compression.type':'lz4','linger.ms':20,'request.timeout.ms':30000,
                'delivery.timeout.ms':120000,**options,'acks':'all','enable.idempotence':True,
                'max.in.flight.requests.per.connection':5})
        else:
            from kafka import KafkaProducer as Legacy
            if {'acks','enable_idempotence','value_serializer','key_serializer'} & set(options):
                raise Invalid('KAFKA_RELIABILITY_OPTIONS_FIXED')
            self.kind = 'kafka-python'
            self.producer = Legacy(**options,acks='all',enable_idempotence=True,
                key_serializer=lambda key:key.encode('utf-8'),
                value_serializer=lambda event:canonical(event).encode('utf-8'))

    def send(self, key, event):
        ack = self.send_many([(key, event)])[0]
        if ack is None:
            raise Invalid('KAFKA_DELIVERY_UNCONFIRMED')
        return ack

    def send_many(self, pairs):
        """Publish a bounded group together and wait once. Returns one ack per pair, None if unconfirmed.

        Only a broker acknowledgement with an offset counts as delivery. Per-key order is kept by the
        idempotent producer (at most 5 in flight), so a group is not weaker than one send per message.
        """
        pairs = list(pairs)
        if self.kind == 'kafka-python':
            futures = [self.producer.send(self.topic,key=key,value=event) for key, event in pairs]
            acks = []
            for future in futures:
                try:
                    metadata = future.get(timeout=60)
                    acks.append(dict(topic=metadata.topic,partition=metadata.partition,offset=metadata.offset))
                except Exception:
                    acks.append(None)
            return acks
        results = [None] * len(pairs)

        def reporter(position):
            def report(error, message):
                results[position] = (error, message)
            return report
        for position, (key, event) in enumerate(pairs):
            while True:
                try:
                    self.producer.produce(self.topic,key=key.encode('utf-8'),value=canonical(event).encode('utf-8'),
                        on_delivery=reporter(position))
                    break
                except BufferError:
                    # Local queue full: let delivery reports drain it instead of failing the group.
                    self.producer.poll(1)
            self.producer.poll(0)
        self.producer.flush(150)
        acks = []
        self.failure_code = None
        for result in results:
            if result is None or result[0] is not None or result[1].offset() is None or result[1].offset() < 0:
                acks.append(None)
                # Only the Kafka error name is kept: a fixed symbolic code, never a message or payload.
                name = result[0].name() if result is not None and result[0] is not None else 'NO_REPORT'
                self.failure_code = self.failure_code or 'KAFKA_DELIVERY_' + re.sub(r'[^A-Z0-9]+', '_', str(name).upper()).strip('_')[:48]
            else:
                message = result[1]
                acks.append(dict(topic=message.topic(),partition=message.partition(),offset=message.offset()))
        return acks

    def close(self):
        if self.kind == 'kafka-python':
            self.producer.close(timeout=30)
        else:
            self.producer.flush(30)


class KafkaRangeReader:
    """Bounded Python read of a delivery manifest's offset ranges (confluent-kafka).

    Partitions are assigned explicitly and nothing is committed, so no consumer group state exists;
    the manifest is the only progress record, exactly like the Spark batch source. No group.id is
    set: the principal has no group ACL, and librdkafka contacts the group coordinator whenever one
    is configured, even for assign-only reads (GROUP_AUTHORIZATION_FAILED on 2026-10-23).
    """
    # Coordinator-side errors that do not affect explicit-offset fetches from an assigned partition.
    GROUP_ONLY = frozenset({'GROUP_AUTHORIZATION_FAILED', 'COORDINATOR_NOT_AVAILABLE', 'NOT_COORDINATOR'})

    def __init__(self, config):
        from confluent_kafka import Consumer
        options = json.loads(os.environ[config.get('reader_options_env') or config['producer_options_env']])
        if {'group.id','enable.auto.commit','auto.offset.reset'} & set(options):
            raise Invalid('KAFKA_READER_OPTIONS_FIXED')
        self.consumer = Consumer({**options,'enable.auto.commit':False,'enable.auto.offset.store':False,
            'auto.offset.reset':'error','enable.partition.eof':True})
        self.ignored_group_errors = 0

    def rows(self, manifest):
        from confluent_kafka import TopicPartition, KafkaError
        for position in manifest['ranges']:
            self.consumer.assign([TopicPartition(position['topic'],position['partition'],position['start_offset'])])
            try:
                expected = position['start_offset']
                while expected < position['end_offset']:
                    message = self.consumer.poll(30)
                    if message is None:
                        raise Invalid('KAFKA_OFFSET_UNAVAILABLE')
                    if message.error() is not None:
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            raise Invalid('KAFKA_OFFSET_UNAVAILABLE')
                        name = re.sub(r'[^A-Z0-9]+','_',str(message.error().name()).upper()).strip('_')[:40]
                        if name in self.GROUP_ONLY:
                            self.ignored_group_errors += 1
                            continue
                        raise Invalid('KAFKA_READ_' + name)
                    if message.offset() < position['start_offset']:
                        continue
                    if message.offset() >= position['end_offset']:
                        break
                    expected = message.offset() + 1
                    yield dict(topic=message.topic(),partition=message.partition(),offset=message.offset(),
                        key=message.key().decode('utf-8') if message.key() is not None else None,
                        value=message.value().decode('utf-8'))
            finally:
                self.consumer.unassign()

    def close(self):
        self.consumer.close()


def read_kafka(spark, config, manifest, min_partitions):
    starts, ends = offsets(manifest)
    if not manifest['ranges']:
        return spark.createDataFrame([], 'topic string, partition int, offset long, key string, value string')
    options = json.loads(os.environ[config['spark_options_env']])
    if any(key in options for key in ('subscribe','assign','startingOffsets','endingOffsets','failOnDataLoss')):
        raise Invalid('KAFKA_INPUT_BOUNDARIES_FIXED')
    assignment = {}
    for position in manifest['ranges']:
        assignment.setdefault(position['topic'], []).append(position['partition'])
    reader = spark.read.format('kafka').options(**options).option('assign', canonical(assignment))
    return (reader.option('startingOffsets',starts).option('endingOffsets',ends)
        .option('failOnDataLoss','true').option('minPartitions',str(min_partitions)).load()
        .selectExpr('topic','partition','offset','CAST(key AS STRING) AS key','CAST(value AS STRING) AS value'))
