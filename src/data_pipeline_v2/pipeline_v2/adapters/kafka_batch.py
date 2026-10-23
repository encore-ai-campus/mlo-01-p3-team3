"""Kafka producer and bounded Spark source. No consumer commit or streaming query."""
import json
import os
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
            self.producer = Producer({**options,'acks':'all','enable.idempotence':True,
                'max.in.flight.requests.per.connection':1,'delivery.timeout.ms':30000})
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
        idempotent single-in-flight producer, so a group is not weaker than one send per message.
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
        self.producer.flush(60)
        acks = []
        for result in results:
            if result is None or result[0] is not None or result[1].offset() is None or result[1].offset() < 0:
                acks.append(None)
            else:
                message = result[1]
                acks.append(dict(topic=message.topic(),partition=message.partition(),offset=message.offset()))
        return acks

    def close(self):
        if self.kind == 'kafka-python':
            self.producer.close(timeout=30)
        else:
            self.producer.flush(30)


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
