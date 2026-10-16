"""Bounded Producer delivery confirmation. Submission alone is not success."""
import time
from .contracts import canonical
from .errors import PipelineError


class KafkaPublisher:
    def __init__(self, topic, config, producer=None, clock=time.monotonic, max_attempts=3):
        if type(max_attempts) is not int or not 2 <= max_attempts <= 3:
            raise PipelineError("KAFKA_IDEMPOTENT_RETRY_BUDGET_REQUIRED")
        if not isinstance(config, dict) or not config.get("bootstrap.servers"):
            raise PipelineError("KAFKA_CONFIG_REQUIRED")
        if config.get("enable.idempotence", True) is not True or config.get("acks", "all") != "all":
            raise PipelineError("KAFKA_DURABILITY_CONFIG_REQUIRED")
        self.topic, self.clock = topic, clock
        fixed = {**config, "enable.idempotence": True, "acks": "all", "max.in.flight.requests.per.connection": 1,
                 "message.send.max.retries": max_attempts - 1, "delivery.timeout.ms": 30000, "request.timeout.ms": 10000,
                 "socket.timeout.ms": 10000}
        if producer is None:
            from confluent_kafka import Producer
            producer = Producer(fixed)
        self.producer = producer

    def preflight(self):
        metadata = self.producer.list_topics(timeout=10)
        topic = metadata.topics.get(self.topic)
        if topic is None or topic.error or not topic.partitions:
            raise PipelineError("KAFKA_TOPIC_REQUIRED")
        if any(len(p.replicas) != 3 or len(p.isrs) < 2 for p in topic.partitions.values()):
            raise PipelineError("KAFKA_REPLICATION_CONTRACT_REQUIRED")

    def send(self, event, key):
        delivery = []
        def callback(error, message):
            delivery.append(error)
        self.producer.produce(self.topic, key=key.encode(), value=canonical(event).encode(), on_delivery=callback)
        deadline = self.clock() + 35
        while not delivery and self.clock() < deadline:
            self.producer.poll(0.1)
        if not delivery or delivery[0] is not None:
            raise PipelineError("EVENT_DELIVERY_UNCONFIRMED")

    def close(self):
        self.producer.flush(5)
