"""Environment configuration and live ingestion adapter composition."""
import os

from .errors import PipelineError
from .files import parse


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError("REQUIRED_ENV_MISSING", name)
    return value



def open_live(settings, stack):
    from .mongo import MongoLedger
    from .objects import S3Objects
    from .publisher import KafkaPublisher
    if any(v.startswith("replace-with-") for v in (settings.environment_id, settings.mongo_database, settings.image_bucket)):
        raise PipelineError("EXAMPLE_CONFIG_NOT_DEPLOYABLE")
    # Read every required configuration before opening a network connection.
    uri = required_env(settings.mongo_uri_env)
    endpoint = required_env(settings.s3_endpoint_env)
    kafka = parse(required_env(settings.kafka_config_env))
    if endpoint != "aws" and not endpoint.startswith("https://"):
        raise PipelineError("S3_TLS_ENDPOINT_REQUIRED")
    ledger = MongoLedger(settings, uri)
    stack.callback(ledger.close)
    objects = S3Objects.from_settings(settings, endpoint)
    publisher = KafkaPublisher(settings.kafka_topic, kafka, max_attempts=settings.max_attempts)
    stack.callback(publisher.close)
    return ledger, objects, publisher
