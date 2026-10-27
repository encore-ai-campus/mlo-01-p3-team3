"""Validate the existing event/envelope contracts before any Bronze write."""
import base64
from dataclasses import dataclass
import hashlib
import re
from uuid import UUID

from ..contracts import (canonical, collector_payload_hash, digest, event_for,
                         image_projection, now, observation_key, utc)
from ..errors import PipelineError
from ..files import parse


class Poison(PipelineError):
    """A deterministic record failure, eligible for durable quarantine."""


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None

    def __post_init__(self):
        if (not isinstance(self.topic, str) or not self.topic
                or type(self.partition) is not int or self.partition < 0
                or type(self.offset) is not int or self.offset < 0
                or not isinstance(self.key, (bytes, type(None)))
                or not isinstance(self.value, (bytes, type(None)))):
            raise PipelineError('INVALID_KAFKA_POSITION')


def event_from(message):
    try:
        event = parse((message.value or b'').decode('utf-8'))
        if (not isinstance(event, dict) or set(event) != {
                'event_id', 'event_type', 'schema_version', 'occurred_at', 'correlation_id', 'payload'}
                or type(event['schema_version']) is not int or event['schema_version'] != 1
                or event['event_type'] != 'product.raw.collected'):
            raise ValueError()
        UUID(event['event_id'])
        if str(UUID(event['event_id'])) != event['event_id'] or utc(event['occurred_at']) != event['occurred_at']:
            raise ValueError()
        payload = event['payload']
        fields = {'raw_document_id', 'source_code', 'source_product_id', 'record_type', 'source_record_key',
                  'collection_run_id', 'collected_at', 'payload_hash', 'schema_version',
                  'observation_key', 'document_hash'}
        if not isinstance(payload, dict) or set(payload) != fields:
            raise ValueError()
        if any(not isinstance(v, str) or not v for v in payload.values()):
            raise ValueError()
        if (not re.fullmatch(r'[0-9a-f]{24}', payload['raw_document_id'])
                or payload['source_code'] != 'danawa' or payload['record_type'] != 'PRODUCT'
                or not re.fullmatch(r'[0-9]+', payload['source_product_id'])
                or payload['source_record_key'] != 'danawa:PRODUCT:' + payload['source_product_id']
                or payload['schema_version'] != 'danawa-ingest-1'
                or event['correlation_id'] != payload['collection_run_id']
                or utc(payload['collected_at']) != payload['collected_at']
                or any(not re.fullmatch(r'[0-9a-f]{64}', payload[k]) for k in
                       ('payload_hash', 'observation_key', 'document_hash'))):
            raise ValueError()
    except (PipelineError, ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError) as exc:
        raise Poison('BRONZE_INVALID_EVENT') from exc
    if message.key != payload['source_record_key'].encode('utf-8'):
        raise Poison('BRONZE_KAFKA_KEY_MISMATCH')
    return event


def validate_raw(doc, event):
    try:
        if not isinstance(doc, dict) or doc['_id'] != event['payload']['raw_document_id']:
            raise ValueError()
        if digest({k: v for k, v in doc.items() if k != 'document_hash'}) != doc['document_hash']:
            raise ValueError()
        raw = {**doc['collector_metadata'], 'payload': doc['payload']}
        images = doc['image_refs']
        if (doc['schema_version'] != 'danawa-ingest-1' or doc['ingest_contract_version'] != 1
                or collector_payload_hash(doc['payload']) != doc['payload_hash']
                or digest(raw) != doc['source_observation_hash']
                or digest([image_projection(i) for i in images]) != doc['gallery_acquisition_hash']
                or observation_key(raw, images) != doc['observation_key']
                or doc['source_product_id'] != raw['product_id']
                or doc['source_code'] != raw['source']
                or doc['collection_run_id'] != raw['job_id']
                or doc['collected_at'] != utc(raw['collected_at'])
                or event != event_for(doc, event['event_id'], event['occurred_at'])):
            raise ValueError()
    except (PipelineError, ValueError, TypeError, KeyError, AttributeError, RecursionError) as exc:
        raise Poison('BRONZE_RAW_CONTRACT_CONFLICT') from exc


def raw_row(doc):
    return {'raw_document_id': doc['_id'], **{k: doc[k] for k in (
        'source_code', 'source_product_id', 'source_record_key', 'collection_run_id',
        'observation_key', 'payload_hash', 'document_hash', 'schema_version', 'collected_at')},
        'payload_json': canonical(doc['payload']), 'raw_envelope_json': canonical(doc), 'ingested_at': now()}


def event_row(event, message):
    return {'event_id': event['event_id'], 'event_hash': digest(event),
            'event_json': canonical(event), 'raw_document_id': event['payload']['raw_document_id'],
            'document_hash': event['payload']['document_hash'], 'kafka_topic': message.topic,
            'kafka_partition': message.partition, 'kafka_offset': message.offset, 'recorded_at': now()}


def quarantine_row(message, code):
    checksum = hashlib.sha256(message.value or b'').hexdigest()
    identity = {'topic': message.topic, 'partition': message.partition,
                'offset': message.offset, 'value_sha256': checksum}
    evidence = {**identity,
                'key_base64': None if message.key is None else base64.b64encode(message.key).decode('ascii'),
                'value_base64': None if message.value is None else base64.b64encode(message.value).decode('ascii')}
    return {'quarantine_id': digest(identity), 'evidence_hash': digest(evidence),
            'record_json': canonical(evidence), 'error_code': code, 'recorded_at': now()}
