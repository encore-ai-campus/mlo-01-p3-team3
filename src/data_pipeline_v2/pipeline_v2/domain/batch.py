"""Finite crawl batches, immutable event identities and offset range contracts."""
from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from .contracts import canonical, digest, verified, utc, Invalid

BATCH_SCHEMA = 1
CODE_VERSION = 'bounded-batch-2'
OBSERVATION = 'product.observation.snapshot'
REJECTED = 'product.observation.rejected'
# Input disposition axis. Quality and stage progress are separate fields.
DISPOSITIONS = ('ACCEPTED', 'INPUT_QUARANTINE', 'PAYLOAD_OMITTED', 'SOURCE_METADATA_ONLY')


@dataclass(frozen=True)
class BatchLimits:
    max_events: int = 10000
    max_input_bytes: int = 512 * 1024 * 1024
    max_event_bytes: int = 2 * 1024 * 1024
    source_page_size: int = 100
    pending_batches: int = 3
    pending_bytes: int = 1024 * 1024 * 1024
    pending_age_seconds: int = 24 * 3600
    history_observations: int = 1000
    history_bytes: int = 64 * 1024 * 1024
    publication_products: int = 100
    publication_bytes: int = 32 * 1024 * 1024
    spark_partitions: int = 3
    outbox_chunk: int = 200
    # At the measured ~0.2 MB/s uncompressed broker throughput, 8 MiB drains well inside the 120 s delivery timeout.
    outbox_chunk_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in vars(self).values()):
            raise Invalid('BATCH_LIMIT_INVALID')
        if self.max_events > 100000 or self.max_event_bytes > 8 * 1024 * 1024 or self.publication_products > 100 or self.outbox_chunk > 1000:
            raise Invalid('BATCH_LIMIT_TOO_LARGE')


@dataclass(frozen=True)
class Retention:
    """Quarantine-only lifetimes. Normal raw/Bronze retention follows its own data contract."""
    quarantine_raw_days: int = 30
    error_metadata_days: int = 90

    def __post_init__(self):
        if any(type(v) is not int or not 1 <= v <= 3650 for v in vars(self).values()):
            raise Invalid('RETENTION_DAYS_INVALID')
        if self.quarantine_raw_days > self.error_metadata_days:
            raise Invalid('RETENTION_RAW_LONGER_THAN_METADATA')

    def expiry(self, base, kind):
        days = self.quarantine_raw_days if kind == 'raw' else self.error_metadata_days
        return utc((datetime.fromisoformat(utc(base).replace('Z', '+00:00')) + timedelta(days=days)).isoformat())


def batch_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value):
        raise Invalid('CRAWL_BATCH_ID_INVALID')
    return value


def product_key(value):
    if not isinstance(value, str) or not re.fullmatch(r'danawa:[0-9]+', value):
        raise Invalid('SOURCE_IDENTITY_INVALID')
    return value


def inventory_hash(inventory):
    return digest(sorted(inventory, key=lambda v: v['event_id']))


def wire_event(event, payload):
    """Kafka record body: admitted payload travels with its event so Executors need no source DB."""
    return dict(event, payload=payload) if payload is not None else event


def outbox_event(value):
    return {k:v for k,v in value.items() if k != 'payload'}


def kafka_key(event):
    # Rejected rows have no trusted product identity; their receive position keys them.
    return event['product_key'] or event['event_id']


def source_contract(value):
    batch_id(value['crawl_batch_id'])
    if value.get('schema_version') != BATCH_SCHEMA or value.get('status') != 'SOURCE_COMPLETE':
        raise Invalid('CRAWL_NOT_COMPLETE')
    required, completed = value.get('required_sources'), value.get('completed_sources')
    if not required or len(set(required)) != len(required) or set(required) != set(completed or []):
        raise Invalid('REQUIRED_SOURCE_INCOMPLETE')
    if utc(value['started_at']) > utc(value['completed_at']):
        raise Invalid('CRAWL_TIME_ORDER_INVALID')
    if not re.fullmatch(r'[a-f0-9]{64}', value.get('input_sha256', '')):
        raise Invalid('CRAWL_INPUT_HASH_REQUIRED')
    if type(value.get('event_count')) is not int or value['event_count'] < 0:
        raise Invalid('CRAWL_COUNT_REQUIRED')
    if value.get('input_semantics') != 'OBSERVATION_SNAPSHOT':
        raise Invalid('SOURCE_SEMANTICS_UNSUPPORTED')
    return value


def admission_contract(value):
    allowed = {'storage_allowed', 'quarantine_allowed', 'field_allowlist'}
    if (not isinstance(value, dict) or not set(value) <= allowed or value.get('storage_allowed') is not True
            or type(value.get('quarantine_allowed')) is not bool):
        raise Invalid('EXPLICIT_SOURCE_ADMISSION_REQUIRED')
    fields = value.get('field_allowlist')
    if fields is not None and (not isinstance(fields, list) or not fields
            or any(not isinstance(v, str) or not v for v in fields) or len(set(fields)) != len(fields)):
        raise Invalid('FIELD_ALLOWLIST_INVALID')
    return value


def event_for_item(item, crawl_batch_id, *, storage_allowed, quarantine_allowed, field_allowlist=None):
    # Admission policy comes from deployment configuration, never from event payload.
    if storage_allowed is not True:
        raise Invalid('SOURCE_STORAGE_NOT_ALLOWED')
    try:
        doc, state = item['document'], item['image_state']
        product = product_key(doc['source_code'] + ':' + doc['source_product_id'])
        if not re.fullmatch(r'[a-f0-9]{24}', doc['_id']):
            raise Invalid('SOURCE_DOCUMENT_ID_INVALID')
        if digest({k:v for k,v in doc.items() if k != 'document_hash'}) != doc['document_hash']:
            raise Invalid('SOURCE_DOCUMENT_HASH_CONFLICT')
        identity = dict(product_key=product, raw_document_id=doc['_id'],
            source_version=doc['observation_key'], document_hash=doc['document_hash'],
            image_revision=state['image_state_revision'], image_fingerprint=state['image_state_fingerprint'])
        collected_at = utc(doc['collected_at'])
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, Invalid):
            raise
        raise Invalid('SOURCE_IDENTITY_MISSING') from None
    payload_hash = digest(item)
    return dict(schema_version=BATCH_SCHEMA, event_type=OBSERVATION,
        crawl_batch_id=batch_id(crawl_batch_id), event_id=digest(identity), product_key=product,
        source_ref=payload_hash, payload_hash=payload_hash, source_version=doc['observation_key'],
        raw_document_id=doc['_id'], collected_at=collected_at,
        quarantine_allowed=quarantine_allowed is True)


# Operational fields that change without any business change. They are diagnostics: the image state
# fingerprint already excludes them, so they must not change event identity or payload hash either.
VOLATILE_IMAGE_STATE = ('image_state_checked_at',)


def stable_item(item):
    """The admitted form of a source item, without diagnostic-only fields."""
    state = item.get('image_state') if isinstance(item, dict) else None
    if not isinstance(state, dict) or not any(k in state for k in VOLATILE_IMAGE_STATE):
        return item
    return dict(item, image_state={k: v for k, v in state.items() if k not in VOLATILE_IMAGE_STATE})


def outside_allowlist(item, field_allowlist):
    """Top-level source fields outside the deployment allowlist block every persistent copy."""
    if field_allowlist is None:
        return False
    document = item.get('document') if isinstance(item, dict) else None
    return not isinstance(document, dict) or not set(document) <= set(field_allowlist)


def rejected_event(crawl_batch_id, position, reason, disposition, payload_hash=None):
    if disposition not in DISPOSITIONS[1:]:
        raise Invalid('REJECTED_DISPOSITION_INVALID')
    if type(position) is not int or position < 0:
        raise Invalid('RECEIVE_POSITION_INVALID')
    if (disposition == 'INPUT_QUARANTINE') != (payload_hash is not None):
        raise Invalid('QUARANTINE_REFERENCE_INVALID')
    # The receive position is the stable reprocessing identity when source keys are unusable.
    event_id = digest(dict(kind=REJECTED, batch=batch_id(crawl_batch_id), position=position))
    return dict(schema_version=BATCH_SCHEMA, event_type=REJECTED, crawl_batch_id=crawl_batch_id,
        event_id=event_id, product_key=None, source_ref=event_id if payload_hash else None,
        payload_hash=payload_hash, received_position=position, reason=error_code(reason), disposition=disposition)


def error_code(value):
    value = str(value)
    return value if re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', value) else 'SOURCE_INPUT_INVALID'


def validate_event(event):
    accepted = {'schema_version','event_type','crawl_batch_id','event_id','product_key','source_ref',
                'payload_hash','source_version','raw_document_id','collected_at','quarantine_allowed'}
    rejected = {'schema_version','event_type','crawl_batch_id','event_id','product_key','source_ref',
                'payload_hash','received_position','reason','disposition'}
    if not isinstance(event, dict) or event.get('schema_version') != BATCH_SCHEMA:
        raise Invalid('BATCH_EVENT_SCHEMA_UNSUPPORTED')
    batch_id(event.get('crawl_batch_id'))
    hexes = lambda *names: all(re.fullmatch(r'[a-f0-9]{64}', str(event[k])) for k in names)
    if event.get('event_type') == OBSERVATION:
        # The Kafka wire copy may carry the admitted payload; the Outbox record never does.
        if set(event) - {'payload'} != accepted:
            raise Invalid('BATCH_EVENT_SCHEMA_INVALID')
        if 'payload' in event and digest(event['payload']) != event['payload_hash']:
            raise Invalid('BATCH_EVENT_PAYLOAD_CHANGED')
        if not hexes('event_id','source_ref','payload_hash','source_version'):
            raise Invalid('BATCH_EVENT_ID_INVALID')
        if event['source_ref'] != event['payload_hash']:
            raise Invalid('BATCH_EVENT_REFERENCE_INVALID')
        try:
            product_key(event['product_key'])
        except Invalid:
            raise Invalid('BATCH_EVENT_REFERENCE_INVALID') from None
        utc(event['collected_at'])
    elif event.get('event_type') == REJECTED:
        if set(event) != rejected:
            raise Invalid('BATCH_EVENT_SCHEMA_INVALID')
        expected = rejected_event(event['crawl_batch_id'], event['received_position'], event['reason'],
            event['disposition'], event['payload_hash'])
        if event != expected:
            raise Invalid('BATCH_EVENT_REFERENCE_INVALID')
    else:
        raise Invalid('BATCH_EVENT_SCHEMA_UNSUPPORTED')
    return event


def delivery_contract(value):
    if value is None:
        raise Invalid('DELIVERY_COMPLETE_REQUIRED')
    verified(value)
    if value.get('status') != 'COMPLETE' or value.get('schema_version') != BATCH_SCHEMA:
        raise Invalid('DELIVERY_NOT_COMPLETE')
    batch_id(value['crawl_batch_id'])
    inventory = value['inventory']
    if (len(inventory) != value['event_count'] or len({v['event_id'] for v in inventory}) != len(inventory)
            or inventory_hash(inventory) != value['event_set_hash']):
        raise Invalid('DELIVERY_INVENTORY_CONFLICT')
    seen = set()
    for position in value['ranges']:
        key = (position['topic'], position['partition'])
        if (key in seen or not re.fullmatch(r'[A-Za-z0-9_.-]{1,249}', key[0])
                or any(type(position[k]) is not int for k in ('partition','start_offset','end_offset'))
                or key[1] < 0 or not 0 <= position['start_offset'] < position['end_offset']):
            raise Invalid('KAFKA_RANGE_INVALID')
        seen.add(key)
    if bool(inventory) != bool(value['ranges']):
        raise Invalid('KAFKA_RANGE_MISSING')
    return value


def offsets(manifest):
    delivery_contract(manifest)
    starts, ends = {}, {}
    for item in manifest['ranges']:
        starts.setdefault(item['topic'], {})[str(item['partition'])] = item['start_offset']
        ends.setdefault(item['topic'], {})[str(item['partition'])] = item['end_offset']
    return canonical(starts), canonical(ends)


def logical_job(manifest, policy_hash, target_hash):
    # Delivery generations change physical offsets, not the logical transformation input.
    return digest(dict(batch=manifest['crawl_batch_id'], events=manifest['event_set_hash'],
        policy=policy_hash, target=target_hash, code=CODE_VERSION))


def retraction(value):
    """An explicit withdrawal of one raw source version. It never deletes other observations."""
    fields = {'product_key', 'raw_document_id', 'reason'}
    if not isinstance(value, dict) or set(value) != fields:
        raise Invalid('RETRACTION_SCHEMA_INVALID')
    product_key(value['product_key'])
    if not re.fullmatch(r'[a-f0-9]{24}', str(value['raw_document_id'])):
        raise Invalid('SOURCE_DOCUMENT_ID_INVALID')
    if error_code(value['reason']) != value['reason']:
        raise Invalid('RETRACTION_REASON_INVALID')
    return value


def repair_job(products, retractions, policy_hash, target_hash):
    if not products or len(products) != len(set(products)):
        raise Invalid('REPAIR_PRODUCTS_REQUIRED')
    for value in products:
        product_key(value)
    retracted = digest(sorted(retractions, key=canonical))
    key = digest(dict(kind='REPAIR', products=sorted(products), retractions=retracted,
        policy=policy_hash, target=target_hash, code=CODE_VERSION))
    return key, 'repair-' + key[:24], retracted
