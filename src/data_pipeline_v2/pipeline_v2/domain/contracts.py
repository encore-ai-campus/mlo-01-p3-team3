from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json


class Invalid(ValueError):
    pass


class SourcePageLimit(Invalid):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def seal(value):
    return {**value, 'content_hash': digest(value)}


def verified(value):
    if not isinstance(value, dict) or value.get('content_hash') != digest({k:v for k,v in value.items() if k != 'content_hash'}):
        raise Invalid('PROOF_CHANGED')
    return value


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def utc(value):
    time = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if time.tzinfo is None:
        raise Invalid('TIMEZONE_REQUIRED')
    return time.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


@dataclass(frozen=True)
class Limits:
    observations: int = 100
    page_bytes: int = 32 * 1024 * 1024
    history_rows: int = 20000
    history_bytes: int = 32 * 1024 * 1024
    pending_high: int = 3
    pending_low: int = 1
    pending_bytes: int = 512 * 1024 * 1024
    min_free_bytes: int = 1024 * 1024 * 1024
    max_attempts: int = 3

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for k,v in vars(self).items() if k != 'pending_low'):
            raise Invalid('POSITIVE_LIMIT_REQUIRED')
        if not 0 <= self.pending_low < self.pending_high or self.observations > 1000:
            raise Invalid('LIMIT_RANGE_INVALID')


def pages(values, limits):
    pending, size = [], 0
    for value in values:
        count = len(canonical(value).encode())
        if count > limits.page_bytes:
            raise Invalid('OBSERVATION_BYTE_LIMIT')
        if pending and (len(pending) >= limits.observations or size + count > limits.page_bytes):
            yield pending
            pending, size = [], 0
        pending.append(value)
        size += count
    if pending:
        yield pending


def row(key, product, payload):
    return dict(row_key=key, product_key=product, payload=payload, record_hash=digest(payload))


def check_rows(rows):
    seen = {}
    for item in rows:
        if item['record_hash'] != digest(item['payload']):
            raise Invalid('ROW_HASH_CHANGED')
        key = item['row_key']
        if key in seen and seen[key] != item:
            raise Invalid('ROW_ID_CONFLICT')
        seen[key] = item
    return list(seen.values())
