"""Explicit table/checkpoint identity. Secrets stay in deployment environment."""
from dataclasses import dataclass
import re
from urllib.parse import unquote, urlsplit

from ..contracts import digest
from ..errors import PipelineError
from ..files import read_json


def location(value):
    if not isinstance(value, str) or not value or '\\' in value:
        raise PipelineError('BRONZE_ABSOLUTE_URI_REQUIRED')
    parsed = urlsplit(value)
    if (parsed.scheme not in {'s3a', 'file'} or parsed.query or parsed.fragment or parsed.username
            or parsed.password or (parsed.scheme == 's3a' and not parsed.netloc)
            or not parsed.path.startswith('/') or parsed.path in {'', '/'}
            or any(p in {'.', '..', ''} for p in unquote(parsed.path).strip('/').split('/'))):
        raise PipelineError('BRONZE_ABSOLUTE_URI_REQUIRED')
    return f'{parsed.scheme}://{parsed.netloc}{parsed.path}'.rstrip('/')


@dataclass(frozen=True)
class BronzeSettings:
    environment_id: str
    mongo_database: str
    bronze_path: str
    events_path: str
    quarantine_path: str
    checkpoint_path: str
    kafka_cluster_id: str
    single_writer_confirmed: bool = False
    mongo_replica_set: str = 'rs-products'
    mongo_uri_env: str = 'PIPELINE_MONGO_URI'
    kafka_options_env: str = 'BRONZE_KAFKA_OPTIONS'
    spark_options_env: str = 'BRONZE_SPARK_OPTIONS'
    kafka_topic: str = 'product.raw.collected'
    max_records: int = 20
    max_raw_bytes: int = 32 * 1024 * 1024
    max_input_bytes: int = 8 * 1024 * 1024
    trigger_seconds: int = 30
    lease_database: str = 'pipeline_ops'
    lease_collection: str = 'bronze_writer_leases'
    lease_uri_env: str = 'BRONZE_LEASE_MONGO_URI'
    lease_seconds: int = 120
    lease_heartbeat_seconds: int = 10
    checkpoint_recovery: dict | None = None

    def __post_init__(self):
        for value in (self.environment_id, self.mongo_database, self.mongo_replica_set, self.kafka_topic,
                      self.kafka_cluster_id, self.lease_database, self.lease_collection):
            if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', value):
                raise PipelineError('INVALID_BRONZE_TARGET')
        for value in (self.mongo_uri_env, self.kafka_options_env, self.spark_options_env, self.lease_uri_env):
            if not isinstance(value, str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*', value):
                raise PipelineError('INVALID_SECRET_ENV_NAME')
        paths = [location(p) for p in (self.bronze_path, self.events_path, self.quarantine_path, self.checkpoint_path)]
        for i, p in enumerate(paths):
            for q in paths[i + 1:]:
                if p == q or p.startswith(q + '/') or q.startswith(p + '/'):
                    raise PipelineError('BRONZE_PATHS_OVERLAP')
        if self.single_writer_confirmed is not True:
            raise PipelineError('BRONZE_SINGLE_WRITER_REQUIRED')
        if (self.lease_database == self.mongo_database or self.lease_collection == 'raw_products'
                or type(self.lease_seconds) is not int or not 60 <= self.lease_seconds <= 3600
                or type(self.lease_heartbeat_seconds) is not int
                or not 1 <= self.lease_heartbeat_seconds <= self.lease_seconds // 6):
            raise PipelineError('BRONZE_INVALID_LEASE_CONFIG')
        if (type(self.max_records) is not int or not 1 <= self.max_records <= 100
                or type(self.max_raw_bytes) is not int or not 1 <= self.max_raw_bytes <= 128 * 1024 * 1024
                or type(self.max_input_bytes) is not int or not 1 <= self.max_input_bytes <= 32 * 1024 * 1024
                or type(self.trigger_seconds) is not int or not 1 <= self.trigger_seconds <= 3600):
            raise PipelineError('BRONZE_INVALID_BATCH_LIMIT')
        from .recovery import recovery
        recovery(self)

    @property
    def fingerprint(self):
        return digest({k: getattr(self, k) for k in ('environment_id', 'mongo_database', 'mongo_replica_set',
            'bronze_path', 'events_path', 'quarantine_path', 'checkpoint_path', 'kafka_topic', 'kafka_cluster_id')} | {'contract_version': 1})

    @classmethod
    def load(cls, path):
        try:
            return cls(**read_json(path))
        except TypeError as exc:
            raise PipelineError('BRONZE_INVALID_CONFIG') from exc
