"""Non-secret target contracts. Credentials/endpoints are injected at runtime."""
from dataclasses import asdict, dataclass
import re

from .contracts import digest
from .errors import PipelineError
from .files import read_json


@dataclass(frozen=True)
class Settings:
    environment_id: str
    mongo_database: str
    image_bucket: str
    kafka_topic: str = "product.raw.collected"
    mongo_replica_set: str = "rs-products"
    mongo_uri_env: str = "PIPELINE_MONGO_URI"
    s3_endpoint_env: str = "PIPELINE_S3_ENDPOINT"
    kafka_config_env: str = "PIPELINE_KAFKA_CONFIG"
    lease_seconds: int = 120
    max_attempts: int = 3
    handoff_bucket: str | None = None

    def __post_init__(self):
        for value in (self.environment_id, self.mongo_database, self.mongo_replica_set, self.image_bucket, self.kafka_topic):
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
                raise PipelineError("INVALID_TARGET_CONFIG")
        for value in (self.mongo_uri_env, self.s3_endpoint_env, self.kafka_config_env):
            if not isinstance(value, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", value):
                raise PipelineError("INVALID_SECRET_ENV_NAME")
        if self.handoff_bucket is not None and (not isinstance(self.handoff_bucket, str)
                or not re.fullmatch(r'[A-Za-z0-9_.-]+', self.handoff_bucket)):
            raise PipelineError('INVALID_HANDOFF_BUCKET')
        if type(self.lease_seconds) is not int or self.lease_seconds < 60:
            raise PipelineError("LEASE_TOO_SHORT")
        if type(self.max_attempts) is not int or not 2 <= self.max_attempts <= 3:
            raise PipelineError("INVALID_ATTEMPT_LIMIT")

    @property
    def fingerprint(self):
        return digest({k: v for k, v in asdict(self).items() if k in {
            "environment_id", "mongo_database", "mongo_replica_set", "image_bucket", "kafka_topic"}} | {"contract_version": 1})

    @classmethod
    def load(cls, path):
        try:
            return cls(**read_json(path))
        except TypeError as exc:
            raise PipelineError("INVALID_CONFIG_KEYS") from exc
