"""Read-only check that a collected image is already in the pipeline's object storage.

The collector only decides whether to download again: a HEAD with the object's size and
its sha256 metadata. The pipeline's ingestion still reads the object and verifies its full
sha256 before the image becomes AVAILABLE, so this check never vouches for integrity.

The settings file is a Java-properties style key=value file kept outside Git:
endpoint, bucket, accessKey, secretKey, and optionally caBundle and region.
"""
import os
import stat
from pathlib import Path

KEYS = {"endpoint", "bucket", "accessKey", "secretKey", "caBundle", "region"}
REQUIRED = {"endpoint", "bucket", "accessKey", "secretKey"}
NOT_FOUND = {"NoSuchKey", "NotFound", "404"}


def object_key(meta: dict) -> str:
    """The pipeline's content address (product_pipeline.snapshot)."""
    return f"images/sha256/{meta['sha256'][:2]}/{meta['sha256']}.{meta['extension']}"


def read_store_config(path: Path) -> dict:
    path = Path(path)
    if os.name == "posix" and stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError(f"image store settings must not be readable by others (chmod 600): {path}")
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key not in KEYS:
            continue
        if not separator or not value or key in values:
            raise ValueError(f"invalid image store settings line: {key}")
        values[key] = value
    missing = REQUIRED - set(values)
    if missing:
        raise ValueError(f"image store settings missing: {', '.join(sorted(missing))}")
    if not values["endpoint"].startswith("https://"):
        raise ValueError("image store endpoint must be https")
    return values


def describe(config: dict) -> dict:
    """What a run fixes in its settings: never the key pair."""
    return {"endpoint": config["endpoint"], "bucket": config["bucket"]}


class S3ImageStore:
    """HEAD-only client for a Garage (S3 v4, path style) image bucket."""

    def __init__(self, config: dict, client=None):
        self.bucket = config["bucket"]
        if client is None:
            import boto3
            from botocore.config import Config
            options = Config(signature_version="s3v4", s3={"addressing_style": "path"},
                             connect_timeout=5, read_timeout=10,
                             retries={"total_max_attempts": 1, "mode": "standard"},
                             request_checksum_calculation="when_required",
                             response_checksum_validation="when_required")
            client = boto3.client("s3", endpoint_url=config["endpoint"], config=options,
                                  region_name=config.get("region", "us-east-1"),
                                  verify=config.get("caBundle", True),
                                  aws_access_key_id=config["accessKey"],
                                  aws_secret_access_key=config["secretKey"])
        self.client = client

    def head(self, key: str) -> dict | None:
        """Size and sha256 metadata of the object, None when it does not exist."""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in NOT_FOUND:
                return None
            raise
        return {"bytes": response.get("ContentLength"),
                "sha256": (response.get("Metadata") or {}).get("sha256")}

    def close(self):
        close = getattr(self.client, "close", None)
        if close:
            close()
