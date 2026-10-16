"""Immutable S3 object adapter. ETag is never interpreted as SHA256."""
import hashlib
from .errors import Conflict, PipelineError


class S3Objects:
    def __init__(self, bucket, endpoint=None, client=None):
        self.bucket = bucket
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client("s3", endpoint_url=endpoint, config=Config(
                connect_timeout=5, read_timeout=30, retries={"total_max_attempts": 1, "mode": "standard"}))
        self.client = client

    def preflight(self):
        self.client.head_bucket(Bucket=self.bucket)

    def _exists(self, key, checksum, size):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "NotFound", "404"}:
                return False
            if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403"}:
                raise PipelineError("OBJECT_PERMISSION_DENIED") from exc
            raise PipelineError("OBJECT_READ_FAILED") from exc
        h, count, body = hashlib.sha256(), 0, response["Body"]
        try:
            while chunk := body.read(64 * 1024):
                count += len(chunk)
                if count > size:
                    raise Conflict("OBJECT_CONTENT_CONFLICT")
                h.update(chunk)
        finally:
            body.close()
        if count != size or h.hexdigest() != checksum:
            raise Conflict("OBJECT_CONTENT_CONFLICT")
        return True

    def ensure(self, key, source, checksum, size):
        data = source.read_bytes()
        if len(data) != size or hashlib.sha256(data).hexdigest() != checksum:
            raise Conflict("IMAGE_SOURCE_CHANGED")
        if self._exists(key, checksum, size):
            return False
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, IfNoneMatch="*", Metadata={"sha256": checksum})
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                raise PipelineError("OBJECT_WRITE_UNCONFIRMED") from exc
        if not self._exists(key, checksum, size):
            raise PipelineError("OBJECT_WRITE_UNCONFIRMED")
        return True
