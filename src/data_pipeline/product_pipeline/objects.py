"""Conditional S3 writes or strictly content-addressed Garage application writes.

Garage mode never writes an arbitrary key. All participating writers must use
this binding; this is application immutability, not a bucket WORM policy.
"""
import hashlib
import json
import os
import re
from .contracts import canonical, digest
from .errors import Conflict, PipelineError


class S3Objects:
    def __init__(self, bucket, endpoint=None, client=None, *, profile='conditional', credentials_file=None):
        if profile not in {'conditional', 'garage_v241'}:
            raise PipelineError('S3_PROFILE_INVALID')
        if profile == 'garage_v241' and client is None and (not endpoint or not endpoint.startswith('https://')):
            raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
        self.bucket, self.profile = bucket, profile
        if client is None:
            import boto3
            from botocore.config import Config
            options = dict(connect_timeout=5, read_timeout=30,
                           retries={"total_max_attempts": 1, "mode": "standard"})
            extra = {}
            if credentials_file:
                from .s3_credentials import service_credentials
                extra.update(service_credentials(credentials_file))
            if profile == 'garage_v241':
                options.update(signature_version='s3v4', s3={'addressing_style': 'path'},
                               request_checksum_calculation='when_required', response_checksum_validation='when_required')
                extra['region_name'] = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
            client = boto3.client('s3', endpoint_url=endpoint, config=Config(**options), **extra)
        self.client = client

    @classmethod
    def from_settings(cls, settings, endpoint, *, bucket=None):
        name = settings.s3_credentials_file_env
        credentials_file = os.environ.get(name) if name else None
        if name and not credentials_file:
            raise PipelineError('S3_SERVICE_CREDENTIALS_ENV_MISSING')
        return cls(bucket or settings.image_bucket, None if endpoint == 'aws' else endpoint,
                   profile=settings.s3_profile, credentials_file=credentials_file)

    @staticmethod
    def _content_address(key, data, checksum):
        """Prove that any valid writer of this key must supply the same bytes."""
        if not isinstance(key, str):
            raise Conflict('OBJECT_KEY_CONTENT_MISMATCH')
        image = re.fullmatch(r'images/sha256/([0-9a-f]{2})/([0-9a-f]{64})\.([a-z0-9]{1,12})', key)
        chunk = re.fullmatch(r'handoff/chunks/sha256/([0-9a-f]{64})', key)
        ready = re.fullmatch(r'handoff/ready/([0-9a-f]{64})\.ready\.json', key)
        if image and image[1] == checksum[:2] and image[2] == checksum:
            return
        if chunk and chunk[1] == checksum:
            return
        if ready and len(data) <= 16 * 1024 * 1024:
            try:
                value = json.loads(data)
                stable = {k: v for k, v in value.items() if k != 'transport_id'}
                if (value.get('transport_schema_version') == 1 and value.get('transport_id') == ready[1]
                        and digest(stable) == ready[1] and canonical(value).encode('utf-8') == data):
                    return
            except (ValueError, TypeError, AttributeError, RecursionError):
                pass
        raise Conflict('OBJECT_KEY_CONTENT_MISMATCH')

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
        if self.profile == 'garage_v241':
            self._content_address(key, data, checksum)  # Reject before any external request.
        if self._exists(key, checksum, size):
            return False
        try:
            request = dict(Bucket=self.bucket, Key=key, Body=data, Metadata={'sha256': checksum})
            if self.profile == 'conditional':
                request['IfNoneMatch'] = '*'
            self.client.put_object(**request)
        except Exception as exc:
            if self.profile == 'garage_v241':
                # An acknowledgement may be lost. Read-back, never blindly retry.
                if self._exists(key, checksum, size):
                    return True
                raise PipelineError('OBJECT_WRITE_UNCONFIRMED') from None
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                raise PipelineError("OBJECT_WRITE_UNCONFIRMED") from exc
        if not self._exists(key, checksum, size):
            raise PipelineError("OBJECT_WRITE_UNCONFIRMED")
        return True
