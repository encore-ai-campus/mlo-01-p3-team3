"""Persisted rehearsal adapters. They do not simulate replication or Broker durability."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
from uuid import uuid4

from .contracts import digest, envelope, event_for, now, registration_identity
from .errors import Conflict, LeaseLost, PipelineError
from .files import atomic_bytes, atomic_json, confined, lock, read_json, sha_file


class FileLedger:
    def __init__(self, directory, settings):
        self.directory, self.settings = Path(directory).resolve(), settings
        self.owner = str(uuid4())

    @contextmanager
    def exclusive(self):
        with lock(self.directory / "writer.lock"):
            marker = self.directory / "target.json"
            expected = {"mode": "rehearsal", "target_fingerprint": self.settings.fingerprint}
            if marker.exists() and read_json(marker) != expected:
                raise Conflict("TARGET_MISMATCH")
            atomic_json(marker, expected)
            yield self

    def _path(self, key):
        return confined(self.directory, f"observations/{key}.json")

    def get(self, key):
        return read_json(self._path(key))

    def register(self, row, snapshot, archived=None):
        key, path = row["observation_key"], self._path(row["observation_key"])
        if path.exists():
            saved = read_json(path)
            if saved["identity"] != registration_identity(row):
                raise Conflict("OBSERVATION_CONFLICT")
            return saved
        if archived is not None:
            saved = {k: archived[k] for k in ("observation_key", "target_fingerprint", "identity", "document", "event")}
            saved.update(stage="OBSERVATION_REGISTERED", lease_token=0, image_state_revision=0,
                         image_state_vector=[], image_state_fingerprint=digest([]))
            atomic_json(path, saved)
            return saved
        doc = envelope(row, secrets.token_hex(12), snapshot["snapshot_id"], self.settings.image_bucket)
        from .mongo import validate_size
        validate_size(doc)
        saved = {"observation_key": key, "target_fingerprint": self.settings.fingerprint,
                 "identity": registration_identity(row),
                 "document": doc, "event": event_for(doc, str(uuid4()), now()),
                 "stage": "OBSERVATION_REGISTERED", "lease_token": 0,
                 "image_state_revision": 0, "image_state_vector": [], "image_state_fingerprint": digest([])}
        validate_size(saved, wire_document=False)
        atomic_json(path, saved)
        return saved

    def acquire(self, key):
        row = self.get(key)
        until = row.get("lease_until")
        if until and datetime.fromisoformat(until.replace("Z", "+00:00")) > datetime.now(timezone.utc):
            raise PipelineError("LEASE_BUSY")
        row.update(lease_owner=self.owner, lease_token=row["lease_token"] + 1,
                   lease_until=(datetime.now(timezone.utc) + timedelta(seconds=self.settings.lease_seconds)).isoformat())
        atomic_json(self._path(key), row)
        return row["lease_token"]

    def _owned(self, key, token):
        row = self.get(key)
        if (row.get("lease_owner") != self.owner or row.get("lease_token") != token
                or datetime.fromisoformat(row["lease_until"].replace("Z", "+00:00")) <= datetime.now(timezone.utc)):
            raise LeaseLost()
        return row

    def renew(self, key, token):
        row = self._owned(key, token)
        row["lease_until"] = (datetime.now(timezone.utc) + timedelta(seconds=self.settings.lease_seconds)).isoformat()
        atomic_json(self._path(key), row)

    def update(self, key, token, **fields):
        row = self._owned(key, token)
        if set(fields) - {"stage", "last_error", "raw_confirmed", "event_confirmed", "image_state_vector", "image_state_fingerprint", "image_state_revision", "image_state_checked_at"}:
            raise PipelineError("IMMUTABLE_FIELD_UPDATE")
        row.update(fields)
        atomic_json(self._path(key), row)

    def images(self, key, token, vector):
        row = self._owned(key, token)
        fingerprint = digest(vector)
        self.update(key, token, image_state_vector=vector, image_state_fingerprint=fingerprint,
                    image_state_revision=row["image_state_revision"] + (fingerprint != row["image_state_fingerprint"]),
                    image_state_checked_at=now())

    def release(self, key, token):
        row = self.get(key)
        if row.get("lease_owner") == self.owner and row.get("lease_token") == token:
            row.pop("lease_owner", None)
            row.pop("lease_until", None)
            atomic_json(self._path(key), row)

    def ensure_raw(self, doc):
        path = confined(self.directory, f"raw_products/{doc['_id']}.json")
        if path.exists():
            if read_json(path) != doc:
                raise Conflict("RAW_DOCUMENT_CONFLICT")
            return False
        atomic_json(path, doc)
        return True

    def attempt(self, key, stage, status, execution_id, duration_ms, error=None):
        atomic_json(self.directory / "attempts" / (uuid4().hex + ".json"), {
            "observation_key": key, "stage": stage, "status": status, "execution_id": execution_id,
            "ingestion_run_id": self.owner, "duration_ms": duration_ms, "error_code": error, "recorded_at": now()})

    def snapshot(self, manifest):
        atomic_json(self.directory / "snapshots" / (manifest["snapshot_id"] + ".json"), manifest)


class FileObjects:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.writes = 0

    def ensure(self, key, source, checksum, size):
        import hashlib
        data = source.read_bytes()
        if len(data) != size or hashlib.sha256(data).hexdigest() != checksum:
            raise Conflict("IMAGE_SOURCE_CHANGED")
        target = confined(self.directory, key)
        if target.exists():
            if target.stat().st_size != size or sha_file(target) != checksum:
                raise Conflict("OBJECT_CONTENT_CONFLICT")
            return False
        atomic_bytes(target, data)
        self.writes += 1
        return True


class FilePublisher:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.calls = 0

    def send(self, event, key):
        self.calls += 1
        path = confined(self.directory, event["event_id"] + ".json")
        body = {"key": key, "event": event}
        if path.exists() and read_json(path) != body:
            raise Conflict("EVENT_BODY_CONFLICT")
        atomic_json(path, body)

    def close(self):
        pass
