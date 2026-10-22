"""Primary/majority ledger, fenced leases and INSERT-only raw observations."""
from contextlib import contextmanager
from datetime import datetime, timezone
import threading
from uuid import uuid4

from .contracts import digest, envelope, event_for, now, registration_identity, utc
from .errors import Conflict, LeaseLost, PipelineError

MAX_BSON = 16 * 1024 * 1024


def bson_document(doc):
    from bson import ObjectId
    return {**doc, "_id": ObjectId(doc["_id"]),
            "collected_at": datetime.fromisoformat(doc["collected_at"].replace("Z", "+00:00"))}


def wire_document(doc):
    return {**doc, "_id": str(doc["_id"]), "collected_at": utc(doc["collected_at"].isoformat())}


def validate_size(doc, wire_document=True):
    from bson import BSON
    from bson.errors import InvalidDocument
    value = bson_document(doc) if wire_document else doc
    try:
        size = len(BSON.encode(value))
    except (InvalidDocument, OverflowError) as exc:
        raise PipelineError("BSON_UNENCODABLE_INPUT") from exc
    if size >= MAX_BSON:
        raise PipelineError("RAW_TOO_LARGE" if wire_document else "REGISTRATION_TOO_LARGE")


class MongoLedger:
    def __init__(self, settings, uri, client=None):
        from pymongo import MongoClient, ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        self.settings, self.owner = settings, str(uuid4())
        self.heartbeats, self.lost_leases = {}, set()
        self.client = client or MongoClient(uri, replicaSet=settings.mongo_replica_set, retryWrites=False,
            tz_aware=True, appname="danawa-data-pipeline", connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000, socketTimeoutMS=30000)
        hello = self.client.admin.command("hello")
        if hello.get("setName") != settings.mongo_replica_set or len(hello.get("hosts", [])) != 3 or hello.get("arbiters"):
            self.client.close()
            raise PipelineError("REPLICA_SET_CONTRACT_REQUIRED")
        self.db = self.client.get_database(settings.mongo_database, read_preference=ReadPreference.PRIMARY,
            read_concern=ReadConcern("majority"), write_concern=WriteConcern(w="majority", j=True, wtimeout=30000))
        self.observations = self.db["ingestion_observations"]
        self.raw = self.db["raw_products"]

    @contextmanager
    def exclusive(self):
        from pymongo.errors import DuplicateKeyError
        self.observations.create_index("observation_key", unique=True)
        self.observations.create_index([("stage", 1), ("next_retry_at", 1)])
        self.raw.create_index("observation_key", unique=True)
        self.raw.create_index([("source_code", 1), ("source_product_id", 1), ("collected_at", 1)])
        expected = {"_id": "target:" + self.settings.environment_id,
                    "target_fingerprint": self.settings.fingerprint}
        try:
            self.db.ingestion_snapshots.insert_one(expected)
        except DuplicateKeyError:
            pass
        if self.db.ingestion_snapshots.find_one({"_id": expected["_id"]}) != expected:
            raise Conflict("TARGET_MISMATCH")
        yield self

    def get(self, key):
        found = self.observations.find_one({"observation_key": key})
        if found is None:
            raise PipelineError("REGISTRATION_UNCONFIRMED")
        return found

    def register(self, row, snapshot, archived=None):
        from bson import ObjectId
        from pymongo.errors import ConnectionFailure, DuplicateKeyError, ExecutionTimeout, WriteConcernError
        identity = registration_identity(row)
        found = self.observations.find_one({"observation_key": row["observation_key"]})
        if found is None:
            if archived is None and self.raw.find_one({"observation_key": row["observation_key"]}) is not None:
                raise PipelineError("REGISTRATION_EVIDENCE_MISSING")
            doc = archived["document"] if archived else envelope(row, str(ObjectId()), snapshot["snapshot_id"], self.settings.image_bucket)
            validate_size(doc)
            candidate = {"_id": row["observation_key"], "observation_key": row["observation_key"],
                         "target_fingerprint": self.settings.fingerprint, "identity": identity,
                         "document": doc, "event": archived["event"] if archived else event_for(doc, str(uuid4()), now()),
                         "stage": "OBSERVATION_REGISTERED", "lease_token": 0,
                         "image_state_revision": 0, "image_state_vector": [], "image_state_fingerprint": digest([])}
            validate_size(candidate, wire_document=False)
            try:
                self.observations.insert_one(candidate)
            except DuplicateKeyError:
                pass
            except (ConnectionFailure, ExecutionTimeout, WriteConcernError):
                if self.observations.find_one({"observation_key": row["observation_key"]}) is None:
                    raise
            found = self.get(row["observation_key"])
        if found["identity"] != identity or found["target_fingerprint"] != self.settings.fingerprint:
            raise Conflict("OBSERVATION_CONFLICT")
        return found

    def acquire(self, key):
        from pymongo import ReturnDocument
        row = self.observations.find_one_and_update({"observation_key": key, "$expr": {"$lte": [
            {"$ifNull": ["$lease_until", datetime(1970, 1, 1, tzinfo=timezone.utc)]}, "$$NOW"]}},
            [{"$set": {"lease_owner": self.owner,
                "lease_token": {"$add": [{"$ifNull": ["$lease_token", 0]}, 1]},
                "lease_until": {"$dateAdd": {"startDate": "$$NOW", "unit": "second", "amount": self.settings.lease_seconds}}}}],
            return_document=ReturnDocument.AFTER)
        if row is None:
            raise PipelineError("LEASE_BUSY")
        token = row["lease_token"]
        stop = threading.Event()
        def beat():
            while not stop.wait(min(30, self.settings.lease_seconds / 3)):
                try:
                    self.renew(key, token)
                except Exception:
                    self.lost_leases.add((key, token))
                    break
        thread = threading.Thread(target=beat, daemon=True, name="pipeline-lease")
        self.heartbeats[key, token] = stop, thread
        thread.start()
        return row["lease_token"]

    def _filter(self, key, token):
        if (key, token) in self.lost_leases:
            raise LeaseLost()
        return {"observation_key": key, "lease_owner": self.owner, "lease_token": token,
                "$expr": {"$gt": ["$lease_until", "$$NOW"]}}

    def renew(self, key, token):
        if (key, token) in self.lost_leases:
            raise LeaseLost()
        result = self.observations.update_one(self._filter(key, token), [{"$set": {
            "lease_until": {"$dateAdd": {"startDate": "$$NOW", "unit": "second", "amount": self.settings.lease_seconds}}}}])
        if result.matched_count != 1:
            raise LeaseLost()

    def update(self, key, token, **fields):
        if set(fields) - {"stage", "last_error", "raw_confirmed", "event_confirmed"}:
            raise PipelineError("IMMUTABLE_FIELD_UPDATE")
        if fields.get('raw_confirmed') or fields.get('event_confirmed'):
            fields['incremental_change']=dict(reason='SOURCE_CONFIRMED')
        result = self.observations.update_one(self._filter(key, token), {"$set": fields})
        if result.matched_count != 1:
            raise LeaseLost()

    def images(self, key, token, vector):
        row = self.get(key)
        fingerprint = digest(vector)
        where = {**self._filter(key, token), "image_state_revision": row["image_state_revision"]}
        fields = {"image_state_vector": vector, "image_state_fingerprint": fingerprint,
                  "image_state_revision": row["image_state_revision"] + (fingerprint != row["image_state_fingerprint"]),
                  "image_state_checked_at": now()}
        if fingerprint != row['image_state_fingerprint']:
            fields['incremental_change']=dict(reason='IMAGE_STATE',revision=fields['image_state_revision'],fingerprint=fingerprint)
        # Check the final operational document, not just the vector in isolation.
        validate_size({**row, **fields}, wire_document=False)
        result = self.observations.update_one(where, {"$set": fields})
        if result.matched_count != 1:
            raise LeaseLost()

    def release(self, key, token):
        heartbeat = self.heartbeats.pop((key, token), None)
        if heartbeat:
            heartbeat[0].set()
            heartbeat[1].join(timeout=35)
        self.observations.update_one({"observation_key": key, "lease_owner": self.owner, "lease_token": token},
                                    {"$unset": {"lease_owner": "", "lease_until": ""}})

    def ensure_raw(self, doc):
        from pymongo.errors import ConnectionFailure, DuplicateKeyError, ExecutionTimeout, WriteConcernError
        from bson import ObjectId
        validate_size(doc)
        existing = self.raw.find_one({"_id": ObjectId(doc["_id"])})
        if existing is not None:
            if wire_document(existing) != doc:
                raise Conflict("RAW_DOCUMENT_CONFLICT")
            return False
        inserted = True
        try:
            self.raw.insert_one(bson_document(doc))
        except DuplicateKeyError:
            inserted = False
        except (ConnectionFailure, ExecutionTimeout, WriteConcernError):
            # Majority readback reconciles an unknown write without issuing another insert.
            if self.raw.find_one({"_id": ObjectId(doc["_id"])}) is None:
                raise
            inserted = False  # an unknown acknowledgement is not a confirmed new write
        stored = self.raw.find_one({"_id": ObjectId(doc["_id"])})
        if stored is None:
            raise PipelineError("RAW_UNCONFIRMED")
        if wire_document(stored) != doc:
            raise Conflict("RAW_DOCUMENT_CONFLICT")
        return inserted

    def attempt(self, key, stage, status, execution_id, duration_ms, error=None):
        self.db.ingestion_attempts.insert_one({"_id": str(uuid4()), "observation_key": key,
            "stage": stage, "status": status, "execution_id": execution_id, "ingestion_run_id": self.owner, "duration_ms": duration_ms,
            "error_code": error, "recorded_at": now()})

    def snapshot(self, manifest):
        from pymongo.errors import DuplicateKeyError
        # Large file manifests remain in sealed input storage; keep a bounded DB receipt.
        receipt = {"_id": manifest["snapshot_id"], "environment_id": self.settings.environment_id,
                   "snapshot_id": manifest["snapshot_id"], "manifest_hash": manifest["snapshot_id"],
                   "collection_run_id": manifest["collection_run_id"], "product_count": manifest["product_count"]}
        try:
            self.db.ingestion_snapshots.insert_one(receipt)
        except DuplicateKeyError:
            pass
        if self.db.ingestion_snapshots.find_one({"_id": receipt["_id"]}) != receipt:
            raise Conflict("SNAPSHOT_RECEIPT_CONFLICT")

    def close(self):
        for stop, thread in self.heartbeats.values():
            stop.set()
        for stop, thread in self.heartbeats.values():
            thread.join(timeout=35)
        self.client.close()
