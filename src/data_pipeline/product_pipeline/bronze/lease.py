"""Fail-closed Driver ownership in a separate Mongo operations collection.

Expiry fences the current Driver but NEVER permits automatic takeover. Delta/S3
cannot revoke an in-flight commit. An expired or uncertain claim must stay put
until an operator has proved the old Driver and its Spark work have terminated.
Do not configure a TTL index on this collection. All deployments sharing a lake
must use the same operations namespace and canonical target URIs.
"""
from threading import Event, RLock, Thread
from uuid import uuid4
import time

from ..contracts import digest,now
from ..errors import PipelineError
from .config import location

RECOVERY_ERRORS=frozenset({'BRONZE_WRITER_LEASE_LOST','BRONZE_WRITER_ALREADY_CLAIMED',
    'BRONZE_WRITER_LEASE_UNCONFIRMED','BRONZE_SHUTDOWN_UNCONFIRMED',
    'BRONZE_KAFKA_OFFSETS_OUT_OF_RANGE','BRONZE_KAFKA_PARTITIONS_CHANGED',
    'BRONZE_CHECKPOINT_OFFSETS_UNCONFIRMED','BRONZE_KAFKA_TOPIC_CHANGED'})


class MongoLease:
    def __init__(self, settings, uri, collection=None):
        self.settings, self.token = settings, str(uuid4())
        self.client = None
        self._lock, self._stop = RLock(), Event()
        self._thread = None
        self._acquired, self._lost, self._closed = False, False, False
        self._claimed = []
        self.loss_diagnostic = None
        self._targets = sorted({location(getattr(settings, name)) for name in (
            'bronze_path', 'events_path', 'quarantine_path', 'checkpoint_path')})
        if getattr(settings,'checkpoint_recovery',None):
            from .recovery import checkpoint_path
            self._targets=sorted({*self._targets,location(checkpoint_path(settings))})
        if collection is None:
            from pymongo import MongoClient, ReadPreference
            from pymongo.read_concern import ReadConcern
            from pymongo.write_concern import WriteConcern
            self.client = MongoClient(uri, replicaSet=settings.mongo_replica_set,
                retryWrites=False, tz_aware=True, appname='danawa-bronze-lease',
                # The client must allow the server's 15s majority/journal wait
                # to return its response, including network/scheduling delay.
                connectTimeoutMS=5000, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000)
            database = self.client.get_database(settings.lease_database,
                read_preference=ReadPreference.PRIMARY, read_concern=ReadConcern('majority'),
                write_concern=WriteConcern(w='majority', j=True, wtimeout=15000))
            collection = database[settings.lease_collection]
        self.collection = collection

    def _update(self):
        return [{'$set': {'token': self.token, 'target_fingerprint': self.settings.fingerprint,
            'heartbeat_at': '$$NOW',
            'expires_at': {'$add': ['$$NOW', self.settings.lease_seconds * 1000]}}}]

    def _diagnostic(self,exc,phase,key,started):
        # SDK messages/details may contain credentials; expose only fixed
        # categories, exception class and numeric server codes.
        if self.loss_diagnostic is not None:return
        code=getattr(exc,'code',None)
        self.loss_diagnostic=dict(phase=phase,target_key=key,exception_type=type(exc).__name__,
            server_code=code if type(code) is int else None,
            reason='OWNER_OR_EXPIRY_MISMATCH' if isinstance(exc,PipelineError) else 'DATABASE_OPERATION_FAILED',
            duration_ms=round((time.monotonic()-started)*1000),recorded_at=now())

    def acquire(self):
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError
        with self._lock:
            if self._acquired or self._closed or self._lost:
                raise PipelineError('BRONZE_WRITER_LEASE_LOST')
            try:
                indexes = list(self.collection.list_indexes())
            except Exception:
                raise PipelineError('BRONZE_LEASE_COLLECTION_UNCONFIRMED') from None
            if any('expireAfterSeconds' in index for index in indexes):
                raise PipelineError('BRONZE_LEASE_TTL_FORBIDDEN')
            started=time.monotonic();key=None
            try:
                for target in self._targets:
                    key = 'bronze-path:' + digest(target)
                    update = self._update()
                    update[0]['$set']['target_uri'] = target
                    # The unique _id index serializes all hosts. Token mismatch
                    # causes duplicate-key, even if an old heartbeat has expired.
                    self.collection.find_one_and_update({'_id': key, 'token': self.token},
                        update, upsert=True, return_document=ReturnDocument.AFTER)
                    self._claimed.append(key)
            except DuplicateKeyError as exc:
                self._diagnostic(exc,'acquire',key,started)
                self._release_claims()  # No target has been touched by this Driver.
                raise PipelineError('BRONZE_WRITER_ALREADY_CLAIMED') from None
            except Exception as exc:
                # A timed-out majority write may have succeeded: preserve all
                # claims for manual recovery instead of guessing it was absent.
                self._lost = True
                self._diagnostic(exc,'acquire',key,started)
                raise PipelineError('BRONZE_WRITER_LEASE_UNCONFIRMED') from None
            self._acquired = True
        return self

    def check(self, settings=None):
        """Renew and verify every path using Mongo server time, never host time."""
        from pymongo import ReturnDocument
        with self._lock:
            if settings is not None and settings.fingerprint != self.settings.fingerprint:
                raise PipelineError('BRONZE_WRITER_TARGET_CONFLICT')
            if not self._acquired or self._lost or self._closed:
                raise PipelineError('BRONZE_WRITER_LEASE_LOST')
            started=time.monotonic();key=None
            try:
                for key in self._claimed:
                    found = self.collection.find_one_and_update({'_id': key, 'token': self.token,
                        '$expr': {'$gt': ['$expires_at', '$$NOW']}}, self._update(),
                        return_document=ReturnDocument.AFTER)
                    if found is None:
                        raise PipelineError('BRONZE_WRITER_LEASE_LOST')
            except Exception as exc:
                self._lost = True
                self._diagnostic(exc,'renew',key,started)
                raise PipelineError('BRONZE_WRITER_LEASE_LOST') from None

    def start_heartbeat(self, on_lost):
        self.check()

        def heartbeat():
            while not self._stop.wait(self.settings.lease_heartbeat_seconds):
                try:
                    self.check()
                except PipelineError:
                    try:
                        on_lost()
                    except Exception:
                        # The lease remains permanently lost; never leak SDK
                        # exception text from a background-thread traceback.
                        pass
                    return

        self._thread = Thread(target=heartbeat, name='bronze-lease-heartbeat', daemon=True)
        self._thread.start()

    def check_s3_bucket(self, bucket):
        """Garage S3SingleDriverLogStore requires the existing latched ownership."""
        from urllib.parse import urlsplit
        if not self._targets or any(urlsplit(target).scheme != 's3a'
                or urlsplit(target).netloc != bucket for target in self._targets):
            raise PipelineError('GARAGE_WRITER_TARGET_MISMATCH')
        self.check()

    def _release_claims(self):
        for key in self._claimed:
            self.collection.delete_one({'_id': key, 'token': self.token})
        self._claimed.clear()

    def close(self, release=False):
        """Release only after the caller confirms all query/Spark work stopped.

        Lost ownership is always left latched for operator recovery.
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
        with self._lock:
            try:
                if release and not self._lost and not self._closed:
                    self._release_claims()
            finally:
                self._closed = True
                if self.client is not None:
                    self.client.close()
