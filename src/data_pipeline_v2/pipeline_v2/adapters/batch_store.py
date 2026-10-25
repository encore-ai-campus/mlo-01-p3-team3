"""Durable control records and immutable source snapshots, portable file/Mongo backends."""
from contextlib import nullcontext
from functools import wraps
from pathlib import Path
from .files import read, write, immutable, locked
from ..domain.contracts import digest, seal, verified, Invalid

# Restricted quarantine copies have a retention deadline; every other record kind is append-only.
DELETABLE = frozenset({'quarantine_raw'})


class CommandCounter:
    """pymongo command monitor: wire commands and their client-observed round trip, no extra calls."""
    def __init__(self):
        self.values = dict(mongo_commands=0, mongo_round_trip_us=0, mongo_failed_commands=0)

    def succeeded(self, event):
        self.values['mongo_commands'] += 1
        self.values['mongo_round_trip_us'] += event.duration_micros

    def failed(self, event):
        self.values['mongo_commands'] += 1
        self.values['mongo_failed_commands'] += 1
        self.values['mongo_round_trip_us'] += event.duration_micros


def measured(operation):
    """Span per store call (method, record kind) carrying the commands it issued."""
    def wrap(method):
        @wraps(method)
        def run(self, kind, *args, **kwargs):
            timing = self.timing
            span = (timing.measure('mongo.' + operation + '.' + kind, timing.active_scope or 'mongo',
                counters=self.monitor.values) if timing else nullcontext())
            with span:
                return method(self, kind, *args, **kwargs)
        return run
    return wrap


class FileBatchStore:
    def __init__(self, root):
        self.root = Path(root).resolve() / 'batches-v1'
        self.identity = dict(kind='FILE_BATCH_STORE', root=str(self.root))

    def _path(self, kind, key):
        if not kind.replace('_', '').isalnum():
            raise Invalid('BATCH_RECORD_KIND_INVALID')
        return self.root / kind / (digest(key) + '.json')

    def get(self, kind, key):
        path = self._path(kind, key)
        return verified(read(path))['value'] if path.exists() else None

    def put(self, kind, key, value):
        with locked(self._path(kind, key).with_suffix('.lock')):
            immutable(self._path(kind, key), seal(dict(value=value)))
        return value

    def mutate(self, kind, key, change):
        with locked(self._path(kind, key).with_suffix('.lock')):
            result = change(self.get(kind, key))
            write(self._path(kind, key), seal(dict(value=result)))
            return result

    def put_many(self, kind, items):
        for key, value in items:
            self.put(kind, key, value)

    def values(self, kind):
        for path in sorted((self.root / kind).glob('*.json')):
            yield verified(read(path))['value']

    def get_many(self, kind, keys):
        return {key:self.get(kind,key) for key in keys}

    def get_list(self, kind, keys):
        return [self.get(kind,key) for key in keys]

    def delete(self, kind, key):
        # Only retention-governed restricted records are deleted; control records are immutable.
        if kind not in DELETABLE:
            raise Invalid('BATCH_RECORD_NOT_DELETABLE')
        with locked(self._path(kind, key).with_suffix('.lock')):
            self._path(kind, key).unlink(missing_ok=True)

    def close(self):
        pass


class MongoBatchStore:
    # Set by the bootstrap once its Timing exists; None keeps the store usable without metrics.
    timing = None

    def __init__(self, uri, database, replica_set):
        from pymongo import MongoClient, ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        from pymongo import monitoring

        counter = self.monitor = CommandCounter()

        class Listener(monitoring.CommandListener):
            def started(self, event):
                pass

            def succeeded(self, event):
                counter.succeeded(event)

            def failed(self, event):
                counter.failed(event)
        # Wire compression: source snapshots are large JSON. zstd when its module is installed,
        # else the built-in zlib; the stored documents are unchanged.
        import importlib.util
        compressors = ','.join((['zstd'] if importlib.util.find_spec('zstandard') else []) + ['zlib'])
        self.client = MongoClient(uri, replicaSet=replica_set, tz_aware=True, compressors=compressors,
            serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=True, event_listeners=[Listener()])
        self.database = self.client.get_database(database, read_preference=ReadPreference.PRIMARY,
            read_concern=ReadConcern('majority'), write_concern=WriteConcern(w='majority', j=True))
        self.identity = dict(kind='MONGO_BATCH_STORE', database=database, replica_set=replica_set)

    def _key(self, kind, key):
        return kind + ':' + digest(key)

    def _collection(self, kind):
        if not kind.replace('_','').isalnum():
            raise Invalid('BATCH_RECORD_KIND_INVALID')
        # Keep small control scans separate from large immutable source snapshots.
        return self.database['pipeline_v2_' + kind]

    @measured('get')
    def get(self, kind, key):
        import json
        record = self._collection(kind).find_one({'_id':self._key(kind, key)})
        return verified(json.loads(record['body_json']))['value'] if record else None

    @measured('put')
    def put(self, kind, key, value):
        from ..domain.contracts import canonical
        from pymongo.errors import DuplicateKeyError
        record = dict(_id=self._key(kind, key), kind=kind, revision=0,
                      body_json=canonical(seal(dict(value=value))))
        try:
            self._collection(kind).insert_one(record)
        except DuplicateKeyError:
            if self.get(kind, key) != value:
                raise Invalid('IMMUTABLE_BATCH_RECORD_CONFLICT')
        return value

    @measured('put_many')
    def put_many(self, kind, items):
        """One unordered bulk insert; an existing key must already hold the same value."""
        from ..domain.contracts import canonical
        from pymongo.errors import BulkWriteError
        items = list(items)
        if not items:
            return
        records = [dict(_id=self._key(kind, key), kind=kind, revision=0, body_json=canonical(seal(dict(value=value))))
                   for key, value in items]
        try:
            self._collection(kind).insert_many(records, ordered=False)
        except BulkWriteError as exc:
            errors = exc.details.get('writeErrors', [])
            # A write concern error leaves the inserted rows unconfirmed (they may roll back), so
            # it always fails the call even when the other errors are only identical duplicates.
            if exc.details.get('writeConcernErrors') or not errors or any(e.get('code') != 11000 for e in errors):
                raise
            duplicates = {e['op']['_id'] for e in errors}
            # Keys may be lists (composite keys), so compare by position, never as dict keys.
            repeated = [(key, value) for key, value in items if self._key(kind, key) in duplicates]
            existing = self.get_list(kind, [key for key, _ in repeated])
            for (key, value), stored in zip(repeated, existing):
                if stored != value:
                    raise Invalid('IMMUTABLE_BATCH_RECORD_CONFLICT')

    @measured('mutate')
    def mutate(self, kind, key, change):
        import json
        from ..domain.contracts import canonical
        from pymongo.errors import DuplicateKeyError
        identity = self._key(kind, key)
        for _ in range(20):
            old = self._collection(kind).find_one({'_id':identity})
            result = change(verified(json.loads(old['body_json']))['value'] if old else None)
            body = canonical(seal(dict(value=result)))
            if old:
                updated = self._collection(kind).update_one({'_id':identity,'revision':old['revision']},
                    {'$set':{'body_json':body},'$inc':{'revision':1}})
                if updated.modified_count:
                    return result
            else:
                try:
                    self._collection(kind).insert_one(dict(_id=identity,kind=kind,revision=0,body_json=body))
                    return result
                except DuplicateKeyError:
                    pass
        raise Invalid('BATCH_CONTROL_CONTENTION')

    def values(self, kind):
        if self.timing is None:
            return self._values(kind)
        return self.timing.iterate('mongo.values.' + kind, self.timing.active_scope or 'mongo', self._values(kind))

    def _values(self, kind):
        import json
        for record in self._collection(kind).find({}, {'body_json':1}).batch_size(100):
            yield verified(json.loads(record['body_json']))['value']

    @measured('get_many')
    def get_many(self, kind, keys):
        import json
        identities = {self._key(kind,key):key for key in keys}
        result = {key:None for key in keys}
        for record in self._collection(kind).find({'_id':{'$in':list(identities)}}):
            result[identities[record['_id']]] = verified(json.loads(record['body_json']))['value']
        return result

    @measured('get_list')
    def get_list(self, kind, keys):
        import json
        identities = [self._key(kind,key) for key in keys]
        found = {}
        for record in self._collection(kind).find({'_id':{'$in':identities}}):
            found[record['_id']] = verified(json.loads(record['body_json']))['value']
        return [found.get(identity) for identity in identities]

    @measured('delete')
    def delete(self, kind, key):
        if kind not in DELETABLE:
            raise Invalid('BATCH_RECORD_NOT_DELETABLE')
        self._collection(kind).delete_one({'_id':self._key(kind, key)})

    def close(self):
        self.client.close()
