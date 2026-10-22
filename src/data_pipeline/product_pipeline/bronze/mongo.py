"""Read-only Mongo source for the Spark Driver. No ingestion schema changes."""
from ..errors import PipelineError
from ..mongo import wire_document
from .contracts import Poison
from .metrics import timed,bump


class MongoSource:
    def __init__(self, settings, uri, client=None):
        from pymongo import MongoClient, ReadPreference
        from pymongo.read_concern import ReadConcern
        self.client = client or MongoClient(uri, replicaSet=settings.mongo_replica_set, retryWrites=False,
            tz_aware=True, appname='danawa-bronze', connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000, socketTimeoutMS=30000)
        hello = self.client.admin.command('hello')
        if hello.get('setName') != settings.mongo_replica_set or len(hello.get('hosts', [])) != 3 or hello.get('arbiters'):
            self.client.close()
            raise PipelineError('REPLICA_SET_CONTRACT_REQUIRED')
        db = self.client.get_database(settings.mongo_database, read_preference=ReadPreference.PRIMARY,
                                      read_concern=ReadConcern('majority'))
        self.raw = db['raw_products']

    def get(self, key):
        from bson import ObjectId
        value = self.raw.find_one({'_id': ObjectId(key)})
        if value is None:
            raise PipelineError('BRONZE_RAW_UNCONFIRMED')
        try:
            return wire_document(value)
        except (KeyError, TypeError, AttributeError, ValueError, PipelineError) as exc:
            raise Poison('BRONZE_RAW_CONTRACT_CONFLICT') from exc

    def close(self):
        self.client.close()

    def get_many(self, keys, *, max_bytes):
        """Bounded primary/majority lookup; malformed rows remain per-row poison."""
        from bson import ObjectId
        from ..contracts import canonical
        keys=set(keys)
        if len(keys)>100:raise PipelineError('BRONZE_BATCH_INPUT_LIMIT')
        if not keys:return {}
        result={};size=0
        with timed('mongo_batch_read'):
            cursor=self.raw.find({'_id':{'$in':[ObjectId(k) for k in sorted(keys)]}}).limit(len(keys)+1).batch_size(min(20,len(keys)))
            try:
                for value in cursor:
                    key=str(value['_id'])
                    if key not in keys or key in result:raise PipelineError('BRONZE_RAW_LOOKUP_CONFLICT')
                    try:
                        doc=wire_document(value)
                        amount=len(canonical(doc).encode('utf-8'))
                    except (KeyError,TypeError,AttributeError,ValueError,PipelineError):
                        result[key]=Poison('BRONZE_RAW_CONTRACT_CONFLICT');continue
                    size+=amount
                    if size>max_bytes:raise PipelineError('BRONZE_BATCH_RAW_LIMIT')
                    result[key]=doc
            finally:cursor.close()
        if set(result)!=keys:raise PipelineError('BRONZE_RAW_UNCONFIRMED')
        bump('mongo_documents',len(result));bump('mongo_raw_bytes',size)
        return result
