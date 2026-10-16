"""Read-only Mongo source for the Spark Driver. No ingestion schema changes."""
from ..errors import PipelineError
from ..mongo import wire_document
from .contracts import Poison


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
