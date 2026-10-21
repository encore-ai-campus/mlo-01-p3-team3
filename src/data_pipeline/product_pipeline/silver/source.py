"""Pinned Bronze reads and read-only primary/majority image-state lookup."""
from ..contracts import canonical,utc
from ..errors import Conflict,PipelineError
from ..files import parse


def document(row):
    doc=parse(row['raw_envelope_json'])
    if (canonical(doc)!=row['raw_envelope_json'] or canonical(doc['payload'])!=row['payload_json']
            or any(doc.get(a)!=row.get(b) for a,b in [('_id','raw_document_id'),('document_hash','document_hash'),
                ('observation_key','observation_key'),('source_product_id','source_product_id'),('source_code','source_code')])):
        raise Conflict('SILVER_BRONZE_CONTENT_CONFLICT')
    return doc


class BronzeSource:
    def __init__(self,spark,settings,version):
        if type(version) is not int or version<0:raise PipelineError('SILVER_FIXED_BRONZE_VERSION_REQUIRED')
        self.spark,self.settings,self.version=spark,settings,version
        self.frame=spark.read.format('delta').option('versionAsOf',version).load(settings.bronze_path)
        required={'raw_document_id','observation_key','document_hash','raw_envelope_json','payload_json','source_code','source_product_id'}
        if not required.issubset(self.frame.columns):raise PipelineError('SILVER_BRONZE_SCHEMA_INVALID')

    def restrict_window(self, window):
        from pyspark.sql import functions as F
        from ..windows import validate_window
        validate_window(window)
        if window['bronze_version']!=self.version:raise Conflict('PIPELINE_WINDOW_VERSION_CHANGED')
        frame=self.frame.filter(F.col('observation_key').isin(window['observation_keys']))
        rows=frame.select('observation_key','source_code','source_product_id').limit(101).collect()
        if (sorted(r['observation_key'] for r in rows)!=window['observation_keys'] or
            sorted({r['source_code']+':'+r['source_product_id'] for r in rows})!=window['product_keys']):
            raise Conflict('PIPELINE_WINDOW_SOURCE_CHANGED')
        self.frame=frame

    def exclude_version(self, base_version):
        if base_version<0:return
        if type(base_version) is not int or base_version>self.version:raise PipelineError('PIPELINE_WINDOW_VERSION_CHANGED')
        old=self.spark.read.format('delta').option('versionAsOf',base_version).load(self.settings.bronze_path)
        self.frame=self.frame.join(old.select('raw_document_id'),'raw_document_id','left_anti')

    def page(self,after):
        from pyspark.sql import functions as F
        frame=self.frame
        if after:frame=frame.filter(F.col('observation_key')>after)
        n=self.settings.max_records
        # Read small identity/length projections before loading full payloads on Driver.
        headers=(frame.select('observation_key','raw_document_id',F.length(F.encode('raw_envelope_json','UTF-8')).alias('bytes'))
                 .orderBy('observation_key').limit(n+1).collect())
        selected,total=[],0
        for row in headers[:n]:
            if not row['bytes'] or row['bytes']>self.settings.max_raw_bytes:raise PipelineError('SILVER_RAW_BYTE_LIMIT')
            if selected and total+row['bytes']>self.settings.max_raw_bytes:break
            selected.append(row['raw_document_id']);total+=row['bytes']
        if not selected:return []
        values=frame.filter(F.col('raw_document_id').isin(selected)).orderBy('observation_key').limit(len(selected)+1).collect()
        if len(values)!=len(selected) or len({r['observation_key'] for r in values})!=len(values):
            raise Conflict('SILVER_BRONZE_DUPLICATE_IDENTITY')
        return [document(r.asDict()) for r in values]


class MongoStates:
    def __init__(self,settings,uri):
        from pymongo import MongoClient,ReadPreference
        from pymongo.read_concern import ReadConcern
        self.client=MongoClient(uri,replicaSet=settings.mongo_replica_set,retryWrites=False,tz_aware=True,
            connectTimeoutMS=5000,serverSelectionTimeoutMS=5000,socketTimeoutMS=30000,appname='danawa-silver-reader')
        hello=self.client.admin.command('hello')
        if hello.get('setName')!=settings.mongo_replica_set or len(hello.get('hosts',[]))!=3 or hello.get('arbiters'):
            self.client.close();raise PipelineError('SILVER_REPLICA_SET_REQUIRED')
        db=self.client.get_database(settings.mongo_database,read_preference=ReadPreference.PRIMARY,read_concern=ReadConcern('majority'))
        self.collection=db['ingestion_observations']
        self.database=db

    def normalized(self, doc):
        from ..catalog_input import CatalogInputs
        return CatalogInputs(self.database['danawa_products']).get(doc)

    def get(self,raw_id,observation):
        value=self.collection.find_one({'observation_key':observation,'document._id':raw_id},
            {'_id':0,'observation_key':1,'image_state_vector':1,'image_state_revision':1,
             'image_state_fingerprint':1,'image_state_checked_at':1,'stage':1,'raw_confirmed':1})
        if value is None:raise PipelineError('SILVER_IMAGE_STATE_UNCONFIRMED')
        if not value.get('raw_confirmed') and value.get('stage') not in {'RAW_CONFIRMED','EVENT_PENDING','EVENT_UNKNOWN','EVENT_CONFIRMED'}:
            raise PipelineError('SILVER_RAW_UNCONFIRMED')
        if hasattr(value.get('image_state_checked_at'),'isoformat'):
            value['image_state_checked_at']=utc(value['image_state_checked_at'].isoformat())
        return dict(value,raw_document_id=raw_id)

    def close(self):self.client.close()


class FileStates:
    """Explicit rehearsal-only immutable exported state; never substituted for DB failure."""
    def __init__(self,values):self.values=values
    def get(self,raw_id,observation):
        value=self.values.get(raw_id)
        if value is None:raise PipelineError('SILVER_FIXTURE_STATE_MISSING')
        return value
