"""Read-only source capture and bounded refetch. No crawler, review or activation."""
from pathlib import Path

from ..domain.contracts import digest, canonical, Invalid, SourcePageLimit
from .files import read


def reference(item):
    doc, state = item['document'], item['image_state']
    return dict(product_key=doc['source_code']+':'+doc['source_product_id'],
        observation_key=doc['observation_key'], raw_document_id=doc['_id'],
        document_hash=doc['document_hash'], image_hash=digest({k:state[k] for k in ('image_state_revision','image_state_fingerprint')}),
        categories=[list(map(str,(v.get('api_product') or {}).get('category',{}).get('codes',[]))) for v in doc['payload']['listings']],
        attributes=[dict(name=v.get('name'),value=v.get('value')) for v in (doc['payload'].get('detail') or {}).get('specifications') or []])


def match(items, references):
    actual = {reference(item)['raw_document_id']: item for item in items}
    if len(actual) != len(items) or set(actual) != {r['raw_document_id'] for r in references}:
        raise Invalid('SOURCE_RANGE_MISSING_OR_DUPLICATE')
    result = []
    for expected in references:
        item = actual[expected['raw_document_id']]
        if reference(item) != expected: raise Invalid('SOURCE_CHANGED')
        result.append(item)
    return result


class JsonSource:
    """Local JSON input for real exported wire documents, not fabricated live results."""
    def __init__(self, path, max_page_bytes=32*1024*1024):
        self.path = Path(path).resolve()
        self.max_page_bytes=max_page_bytes
        if self.path.stat().st_size>128*1024*1024:raise Invalid('LOCAL_EXPORT_BYTE_LIMIT')
        values = read(self.path)
        if not isinstance(values, list): raise Invalid('SOURCE_ARRAY_REQUIRED')
        self.items = {reference(v)['raw_document_id']:v for v in values}
        if len(self.items) != len(values): raise Invalid('SOURCE_DUPLICATE_ID')
        self.identity = dict(kind='SEALED_EXPORT', content_hash=digest(values))

    def references(self):
        return iter(sorted((reference(v) for v in self.items.values()), key=lambda r:(r['product_key'],r['observation_key'])))

    def read_many(self, references):
        values=match([self.items[r['raw_document_id']] for r in references if r['raw_document_id'] in self.items], references)
        if sum(len(canonical(v).encode()) for v in values)>self.max_page_bytes:raise SourcePageLimit('SOURCE_PAGE_BYTES')
        return values

    def current(self, references):
        try:
            latest = JsonSource(self.path)
            match([latest.items[r['raw_document_id']] for r in references if r['raw_document_id'] in latest.items],references)
            products={r['product_key'] for r in references}
            if {r['observation_key'] for r in references}!={reference(v)['observation_key'] for v in latest.items.values() if reference(v)['product_key'] in products}:
                return False
            return True
        except (Invalid, KeyError): return False

    def close(self): pass


class MongoSource:
    def __init__(self, uri, database, replica_set, collection_run_id, max_page_bytes=32*1024*1024,
                 source_product_ids=None):
        from pymongo import MongoClient, ReadPreference
        from pymongo.read_concern import ReadConcern
        self.client = MongoClient(uri, replicaSet=replica_set, tz_aware=True, connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=False)
        hello = self.client.admin.command('hello')
        if hello.get('setName') != replica_set or len(hello.get('hosts',[])) != 3:
            self.client.close(); raise Invalid('MONGO_REPLICA_SET_REQUIRED')
        db = self.client.get_database(database, read_preference=ReadPreference.PRIMARY, read_concern=ReadConcern('majority'))
        self.raw, self.states = db['raw_products'], db['ingestion_observations']
        self.run = collection_run_id
        self.source_product_ids=source_product_ids
        self.max_page_bytes=max_page_bytes
        if not self.run: raise Invalid('COLLECTION_SCOPE_REQUIRED')
        if source_product_ids is not None and (not isinstance(source_product_ids,list) or not source_product_ids
                or len(source_product_ids)>100 or any(not isinstance(v,str) or not v or len(v)>64 for v in source_product_ids)
                or len(set(source_product_ids))!=len(source_product_ids)):
            raise Invalid('SOURCE_PRODUCT_SCOPE_INVALID')
        self.identity = dict(kind='MONGO', database=database, replica_set=replica_set,
            collection_run_id=self.run, source_product_ids=source_product_ids)

    def _states(self, docs):
        from pipeline_v2.legacy.mongo import wire_document
        wired = [wire_document(d) for d in docs]
        states = {s['observation_key']:s for s in self.states.find({'observation_key':{'$in':[d['observation_key'] for d in wired]}},
            {'_id':0,'observation_key':1,'image_state_vector':1,'image_state_revision':1,'image_state_fingerprint':1,'image_state_checked_at':1})}
        result = []
        for doc in wired:
            state = states.get(doc['observation_key'])
            if not state: raise Invalid('IMAGE_STATE_MISSING')
            checked = state.get('image_state_checked_at')
            if hasattr(checked, 'isoformat'): checked = checked.isoformat()
            state = dict(state, raw_document_id=doc['_id'], image_state_checked_at=checked)
            result.append(dict(document=doc, image_state=state))
        return result

    def references(self):
        # Read only discovery metadata; payload bodies are fetched for changed work later.
        projection={k:1 for k in ('_id','source_code','source_product_id','observation_key','document_hash',
            'payload.listings.api_product.category.codes','payload.detail.specifications')}
        # The run selects products; all their immutable observations are required for current-value reduction.
        query={'source_code':'danawa','collection_run_id':self.run}
        if self.source_product_ids is not None:query['source_product_id']={'$in':self.source_product_ids}
        products=sorted(self.raw.distinct('source_product_id',query))
        if self.source_product_ids is not None and set(products)!=set(self.source_product_ids):
            raise Invalid('SOURCE_PRODUCT_SCOPE_MISMATCH')
        for start in range(0,len(products),100):
            cursor=self.raw.find({'source_code':'danawa','source_product_id':{'$in':products[start:start+100]}},projection)
            cursor=cursor.sort([('source_product_id',1),('observation_key',1)]).batch_size(100)
            try:
                chunk=[]
                for document in cursor:
                    chunk.append(document)
                    if len(chunk)==100:
                        yield from self._metadata(chunk);chunk=[]
                if chunk:yield from self._metadata(chunk)
            finally:cursor.close()

    def _metadata(self, docs):
        states={s['observation_key']:s for s in self.states.find({'observation_key':{'$in':[d['observation_key'] for d in docs]}},
            {'_id':0,'observation_key':1,'image_state_revision':1,'image_state_fingerprint':1})}
        for doc in docs:
            doc['_id']=str(doc['_id'])
            if doc['observation_key'] not in states:raise Invalid('IMAGE_STATE_MISSING')
            yield reference(dict(document=doc,image_state=states[doc['observation_key']]))

    def read_many(self, references):
        from bson import ObjectId
        from pipeline_v2.legacy.mongo import wire_document
        cursor=self.raw.find({'_id':{'$in':[ObjectId(r['raw_document_id']) for r in references]}}).limit(len(references)+1)
        docs=[];amount=0
        try:
            for doc in cursor:
                amount+=len(canonical(wire_document(doc)).encode())
                if amount>self.max_page_bytes:raise SourcePageLimit('SOURCE_PAGE_BYTES')
                docs.append(doc)
        finally:cursor.close()
        return match(self._states(docs), references)

    def current(self, references):
        try:
            products=sorted({r['product_key'].split(':',1)[1] for r in references})
            fields={k:1 for k in ('_id','source_code','source_product_id','observation_key','document_hash',
                'payload.listings.api_product.category.codes','payload.detail.specifications')}
            cursor=self.raw.find({'source_code':'danawa','source_product_id':{'$in':products}},fields).limit(len(references)+1)
            try:current=list(self._metadata(list(cursor)))
            finally:cursor.close()
            return sorted(current,key=lambda r:r['raw_document_id'])==sorted(references,key=lambda r:r['raw_document_id'])
        except Invalid: return False

    def close(self): self.client.close()
