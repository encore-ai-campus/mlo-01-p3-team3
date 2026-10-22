"""Read-only collector-normalized companion input; never a service approval.

Mongo danawa_products is paired with the immutable raw observation, not joined
to an arbitrary latest product. Preserve its exact content in a hashed archive.
"""
from .contracts import GALLERY_FIELDS, digest
from .errors import PipelineError, Conflict
from .snapshot import _validate


def bind_normalized(doc, normalized):
    raw = dict(doc['collector_metadata'], payload=doc['payload'])
    _validate(raw, normalized, None)
    expected = [{k: i.get(k) for k in GALLERY_FIELDS} for i in doc['image_refs']]
    images=normalized.get('images')
    if images is None:images=[]  # no-gallery observations keep their original null
    if not isinstance(images,list) or any(not isinstance(i,dict) for i in images):
        raise PipelineError('CATALOG_GALLERY_INVALID')
    actual = [{k: i.get(k) for k in GALLERY_FIELDS} for i in images]
    if expected != actual:
        raise Conflict('CATALOG_GALLERY_OBSERVATION_MISMATCH')
    return dict(contract_version=1, stage='COLLECTOR_NORMALIZED_INPUT', status='VERIFIED',
                collection='danawa_products', normalized_id=str(normalized['_id']),
                normalized_hash=digest(normalized), normalized_document=normalized,
                raw_document_id=doc['_id'], observation_key=doc['observation_key'],
                raw_document_hash=doc['document_hash'], payload_hash=doc['payload_hash'])


class CatalogInputs:
    def __init__(self, collection):
        self.collection = collection

    def get(self, doc):
        raw = doc['collector_metadata']
        query = dict(source=doc['source_code'], product_id=doc['source_product_id'],
                     job_id=doc['collection_run_id'], raw_payload_hash=doc['payload_hash'],
                     collected_at=raw['collected_at'])
        values = list(self.collection.find(query).limit(2))
        if len(values) != 1:
            raise PipelineError('CATALOG_OBSERVATION_MISSING' if not values else 'CATALOG_OBSERVATION_AMBIGUOUS')
        return bind_normalized(doc, values[0])
