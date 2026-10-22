"""Immutable source-gallery wait evidence, separate from acquisition readiness."""
from pathlib import Path
from .contracts import digest
from .errors import Conflict
from .files import atomic_json,read_json


def record_gallery_wait(directory,doc,proof):
    if (proof.get('raw_document_id')!=doc['_id'] or proof.get('observation_key')!=doc['observation_key']
            or proof.get('status')!='VERIFIED'):
        raise Conflict('GALLERY_WAIT_OBSERVATION_CHANGED')
    if doc['image_refs']:return None  # byte-upload waiting is a different state
    detail_status=doc['collector_metadata'].get('detail_status')
    body=dict(contract_version=1,status='WAITING_AT_SOURCE_OBSERVATION',
        reason='SOURCE_DETAIL_REQUEST_FAILED' if detail_status=='request_failed' else 'SOURCE_GALLERY_URL_MISSING',
        product_key=doc['source_code']+':'+doc['source_product_id'],raw_document_id=doc['_id'],
        observation_key=doc['observation_key'],document_hash=doc['document_hash'],source_url=doc['source_url'],
        collected_at=doc['collected_at'],detail_status=detail_status,normalized_id=proof['normalized_id'],
        normalized_hash=proof['normalized_hash'],next_action='COLLECTOR_NEW_CONFIRMED_OBSERVATION',
        changes_service_approval=False)
    body['content_hash']=digest(body)
    path=Path(directory)/'input-waits'/'gallery-url'/(doc['observation_key']+'.json')
    if path.exists():
        if read_json(path)!=body:raise Conflict('GALLERY_WAIT_EVIDENCE_CHANGED')
    else:atomic_json(path,body)
    return path
