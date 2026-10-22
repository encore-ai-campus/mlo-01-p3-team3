"""Resolve every historical raw observation from receipts, never processing order."""
from collections import defaultdict
from ..errors import Conflict,PipelineError
from ..files import parse
from .build import validate_group


def published_choices(db,product_keys):
    """Only the current successfully published snapshot supplies prior choices.

    Retained scalar lineage is not an instruction to select another evaluation.
    The caller owns a consistent read snapshot; no Spark/S3 calls occur here.
    """
    from ..publication.runner import audit_reference
    result=[]
    for key in product_keys:
        saved=db.get('products',dict(product_key=key),locking=False)
        if not saved:continue
        group=audit_reference(db,saved['gold_snapshot_ref_json'])
        catalog=group['tables']['product_catalog'][0]
        if catalog['product_key']!=key:raise Conflict('GOLD_PRIOR_PRODUCT_CHANGED')
        for ref in parse(catalog['source_refs_json']):
            result.append(dict(product_key=key,raw_document_id=ref['raw_document_id'],
                evaluation_key=ref['evaluation_key'],document_hash=ref['document_hash'],
                evidence_ref='mysql-publication://'+db.target+'/'+parse(saved['gold_snapshot_ref_json'])['plan_id']))
    return result


def resolve(groups,current=(),prior=()):
    """Current completion > proven published choice > single complete receipt.

    A published evaluation absent at the pinned versions requires normalization;
    it is not silently replaced with another candidate. Missing historical raw
    choices are returned explicitly for a separately registered supplement.
    """
    candidates=defaultdict(dict)
    for group in groups:
        validate_group(group);r=group['receipt'];key=(r['product_key'],r['raw_document_id'])
        if r['evaluation_key'] in candidates[key]:raise Conflict('GOLD_DUPLICATE_SILVER_RECEIPT')
        candidates[key][r['evaluation_key']]=r
    def index(choices):
        result={}
        for c in choices:
            key=(c['product_key'],c['raw_document_id'])
            if key in result and result[key]!=c:raise Conflict('GOLD_COMPLETION_SELECTION_CONFLICT')
            result[key]=c
        return result
    explicit=index(current);previous=index(prior)
    if not set(explicit)<=set(candidates):raise Conflict('GOLD_CURRENT_SELECTION_NOT_CONFIRMED')
    selected=[];missing=[]
    for key,values in sorted(candidates.items()):
        choice=explicit.get(key) or previous.get(key)
        if choice:
            receipt=values.get(choice['evaluation_key'])
            if receipt is None:
                if key in explicit:raise Conflict('GOLD_CURRENT_SELECTION_NOT_CONFIRMED')
                missing.append(dict(product_key=key[0],raw_document_id=key[1],reason='PRIOR_EVALUATION_NOT_IN_PIN'));continue
            if choice.get('document_hash',receipt['document_hash'])!=receipt['document_hash']:
                raise Conflict('GOLD_PRIOR_DOCUMENT_CHANGED')
            evidence=choice['evidence_ref']
        elif len(values)==1:
            receipt=next(iter(values.values()));evidence='silver-receipt://'+receipt['evaluation_key']
        else:
            missing.append(dict(product_key=key[0],raw_document_id=key[1],reason='HISTORICAL_SELECTION_REQUIRED'));continue
        selected.append(dict(product_key=key[0],raw_document_id=key[1],evaluation_key=receipt['evaluation_key'],evidence_ref=evidence))
    if len(selected)+len(missing)>20000:raise PipelineError('GOLD_HISTORY_SELECTION_LIMIT')
    return selected,missing
