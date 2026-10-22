"""Pure owner review validation and identity rules."""
from uuid import UUID
from .contracts import canonical,digest,now,utc
from .errors import PipelineError
from .publication.policy import uuid_hex

KINDS={'CATEGORY','MAPPING','PRODUCT','IMAGE','POLICY'}
SCOPES={'STORAGE','DISPLAY','RECOMMENDATION','EMBEDDING','ANALYSIS'}


def seal(value):
    return {**value,'content_hash':digest({k:v for k,v in value.items() if k!='content_hash'})}


def subject(kind,payload):
    if kind=='CATEGORY':return payload['code']
    if kind=='MAPPING':return payload['source_code']+':'+digest(payload['source_category_codes'])
    if kind=='PRODUCT':return payload['product_key']
    if kind=='IMAGE':return uuid_hex(payload['image_id'])
    return 'publication'


def validate(bundle,*,at=None):
    at=utc(at or now())
    if (bundle.get('contract_version')!=1 or bundle.get('status')!='REVIEWED' or
            bundle.get('content_hash')!=digest({k:v for k,v in bundle.items() if k!='content_hash'})):
        raise PipelineError('REVIEW_REVIEWED_BUNDLE_REQUIRED')
    for key in ('actor_ref','evidence_ref','criteria_ref'):
        if not isinstance(bundle.get(key),str) or not bundle[key].strip() or bundle[key].startswith('replace-with-'):
            raise PipelineError('REVIEW_ACTOR_AND_EVIDENCE_REQUIRED')
    if not utc(bundle['reviewed_at'])<=at<utc(bundle['expires_at']):raise PipelineError('REVIEW_EXPIRED_OR_FUTURE')
    if len(bundle.get('target',''))!=64 or not 1<=len(bundle.get('items',[]))<=1000 or len(canonical(bundle).encode())>32*1024*1024:
        raise PipelineError('REVIEW_BUNDLE_LIMIT_OR_TARGET')
    seen=set()
    try:
        UUID(bundle['bundle_id'])
        for item in bundle['items']:
            UUID(item['id']);kind=item['kind'];p=item['payload']
            if kind not in KINDS or item['subject_key']!=subject(kind,p) or (kind,item['subject_key']) in seen:
                raise ValueError()
            seen.add((kind,item['subject_key']))
            if type(item['revision']) is not int or item['revision']<1 or item['decision'] not in {'APPROVED','PENDING','REJECTED','REVOKED'}:
                raise ValueError()
            for field in ('expected_previous_hash','expected_current_hash'):
                if item.get(field) is not None and len(item[field])!=64:raise ValueError()
            if kind=='CATEGORY':
                UUID(p['category_id'])
                if not 1<=len(p['code'])<=100 or p['code']=='PIPELINE_UNMAPPED' or not 1<=len(p['name'])<=255 or any(type(p[k]) is not bool for k in ('active','recognition_enabled')):raise ValueError()
            if kind=='MAPPING':
                if p['source_code']!='danawa' or not p['source_category_codes'] or not all(isinstance(c,str) and c for c in p['source_category_codes']):raise ValueError()
                if not p.get('excluded') or p.get('service_category_id'):UUID(p['service_category_id'])
                if not 1<=len(p['mapping_version'])<=100 or len(canonical(p['source_category_codes']))>500 or any(type(p[k]) is not bool for k in ('active','excluded')):raise ValueError()
            if kind=='PRODUCT':
                if not p['product_key'].startswith('danawa:') or any(type(p[k]) is not bool for k in ('active','supported')):raise ValueError()
                UUID(p['service_category_id'])
                if (not isinstance(p['applicable_attributes'],list) or len(set(p['applicable_attributes']))!=len(p['applicable_attributes']) or
                        not p['raw_document_ids'] or any(len(r)!=24 for r in p['raw_document_ids'])):raise ValueError()
            if kind=='IMAGE':
                UUID(p['image_id'])
                if not isinstance(p['scopes'],list) or not set(p['scopes'])<=SCOPES or len(p['checksum'])!=64 or not p['source_version']:raise ValueError()
                if item['decision']=='APPROVED' and (not p['scopes'] or not p.get('permission_evidence_ref') or not isinstance(p.get('asset_policy'),dict)):
                    raise PipelineError('REVIEW_IMAGE_PERMISSION_EVIDENCE_REQUIRED')
                if item['decision']=='APPROVED':
                    policy=p['asset_policy']
                    if (policy.get('decision')!='APPROVED' or not policy.get('evidence_ref') or
                            not isinstance(policy.get('allowed_scopes'),list) or not set(p['scopes'])<=set(policy['allowed_scopes']) or
                            not set(policy['allowed_scopes'])<=SCOPES or set(policy.get('denied_scopes',[]))&set(p['scopes'])):
                        raise PipelineError('REVIEW_ASSET_PERMISSION_SCOPE_REQUIRED')
            if kind=='POLICY' and (p['price_history_mode'] not in {'SNAPSHOT_V1','PRICE_EFFECT_V2'} or
                    type(p['price_max_age_seconds']) is not int or p['price_max_age_seconds']<=0):raise ValueError()
    except (KeyError,TypeError,ValueError,AttributeError) as exc:raise PipelineError('REVIEW_ITEM_INVALID') from exc
    return bundle

