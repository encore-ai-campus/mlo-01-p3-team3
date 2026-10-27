"""Owner-provided immutable inputs and effective, observation-specific projections."""
import copy
from uuid import UUID

from ..contracts import canonical, digest, utc
from ..errors import Conflict, PipelineError

KINDS = ('mappings', 'reviews', 'links', 'rules')


def input_snapshot(kind, values=None, provider_version=None, evidence_ref=None):
    if kind not in KINDS or (values is not None and not isinstance(values, list)):
        raise PipelineError('SILVER_INPUT_CONTRACT_INVALID')
    value = dict(contract_version=1, kind=kind, status='NOT_PROVIDED' if values is None else 'PROVIDED',
                 rows=copy.deepcopy(values or []), provider_version=provider_version, evidence_ref=evidence_ref)
    value['content_hash'] = digest(value)
    return value


def validate_inputs(inputs):
    if set(inputs) != set(KINDS):
        raise PipelineError('SILVER_INPUTS_MUST_BE_EXPLICIT')
    for kind, value in inputs.items():
        if (value.get('contract_version') != 1 or value.get('kind') != kind
                or value.get('status') not in {'PROVIDED', 'NOT_PROVIDED'}
                or not isinstance(value.get('rows'), list)
                or (value['status'] == 'NOT_PROVIDED' and value['rows'])
                or digest({k:v for k,v in value.items() if k!='content_hash'}) != value.get('content_hash')):
            raise PipelineError('SILVER_INPUT_HASH_CONFLICT')
        if value['status'] == 'PROVIDED' and (not value.get('evidence_ref') or not value.get('provider_version')):
            raise PipelineError('SILVER_OWNER_EVIDENCE_REQUIRED')
        if any(not isinstance(r, dict) for r in value['rows']):
            raise PipelineError('SILVER_INPUT_ROW_INVALID')
        for row in value['rows']:
            required={'mappings':{'source_code','source_category_codes','service_category_id','service_category_code','active','excluded'},
                      'reviews':{'source_code','source_product_id','source_version','checksum','scopes','decision'},
                      'links':{'source_code','source_product_id','url','kind','status'},'rules':{'type'}}[kind]
            if not required.issubset(row):raise PipelineError('SILVER_INPUT_ROW_INVALID')
            if kind=='mappings' and (not isinstance(row['source_category_codes'],list)
                    or not row['source_category_codes']
                    or not all(isinstance(c,str) and c for c in row['source_category_codes'])
                    or type(row['active']) is not bool or type(row['excluded']) is not bool):
                raise PipelineError('SILVER_MAPPING_ROW_INVALID')
            if kind=='reviews' and (not isinstance(row['scopes'],list)
                    or row['decision'] not in {'APPROVED','PENDING','REVOKED','REJECTED'}):
                raise PipelineError('SILVER_REVIEW_ROW_INVALID')
            if kind=='links' and row['status'] not in {'VALID','INVALID','UNKNOWN'}:raise PipelineError('SILVER_LINK_ROW_INVALID')
            if kind=='rules' and row['type'] not in {'AXIS_RULE','ATTRIBUTE_ALIAS','OFFER_IDENTITY'}:raise PipelineError('SILVER_RULE_UNSUPPORTED')
            for name in ('valid_from','expires_at','checked_at'):
                if row.get(name):utc(row[name])


def registration(value, rehearse=False):
    try:
        run_id = str(UUID(value['processing_run_id']))
        if (run_id != value['processing_run_id'] or value['stage'] != 'NORMALIZATION'
                or value['status'] != ('FIXTURE' if rehearse else 'REGISTERED')
                or not value['evidence_ref']
                or (not rehearse and value.get('provider') != 'MYSQL_PIPELINE_RUNS_EXPORT')):
            raise ValueError()
        return run_id
    except (KeyError, ValueError, TypeError) as exc:
        raise PipelineError('SILVER_RUN_REGISTRATION_REQUIRED') from exc


def effective(row, as_of):
    result = copy.deepcopy(row)
    status = 'ACTIVE'
    if row.get('valid_from') and utc(as_of) < utc(row['valid_from']):
        status = 'NOT_YET_VALID'
    if row.get('expires_at') and utc(as_of) >= utc(row['expires_at']):
        status = 'EXPIRED'
    result['effective_status'] = status
    return result


def project(doc, inputs, as_of):
    validate_inputs(inputs)
    code = doc['source_product_id']
    detail=doc['payload'].get('detail') or {}
    image_links={(i['source_version'],i.get('sha256')) for i in doc['image_refs']}
    urls={detail.get('url')}
    urls.update(l.get('product_url') for l in doc['payload']['listings'])
    urls.update(r.get('url') for r in detail.get('seller_offers') or [])
    products=[detail.get('selected_product') or {},*(p for p in detail.get('json_ld') or [] if isinstance(p,dict))]
    for product in products:
        offers=product.get('offers') or []
        if isinstance(offers,dict):offers=[offers]
        urls.update(o.get('url') for o in offers if isinstance(o,dict))
    categories, attribute_names, raw_values = [], set(), set()
    for listing in doc['payload']['listings']:
        categories.append([str(x) for x in (listing.get('api_product') or {}).get('category', {}).get('codes', [])])
    for spec in (doc['payload'].get('detail') or {}).get('specifications') or []:
        attribute_names.add(str(spec.get('name', '')))
        raw_values.add(str(spec.get('value', '')))
    projected = {}
    for kind, snapshot in inputs.items():
        related = []
        for row in snapshot['rows']:
            if row.get('source_code', 'danawa') != 'danawa' or str(row.get('source_product_id',code)) != code:
                continue
            if kind == 'mappings' and row.get('source_category_codes') not in categories:
                continue
            if kind=='reviews' and (row.get('source_version'),row.get('checksum')) not in image_links:continue
            if kind=='links' and row.get('url') not in urls:continue
            if kind == 'rules':
                if row.get('type') == 'AXIS_RULE' and row.get('source_name') not in attribute_names:
                    continue
                if row.get('type') == 'ATTRIBUTE_ALIAS' and row.get('raw_value') not in raw_values:
                    continue
            related.append(effective(row, as_of))
        projected[kind] = dict(status=snapshot['status'], rows=sorted(related, key=canonical))
    return projected


def image_state(doc, state):
    raw_id = state.get('raw_document_id') or (state.get('document') or {}).get('_id')
    vector = state.get('image_state_vector')
    if (str(raw_id) != doc['_id'] or state.get('observation_key') != doc['observation_key']
            or type(state.get('image_state_revision')) is not int or state['image_state_revision'] < 0
            or not isinstance(vector, list) or digest(vector) != state.get('image_state_fingerprint')):
        raise Conflict('SILVER_IMAGE_STATE_CONFLICT')
    by_slot = {r['image_slot']:r for r in vector}
    if len(by_slot) != len(vector) or set(by_slot)-{i['image_slot'] for i in doc['image_refs']}:
        raise Conflict('SILVER_IMAGE_STATE_CONFLICT')
    for image in doc['image_refs']:
        item = by_slot.get(image['image_slot'])
        if item and (any(item.get(k) != image.get(v) for k,v in
            [('checksum','sha256'),('bucket','bucket'),('object_key','object_key')])
            or item.get('availability') not in {'AVAILABLE','FAILED','MISSING','UNCONFIRMED'}):
            raise Conflict('SILVER_IMAGE_STATE_CONFLICT')
        if item and item['availability']=='AVAILABLE' and (image['acquisition_state']!='VERIFIED'
                or not all(image.get(k) for k in ('sha256','bucket','object_key'))
                or item.get('verification')!='sha256-bytes-v1'):
            raise Conflict('SILVER_IMAGE_STATE_CONFLICT')
    return dict(revision=state['image_state_revision'], fingerprint=state['image_state_fingerprint'],
                vector=copy.deepcopy(vector), checked_at=state.get('image_state_checked_at'))
