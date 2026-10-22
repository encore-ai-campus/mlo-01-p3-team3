"""Reuse fact parsing only. Gold selection has no approval or activation dependency."""
from collections import defaultdict
from uuid import UUID

from .contracts import canonical, digest, Invalid

VERSION = 'direct-catalog-1'
RUN = '71eb53dc-645d-45af-a7bc-16a8e9426eb6'


def related_policy(policy, references):
    categories={tuple(v) for r in references for v in r['categories']}
    names={str(v['name']) for r in references for v in r['attributes']}
    values={str(v['value']) for r in references for v in r['attributes']}
    mappings=[r for r in policy.get('mappings',[]) if tuple(r['source_category_codes']) in categories]
    rules=[r for r in policy.get('rules',[]) if r.get('type')=='OFFER_IDENTITY'
        or r.get('source_name') in names or r.get('raw_value') in values]
    return dict(mappings=sorted(mappings,key=canonical),rules=sorted(rules,key=canonical),gold=policy.get('gold',{}))


def normalization_inputs(policy):
    from product_pipeline.silver.inputs import input_snapshot
    return {kind:input_snapshot(kind, policy.get(kind) if kind in {'mappings','rules'} else None,
        provider_version=VERSION, evidence_ref='config://direct-pipeline-policy')
        for kind in ('mappings','rules','reviews','links')}


def silver_identity(item, policy):
    from product_pipeline.silver.inputs import project
    doc, state = item['document'], item['image_state']
    projected = project(doc, normalization_inputs(policy), doc['collected_at'])
    return digest(dict(version=VERSION, document_hash=doc['document_hash'],
        image_revision=state['image_state_revision'], image_fingerprint=state['image_state_fingerprint'],
        rules=projected['rules'], mappings=projected['mappings']))


def normalize(item, policy):
    from product_pipeline.silver.normalize import normalize as parse_facts
    doc, state = item['document'], item['image_state']
    result = parse_facts(doc, state, normalization_inputs(policy), processing_run_id=RUN,
        as_of=doc['collected_at'], processed_at=doc['collected_at'])
    # Keep source facts and parser diagnostics. Legacy review fields are never decisions in v2.
    result['direct_input_hash'] = silver_identity(item, policy)
    result['direct_images'] = []
    by_slot = {v['image_slot']:v for v in state['image_state_vector']}
    for image in doc['image_refs']:
        status = by_slot.get(image['image_slot'], {})
        result['direct_images'].append(dict(image, available=status.get('availability') == 'AVAILABLE',
            raw_document_id=doc['_id'], raw_payload_hash=doc['payload_hash'], raw_collected_at=doc['collected_at']))
    result['raw_ref'] = {k:doc[k] for k in ('_id','observation_key','document_hash','collected_at','source_code','source_product_id')}
    return result


def latest(rows, fields):
    grouped = defaultdict(list)
    for value in rows:
        if value.get('observed_at'):
            grouped[value['observed_at']].append(value)
    for at in sorted(grouped, reverse=True):
        candidates = grouped[at]
        if len({digest({name:v.get(name) for name in fields}) for v in candidates}) == 1:
            return candidates[0]
    return None


def gold(product_key, observations, policy):
    if not observations: raise Invalid('EMPTY_PRODUCT_HISTORY')
    facts = defaultdict(list)
    images, refs = [], []
    for observation in observations:
        if observation['raw_ref']['source_code']+':'+observation['raw_ref']['source_product_id'] != product_key:
            raise Invalid('PRODUCT_HISTORY_MIXED')
        refs.append(observation['raw_ref'])
        images.extend(observation['direct_images'])
        for table, rows in observation['tables'].items(): facts[table].extend(rows)
    scalars = {}
    lineage = {}
    for name in ('display_name','manufacturer','brand','model','width_mm','depth_mm','height_mm'):
        selected = latest([r for r in facts['product_observations'] if r.get(name) is not None], [name])
        scalars[name] = selected[name] if selected else None
        lineage[name] = dict(observed_at=selected['observed_at'], raw_document_id=selected['raw_document_id']) if selected else None
    category = latest([r for r in facts['product_observations'] if r.get('mapping_status') == 'MAPPED'],
        ['service_category_id','service_category_code'])
    category_id = category['service_category_id'] if category else None
    if category_id: UUID(category_id)
    # The old parser's boolean usable also depends on a review result. Validate the observed URL directly.
    from product_pipeline.contracts import valid_product_url
    comparisons = [r for r in facts['link_observations'] if r['link_kind'] == 'COMPARISON'
        and valid_product_url(r['source_url'], product_key.split(':',1)[1])]
    comparison = latest(comparisons, ['source_url'])
    lineage['comparison_url'] = dict(observed_at=comparison['observed_at'], raw_document_id=comparison['raw_document_id']) if comparison else None
    by_slot = defaultdict(list)
    for image in images: by_slot[image['image_slot']].append(image)
    current_images = []
    for slot, values in by_slot.items():
        at = max(i['raw_collected_at'] for i in values)
        current = [i for i in values if i['raw_collected_at'] == at]
        if len({(i.get('sha256'),i.get('bucket'),i.get('object_key'),i['available']) for i in current}) != 1:
            continue
        image = current[0]
        if image['available'] and all(image.get(k) for k in ('sha256','bucket','object_key','bytes','width','height','format')):
            current_images.append(image)
    offers = []
    for identity in {r['offer_key']:r for r in facts['normalized_offers']}.values():
        price = latest([p for p in facts['price_observations'] if p.get('offer_key') == identity['offer_key']
            and p['numeric_status'] == 'CONFIRMED' and p['currency'] == 'KRW'], ['amount','currency','condition_json'])
        if price:
            offers.append(dict(identity=identity, price=price, purchase_ready=False))
    issues = []
    if not scalars['display_name']: issues.append('NAME_MISSING_OR_CONFLICT')
    if not category_id: issues.append('CATEGORY_UNMAPPED_OR_CONFLICT')
    if not current_images: issues.append('IMAGE_UNAVAILABLE')
    # Preserve price kinds. A comparison minimum is never promoted to a seller offer or regular price.
    prices = facts['price_observations']
    attributes = list({digest({k:v for k,v in r.items() if k not in {'processed_at','processing_run_id'}}):r
        for r in facts['product_attributes']}.values())
    ready=bool(scalars['display_name'] and category_id)
    if policy.get('gold',{}).get('require_image'):ready=ready and bool(current_images)
    if policy.get('gold',{}).get('require_comparison_link'):ready=ready and bool(comparison)
    return dict(product_key=product_key, source_code=refs[0]['source_code'], source_product_id=refs[0]['source_product_id'],
        **scalars, category_id=category_id, category_code=category['service_category_code'] if category else None,
        comparison_url=comparison['source_url'] if comparison else None, images=sorted(current_images,key=lambda r:r['order']),
        attributes=attributes, offers=offers, price_observations=prices, links=facts['link_observations'],
        source_refs=sorted(refs,key=lambda r:r['observation_key']), lineage=lineage,
        first_collected_at=min(r['collected_at'] for r in refs),
        catalog_ready=bool(ready), recommendation_ready=False,
        purchase_link_ready=False, comparison_link_ready=bool(comparison),
        quality_reason_codes=issues, outcome='OUTPUT_COMMITTED' if ready else 'QUALITY_WAIT',
        policy_version=VERSION, normalization_policy_hash=digest(dict(version=VERSION,**{k:policy.get(k,[]) for k in ('mappings','rules')})))
