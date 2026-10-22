"""Explicit inactive intake. This path never manufactures owner approvals."""
from decimal import Decimal

from ..contracts import canonical
from ..errors import Conflict, PipelineError
from ..files import parse
from .values import sql_time

READY_FIELDS=('catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready')


def product(db,item,plan,at):
    from .policy import uuid_hex,guarded_projection,extension_values,product_reference,snapshot_ref
    catalog=item['group']['tables']['product_catalog'][0]
    category=db.get('categories',dict(id=uuid_hex(plan['pending_category_id'])))
    if (not category or category['code']!='PIPELINE_UNMAPPED' or category['active'] or category['recognition_enabled']):
        raise PipelineError('PUBLICATION_PENDING_CATEGORY_REQUIRED')
    key=item['product_key'];old=db.get('products',dict(product_key=key))
    if old and (old['status']!='PENDING_REVIEW' or old['source_code']!=catalog['source_code'] or
                old['source_product_id']!=catalog['source_product_id']):
        raise Conflict('PUBLICATION_PENDING_OWNER_STATE_CHANGED')
    version=db.get('pipeline_product_versions',dict(product_key=key))
    if old and not version:raise PipelineError('PUBLICATION_EXISTING_PRODUCT_BASELINE_REQUIRED')
    if old and (old['publication_status']!='PENDING_REVIEW' or any(old[k] for k in READY_FIELDS) or
                old['price_krw'] is not None or old['comparison_url'] is not None or old['representative_image_id'] is not None):
        raise Conflict('PUBLICATION_PENDING_OWNER_STATE_CHANGED')
    incoming={k:catalog[k] for k in ('manufacturer','brand','width_mm','depth_mm','height_mm')}
    incoming.update(name=catalog['display_name'],model_name=catalog['model'])
    lineage=parse(catalog['field_lineage_json'])
    lineage.update(name=lineage.get('display_name'),model_name=lineage.get('model'))
    comparable=dict(old or {})
    for field in ('width_mm','depth_mm','height_mm'):
        if incoming[field] is not None:incoming[field]=format(Decimal(str(incoming[field])).normalize(),'f')
        if comparable.get(field) is not None:comparable[field]=format(Decimal(str(comparable[field])).normalize(),'f')
    values,proofs=guarded_projection(comparable,incoming,lineage,parse(version['field_versions_json']) if version else {})
    newer=not version or sql_time(plan['as_of'])>=version['latest_as_of']
    values.update(extension_values(old,catalog,values,newer=newer))
    first=sql_time(catalog['first_collected_at'])
    if version and version['first_collected_at']:
        first=min(first,version['first_collected_at']) if first else version['first_collected_at']
    if newer:
        values.update({k:False for k in READY_FIELDS})
        values.update(category_id=category['id'],publication_status='PENDING_REVIEW',
            publication_reason_codes=canonical(sorted(set(catalog['publication_reason_codes'])|{'OWNER_REVIEW_NOT_PROVIDED'})),
            embedding_json='{}',price_krw=None,comparison_url=None,representative_image_id=None,first_collected_at=first)
        values['gold_snapshot_ref_json']=product_reference(item,plan,old,values)
    if old:
        db.update('products',dict(id=old['id']),dict(**values,updated_at=at));saved=old
    else:
        if not incoming['name']:raise PipelineError('PUBLICATION_PRODUCT_NAME_REQUIRED')
        saved,_=db.identity('products',dict(product_key=key),dict(**values,
            source_code=catalog['source_code'],source_product_id=catalog['source_product_id'],
            status='PENDING_REVIEW',dedup_status='SOURCE_IDENTITY_ONLY',created_at=at,updated_at=at))
    current=dict(field_versions_json=canonical(proofs),first_collected_at=first,
        latest_as_of=sql_time(plan['as_of']) if newer else version['latest_as_of'],
        latest_evaluation=catalog['gold_evaluation_key'] if newer else version['latest_evaluation'])
    if version:db.update('pipeline_product_versions',dict(product_key=key),current)
    else:db.identity('pipeline_product_versions',dict(product_key=key),current)
    for image in item['group']['tables']['product_images']:
        evidence=parse(image['evidence_json']);source=parse(evidence['evidence_json'])
        if not image.get('object_key') or not image.get('image_checksum'):continue
        if (any(not evidence.get(k) for k in ('bytes','width','height','format')) or
                not source.get('raw_payload_hash') or not source.get('raw_collected_at')):
            raise PipelineError('PUBLICATION_IMAGE_RAW_PROVENANCE_REQUIRED')
        asset,_=db.identity('assets',dict(bucket=image['bucket'],object_key=image['object_key']),dict(
            owner_id=None,kind='PRODUCT_IMAGE',content_type='image/'+evidence['format'].lower().replace('jpg','jpeg'),
            size_bytes=evidence['bytes'],width_px=evidence['width'],height_px=evidence['height'],
            checksum=image['image_checksum'],status='PENDING_REVIEW',
            usage_policy_json=canonical(dict(decision='NOT_REVIEWED',recommendation_allowed=False)),created_at=at,updated_at=at))
        if any(asset[k]!=v for k,v in dict(checksum=image['image_checksum'],size_bytes=evidence['bytes'],
            width_px=evidence['width'],height_px=evidence['height']).items()):raise Conflict('PUBLICATION_ASSET_CONFLICT')
        link,created=db.identity('product_images',dict(product_id=saved['id'],source_code=catalog['source_code'],
            source_product_id=catalog['source_product_id'],image_variant=image['image_slot'],
            raw_document_id=image['raw_document_id'],source_version=image['source_version']),dict(
            asset_id=asset['id'],display_order=image['display_order'],source_url=image['source_url'],
            raw_payload_hash=source['raw_payload_hash'],raw_collected_at=sql_time(source['raw_collected_at']),linked_at=at,
            active=False,usage_review_json=canonical(dict(decision='NOT_REVIEWED')),created_at=at,updated_at=at,
            image_relation_key=image['image_relation_key'],gold_recommendation_approved=False,
            gold_review_snapshot_hash=None,source_evidence_json=image['evidence_json'],
            gold_snapshot_ref_json=snapshot_ref(item,plan,[('product_images',image)])))
        if (link['asset_id']!=asset['id'] or link['source_url']!=image['source_url'] or
                link['raw_payload_hash']!=source['raw_payload_hash']):
            raise Conflict('PUBLICATION_IMAGE_LINEAGE_CONFLICT')
        if not created and (link['active'] or parse(link['usage_review_json'])!={'decision':'NOT_REVIEWED'}):
            raise Conflict('PUBLICATION_PENDING_OWNER_STATE_CHANGED')
        if not created and newer:
            db.update('product_images',dict(id=link['id']),dict(image_relation_key=image['image_relation_key'],display_order=image['display_order'],
                source_evidence_json=image['evidence_json'],gold_snapshot_ref_json=snapshot_ref(item,plan,[('product_images',image)]),updated_at=at))
    from .repository import _links
    _links(db,saved,item['group']['tables'],item,plan,at)
    from .repository import _observed_offers
    _observed_offers(db,saved,catalog,item['group']['tables'],at,item,plan)
    return saved['id']


def reconcile_links(plan,db):
    """Restore link observations from already committed immutable staging.

No product activation, destination approval or offer is synthesized. This is
additive recovery for older inactive publications that omitted link rows.
"""
    from .plan import verify_plan
    from .repository import _links
    from ..contracts import digest
    verify_plan(plan)
    if plan.get('publication_mode')!='PENDING_REVIEW' or plan['target']!=db.target:
        raise Conflict('PUBLICATION_TARGET_MISMATCH')
    count=0
    with db.transaction():
        receipt=db.get('pipeline_publications',dict(plan_id=plan['plan_id']))
        if not receipt or receipt['status']!='COMPLETED' or receipt['plan_hash']!=digest(plan):
            raise Conflict('PUBLICATION_PENDING_RECEIPT_CHANGED')
        at=db.one('SELECT UTC_TIMESTAMP(3) AS stamp')['stamp']
        for item in plan['products']:
            staged=db.get('pipeline_publication_staging',dict(plan_id=plan['plan_id'],product_key=item['product_key']))
            if not staged or staged['body_hash']!=digest(item) or parse(staged['body_json'])!=item:
                raise Conflict('PUBLICATION_STAGING_CHANGED')
            saved=db.get('products',dict(product_key=item['product_key']))
            catalog=item['group']['tables']['product_catalog'][0]
            if (not saved or saved['source_code']!=catalog['source_code'] or saved['source_product_id']!=catalog['source_product_id']):
                raise Conflict('PUBLICATION_PRODUCT_STATE_CHANGED')
            _links(db,saved,item['group']['tables'],item,plan,at)
            count+=len(item['group']['tables']['product_links'])
    return dict(status='LINK_OBSERVATIONS_RECONCILED',products=len(plan['products']),links=count,plan_id=plan['plan_id'])


def audit(plan,db):
    from .audit import audit_reference, _sources
    from .policy import attribute_projection, uuid_hex
    from ..contracts import digest
    receipt=db.get('pipeline_publications',dict(plan_id=plan['plan_id']),locking=False)
    if not receipt or receipt['status']!='COMPLETED' or receipt['plan_hash']!=digest(plan):
        raise Conflict('PUBLICATION_PENDING_RECEIPT_CHANGED')
    images=0
    for item in plan['products']:
        staged=db.get('pipeline_publication_staging',dict(plan_id=plan['plan_id'],product_key=item['product_key']),locking=False)
        if not staged or staged['body_hash']!=digest(item) or parse(staged['body_json'])!=item:
            raise Conflict('PUBLICATION_STAGING_CHANGED')
        saved=db.get('products',dict(product_key=item['product_key']),locking=False)
        catalog=item['group']['tables']['product_catalog'][0]
        for expected in item['group']['tables']['product_links']:
            linked=db.get('product_link_observations',dict(product_id=saved['id'],
                link_observation_key=expected['link_observation_key'],gold_evaluation_key=expected['gold_evaluation_key']),locking=False)
            if not linked or any(linked.get(k)!=expected[k] for k in ('source_url','link_kind','offer_key','usable','raw_document_id')):
                raise Conflict('PUBLICATION_LINK_OBSERVATION_CHANGED')
        if (not saved or saved['status']!='PENDING_REVIEW' or saved['publication_status']!='PENDING_REVIEW' or
                saved['source_code']!=catalog['source_code'] or saved['source_product_id']!=catalog['source_product_id'] or
                any(saved[k] for k in READY_FIELDS) or saved['price_krw'] is not None or saved['comparison_url'] is not None or
                saved['representative_image_id'] is not None or parse(saved['embedding_json'])!={}):
            raise Conflict('PUBLICATION_PENDING_STATE_CHANGED')
        category=db.get('categories',dict(id=uuid_hex(plan['pending_category_id'])),locking=False)
        if (not category or saved['category_id']!=category['id'] or category['active'] or category['recognition_enabled'] or
                category['code']!='PIPELINE_UNMAPPED'):
            raise Conflict('PUBLICATION_PENDING_CATEGORY_CHANGED')
        audit_reference(db,saved['gold_snapshot_ref_json'])
        sources=_sources(db,parse(saved['gold_snapshot_ref_json']))
        for service,source in [('name','display_name'),('manufacturer','manufacturer'),('brand','brand'),('model_name','model'),
                               ('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm')]:
            value=saved.get(service)
            if value is None:continue
            matching=[r for r in sources if r.get(source) is not None and
                (Decimal(str(value))==Decimal(str(r[source])) if service.endswith('_mm') else value==r[source])]
            if not matching or not any(parse(saved['field_lineage_json']).get(source)==parse(r['field_lineage_json']).get(source)
                and parse(saved['field_status_json']).get(source)==parse(r['field_status_json']).get(source) for r in matching):
                raise Conflict('PUBLICATION_PENDING_PROJECTION_CHANGED')
        attributes=[]
        for row in reversed(sources):attributes=attribute_projection(attributes,parse(row['attributes_json']))
        if (parse(saved['attributes_json'])!=attributes or
                {canonical(r) for r in parse(saved['source_refs_json'])}!={canonical(r) for source in sources for r in parse(source['source_refs_json'])}):
            raise Conflict('PUBLICATION_PENDING_PROJECTION_CHANGED')
        for image in item['group']['tables']['product_images']:
            if not image.get('object_key') or not image.get('image_checksum'):continue
            link=db.get('product_images',dict(product_id=saved['id'],image_relation_key=image['image_relation_key']),locking=False)
            asset=db.get('assets',dict(bucket=image['bucket'],object_key=image['object_key']),locking=False)
            if (not link or not asset or link['asset_id']!=asset['id'] or link['active'] or
                    link['gold_recommendation_approved'] or asset['checksum']!=image['image_checksum'] or
                    link['source_url']!=image['source_url'] or parse(link['usage_review_json'])!={'decision':'NOT_REVIEWED'}):
                raise Conflict('PUBLICATION_PENDING_IMAGE_CHANGED')
            audit_reference(db,link['gold_snapshot_ref_json']);images+=1
    return dict(status='VERIFIED',scope='PENDING_REVIEW_INACTIVE',products=len(plan['products']),images=images)
