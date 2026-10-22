"""Read-only checks of durable publication receipts and current provenance."""
from decimal import Decimal
from ..contracts import canonical, digest
from ..errors import Conflict
from ..files import parse
from .plan import verify_plan
from .policy import attribute_projection


def sql_plan_audit(plan, db):
    """Check durable SQL receipts, product identities and immutable image/price links.

    Current values/approval fields may legitimately change later; do not equate
    an old plan to the current product projection or overwrite owner decisions.
    """
    from .plan import verify_plan
    verify_plan(plan)
    receipt=db.get('pipeline_publications',dict(plan_id=plan['plan_id']))
    if not receipt or receipt['status']!='COMPLETED' or receipt['plan_hash']!=digest(plan):
        raise Conflict('FLOW_SQL_PUBLICATION_MISSING_OR_CHANGED')
    images=offers=history=0
    for item in plan['products']:
        staged=db.get('pipeline_publication_staging',dict(plan_id=plan['plan_id'],product_key=item['product_key']))
        if not staged or staged['body_hash']!=digest(item) or parse(staged['body_json'])!=item:
            raise Conflict('FLOW_SQL_STAGING_MISSING_OR_CHANGED')
        product=db.get('products',dict(product_key=item['product_key']))
        catalog=item['group']['tables']['product_catalog'][0]
        if not product or any(product[k]!=catalog[k] for k in ('source_code','source_product_id')):
            raise Conflict('FLOW_SQL_PRODUCT_IDENTITY_CHANGED')
        for image in item['group']['tables']['product_images']:
            if not image['recommendation_approved']:continue
            asset=db.get('assets',dict(bucket=image['bucket'],object_key=image['object_key']))
            link=db.get('product_images',dict(product_id=product['id'],source_code=catalog['source_code'],
                source_product_id=catalog['source_product_id'],image_variant=image['image_slot'],
                raw_document_id=image['raw_document_id'],source_version=image['source_version']))
            source=parse(parse(image['evidence_json'])['evidence_json'])
            if (not asset or asset['checksum']!=image['image_checksum'] or not link or link['asset_id']!=asset['id']
                    or link['source_url']!=image['source_url'] or link['raw_payload_hash']!=source['raw_payload_hash']):
                raise Conflict('FLOW_SQL_IMAGE_LINK_CHANGED')
            images+=1
        for offer in item['group']['tables']['current_offers']:
            saved=db.get('offers',dict(offer_key=offer['offer_key']))
            if not saved or saved['product_id']!=product['id']:raise Conflict('FLOW_SQL_OFFER_LINK_CHANGED')
            offers+=1
        for row in item['group']['tables']['offer_history']:
            if not row['eligible']:continue
            key=row['snapshot_key'] if plan['price_history_mode']=='SNAPSHOT_V1' else row['price_effect_key']
            saved=db.get('offer_snapshots',dict(snapshot_key=key))
            from .policy import money
            offer=db.get('offers',dict(offer_key=row['offer_key']))
            if (not saved or not offer or saved['offer_id']!=offer['id']
                    or (plan['price_history_mode']=='SNAPSHOT_V1' and saved['raw_document_id']!=row['raw_document_id'])
                    or saved['price_amount']!=money(row['amount']) or saved['currency']!=row['currency']
                    or parse(saved['pricing_terms_json'])!=parse(row['evidence_json'])['price_effect']):
                raise Conflict('FLOW_SQL_PRICE_HISTORY_CHANGED')
            history+=1
    return dict(products=len(plan['products']),images=images,offers=offers,history=history)



def audit_reference(db, ref):
    """Resolve every reference against immutable staging, including older retained fields."""
    ref=parse(ref) if isinstance(ref,str) else ref
    required={'schema_version','plan_id','product_key','export_hash','gold_evaluation_key','input_fingerprint','gold_versions','source_rows'}
    if not required.issubset(ref) or ref['schema_version']!=2 or not ref['source_rows']:
        raise Conflict('PUBLICATION_SNAPSHOT_REFERENCE_INVALID')
    receipt=db.get('pipeline_publications',dict(plan_id=ref['plan_id']),locking=False)
    if not receipt or receipt['status']!='COMPLETED' or parse(receipt['gold_versions_json'])!=ref['gold_versions']:
        raise Conflict('PUBLICATION_SNAPSHOT_NOT_COMPLETED')
    staged=db.get('pipeline_publication_staging',dict(plan_id=ref['plan_id'],product_key=ref['product_key']),locking=False)
    if not staged or digest(parse(staged['body_json']))!=staged['body_hash']:
        raise Conflict('PUBLICATION_SNAPSHOT_STAGING_CHANGED')
    item=parse(staged['body_json']);group=item['group']
    catalog=group['tables']['product_catalog'][0]
    if (item['product_key']!=ref['product_key'] or catalog['gold_evaluation_key']!=ref['gold_evaluation_key']
            or catalog['input_fingerprint']!=ref['input_fingerprint']):
        raise Conflict('PUBLICATION_SNAPSHOT_IDENTITY_CHANGED')
    from .plan import validate_group
    validate_group(group,ref['gold_versions'])
    for proof in ref['source_rows']:
        rows=[group['receipt']] if proof['table']=='pipeline_evaluations' else group['tables'][proof['table']]
        if not any(r['row_key']==proof['row_key'] and r['record_hash']==proof['record_hash'] for r in rows):
            raise Conflict('PUBLICATION_SNAPSHOT_ROW_CHANGED')
    return group



def _sources(db, reference, *, depth=0, seen=None):
    if depth>64:raise Conflict('PUBLICATION_REFERENCE_DEPTH_EXCEEDED')
    ref=parse(reference) if isinstance(reference,str) else reference
    seen=set() if seen is None else seen
    key=(ref['plan_id'],ref['product_key'])
    if key in seen:raise Conflict('PUBLICATION_REFERENCE_CYCLE')
    seen=seen|{key}
    group=audit_reference(db,ref)
    result=[group['tables']['product_catalog'][0]]
    for prior in ref.get('retained_snapshot_refs',[]):
        if prior['product_key']!=ref['product_key']:raise Conflict('PUBLICATION_REFERENCE_PRODUCT_MISMATCH')
        result.extend(_sources(db,prior,depth=depth+1,seen=seen))
    return result



def audit_publication(plan,db):
    if plan.get('publication_mode')=='PENDING_REVIEW':
        verify_plan(plan)
        if plan['target']!=db.target:raise Conflict('PUBLICATION_TARGET_MISMATCH')
        from .pending import audit
        return audit(plan,db)
    verify_plan(plan)
    if plan['target']!=db.target:raise Conflict('PUBLICATION_TARGET_MISMATCH')
    class ReadOnly:
        def get(self,table,key,**kwargs):return db.get(table,key,locking=False)
    base=sql_plan_audit(plan,ReadOnly())
    checked=superseded=links=0
    for item in plan['products']:
        product=db.get('products',dict(product_key=item['product_key']),locking=False)
        catalog=item['group']['tables']['product_catalog'][0]
        ref=parse(product['gold_snapshot_ref_json'])
        sources=_sources(db,ref)
        if ref['plan_id']!=plan['plan_id']:
            superseded+=1;continue
        if (ref['export_hash']!=plan['export_hash'] or ref['gold_versions']!=plan['gold_versions']
                or ref['gold_evaluation_key']!=catalog['gold_evaluation_key']):
            raise Conflict('PUBLICATION_PRODUCT_SNAPSHOT_CHANGED')
        for field in ('catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready','publication_status'):
            if product[field]!=catalog[field]:raise Conflict('PUBLICATION_SERVICE_VALUE_CHANGED',field)
        for field in ('embedding_json','quality_reason_codes','publication_reason_codes'):
            expected=parse(catalog[field]) if isinstance(catalog[field],str) else catalog[field]
            if parse(product[field])!=expected:raise Conflict('PUBLICATION_SERVICE_JSON_CHANGED',field)
        current_lineage=parse(product['field_lineage_json']);current_status=parse(product['field_status_json'])
        for service,source in [('name','display_name'),('manufacturer','manufacturer'),('brand','brand'),('model_name','model'),
                               ('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm'),('price_krw','price_krw')]:
            value=product.get(service)
            if value is None:continue
            same=lambda candidate: (Decimal(str(value))==Decimal(str(candidate)) if service.endswith(('_mm','_krw')) else value==candidate)
            matching=[row for row in sources if row.get(source) is not None and same(row[source])]
            if not matching:raise Conflict('PUBLICATION_SERVICE_VALUE_CHANGED',service)
            if source!='price_krw' and not any(current_lineage.get(source)==parse(row['field_lineage_json']).get(source)
                and current_status.get(source)==parse(row['field_status_json']).get(source) for row in matching):
                raise Conflict('PUBLICATION_SERVICE_PROVENANCE_CHANGED',service)
        expected_attrs=[]
        for row in reversed(sources):expected_attrs=attribute_projection(expected_attrs,parse(row['attributes_json']))
        if parse(product['attributes_json'])!=expected_attrs:
            raise Conflict('PUBLICATION_SERVICE_ATTRIBUTE_CHANGED')
        expected_refs={canonical(ref) for row in sources for ref in parse(row['source_refs_json'])}
        if {canonical(ref) for ref in parse(product['source_refs_json'])}!=expected_refs:
            raise Conflict('PUBLICATION_SERVICE_SOURCE_REFS_CHANGED')
        if product['representative_image_id']:
            image=db.get('product_images',dict(id=product['representative_image_id']),locking=False)
            if not image or image['product_id']!=product['id'] or image['image_relation_key']!=catalog['representative_image_key']:
                raise Conflict('PUBLICATION_REPRESENTATIVE_IMAGE_CHANGED')
            audit_reference(db,image['gold_snapshot_ref_json'])
        for row in item['group']['tables']['current_offers']:
            saved=db.get('offers',dict(offer_key=row['offer_key']),locking=False)
            audit_reference(db,saved['gold_snapshot_ref_json'])
        for row in item['group']['tables']['offer_history']:
            if not row['eligible']:continue
            key=row['snapshot_key'] if plan['price_history_mode']=='SNAPSHOT_V1' else row['price_effect_key']
            saved=db.get('offer_snapshots',dict(snapshot_key=key),locking=False)
            audit_reference(db,saved['gold_snapshot_ref_json'])
        for row in item['group']['tables']['product_links']:
            saved=db.get('product_link_observations',dict(product_id=product['id'],
                link_observation_key=row['link_observation_key'],gold_evaluation_key=row['gold_evaluation_key']),locking=False)
            if not saved or any(saved[k]!=row[k] for k in ('source_url','link_kind','offer_key','usable','raw_document_id')):
                raise Conflict('PUBLICATION_SERVICE_LINK_CHANGED')
            if parse(saved['evidence_json'])!=parse(row['evidence_json']):raise Conflict('PUBLICATION_SERVICE_LINK_CHANGED')
            audit_reference(db,saved['gold_snapshot_ref_json']);links+=1
        checked+=1
    return dict(status='VERIFIED',scope='PINNED_GOLD_AND_SERVICE_PROJECTION',
                current_products=checked,superseded_products=superseded,link_observations=links,**base)
