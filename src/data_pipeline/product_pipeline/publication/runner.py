"""Stage pinned plans, lock current owner guards, then atomically publish products.

Only ETL columns are updated. Existing UUIDs, product status, image active/review
and asset usage policy remain owned by their original service/owner workflows.
"""
from decimal import Decimal
import time
from uuid import UUID,uuid4

from ..contracts import canonical,digest,utc
from ..errors import Conflict,PipelineError
from ..files import parse
from .mysql import sql_time
from .plan import verify_plan


def uuid_hex(value):
    try:return UUID(value).hex
    except (ValueError,TypeError) as exc:raise PipelineError('PUBLICATION_UUID_INVALID') from exc


def money(value):
    number=Decimal(str(value))
    if not number.is_finite() or number<=0 or number!=number.to_integral_value() or number>9223372036854775807:
        raise PipelineError('PUBLICATION_KRW_INTEGER_REQUIRED')
    return int(number)


def guarded_projection(old, incoming, lineage, versions):
    """Per-field event time, not arrival time. NULL never erases a verified value."""
    values={};proofs=dict(versions)
    for name,value in incoming.items():
        if value is None:continue
        proof=lineage.get(name) or {};stamp=proof.get('observed_at')
        previous=proofs.get(name)
        if previous and stamp:
            if utc(stamp)<previous['observed_at']:continue
            if utc(stamp)==previous['observed_at'] and digest(value)!=previous['value_hash']:
                raise Conflict('PUBLICATION_EQUAL_TIME_CONFLICT')
        if old and old.get(name) is not None and old[name]!=value and not stamp:
            raise PipelineError('PUBLICATION_FIELD_TIME_UNAVAILABLE')
        # Decimal DB values and canonical source decimal strings represent the same number.
        values[name]=value
        if stamp:proofs[name]=dict(observed_at=utc(stamp),value_hash=digest(value))
    return values,proofs


def _check_guard(db,item,catalog,at):
    guard=db.get('pipeline_publication_guards',dict(product_key=item['product_key']))
    if guard is None:raise PipelineError('PUBLICATION_OWNER_GUARD_NOT_PROVIDED')
    value=parse(guard['guard_json'])
    if digest(value)!=item['guard_hash']:raise Conflict('PUBLICATION_OWNER_GUARD_CHANGED')
    if (value.get('contract_version')!=1 or not value.get('evidence_ref') or
            value.get('gold_evaluation_key')!=catalog['gold_evaluation_key'] or
            value.get('input_fingerprint')!=catalog['input_fingerprint'] or
            uuid_hex(value.get('category_id'))!=uuid_hex(catalog['service_category_id']) or
            value.get('product_active') is not True or not value.get('expires_at') or sql_time(value['expires_at'])<=at):
        raise PipelineError('PUBLICATION_OWNER_GUARD_INVALID_OR_EXPIRED')
    if value.get('price_history_mode') not in {'SNAPSHOT_V1','PRICE_EFFECT_V2'}:
        raise PipelineError('PUBLICATION_HISTORY_POLICY_REQUIRED')
    category=db.get('categories',dict(id=uuid_hex(catalog['service_category_id'])))
    if not category or not category['active'] or category['code']!=catalog['service_category_code']:
        raise Conflict('PUBLICATION_CATEGORY_CHANGED')
    return value


def _product(db,item,plan,at):
    tables=item['group']['tables'];catalog=tables['product_catalog'][0]
    guard=_check_guard(db,item,catalog,at)
    if guard['price_history_mode']!=plan['price_history_mode']:raise Conflict('PUBLICATION_PRICE_HISTORY_POLICY_CHANGED')
    key=catalog['product_key'];old=db.get('products',dict(product_key=key))
    if old and (old['status']!='ACTIVE' or old['source_code']!=catalog['source_code'] or old['source_product_id']!=catalog['source_product_id']):
        raise Conflict('PUBLICATION_PRODUCT_STATE_CHANGED')
    version=db.get('pipeline_product_versions',dict(product_key=key))
    old_proofs=parse(version['field_versions_json']) if version else {}
    if old and version is None:raise PipelineError('PUBLICATION_EXISTING_PRODUCT_BASELINE_REQUIRED')
    incoming={name:catalog[name] for name in ('brand','width_mm','depth_mm','height_mm')}
    incoming.update(name=catalog['display_name'],model_name=catalog['model'])
    lineage=parse(catalog['field_lineage_json'])
    lineage={**lineage,'name':lineage.get('display_name'),'model_name':lineage.get('model')}
    # Normalize exact decimal representations before event-time conflict hashes.
    for field in ('width_mm','depth_mm','height_mm'):
        if incoming[field] is not None:incoming[field]=format(Decimal(str(incoming[field])).normalize(),'f')
    comparable=dict(old or {})
    for field in ('width_mm','depth_mm','height_mm'):
        if comparable.get(field) is not None:comparable[field]=format(Decimal(str(comparable[field])).normalize(),'f')
    values,proofs=guarded_projection(comparable,incoming,lineage,old_proofs)
    newer=not version or sql_time(plan['as_of'])>=version['latest_as_of']
    if newer:
        values['category_id']=uuid_hex(catalog['service_category_id'])
        # URL uses its own proof timestamp; plan evaluation time is not URL observation time.
        links=[r for r in tables['product_links'] if r['usable'] and r['link_kind']=='COMPARISON' and r['source_url']==catalog['comparison_url']]
        if links:
            link=max(links,key=lambda r:parse(r['evidence_json']).get('observed_at') or '')
            stamp=parse(link['evidence_json']).get('observed_at')
            expires=parse(link['evidence_json']).get('expires_at')
            if not expires or sql_time(expires)<=at:raise PipelineError('PUBLICATION_COMPARISON_REVIEW_EXPIRED')
            projected,linkproof=guarded_projection(old,dict(comparison_url=link['source_url']),
                dict(comparison_url=dict(observed_at=stamp)),{k:v for k,v in old_proofs.items() if k=='comparison_url'})
            values.update(projected);proofs.update(linkproof)
    if old:
        if values:db.update('products',dict(id=old['id']),dict(**values,updated_at=at))
        product=old
    else:
        if not incoming['name']:raise PipelineError('PUBLICATION_PRODUCT_NAME_REQUIRED')
        product,_=db.identity('products',dict(product_key=key),dict(**values,
            source_code=catalog['source_code'],source_product_id=catalog['source_product_id'],
            status='ACTIVE',dedup_status='SOURCE_IDENTITY_ONLY',created_at=at,updated_at=at))
    first=sql_time(catalog['first_collected_at'])
    if version and version['first_collected_at'] is not None:
        first=min(first,version['first_collected_at']) if first else version['first_collected_at']
    current=dict(field_versions_json=canonical(proofs),first_collected_at=first,
                 latest_as_of=sql_time(plan['as_of']) if newer else version['latest_as_of'],
                 latest_evaluation=catalog['gold_evaluation_key'] if newer else version['latest_evaluation'])
    if version:db.update('pipeline_product_versions',dict(product_key=key),current)
    else:db.identity('pipeline_product_versions',dict(product_key=key),current)
    _images(db,product,catalog,tables,guard,at,newer)
    _offers(db,product,catalog,tables,guard,at,plan['price_max_age_seconds'])
    return product['id']


def _images(db,product,catalog,tables,guard,at,newer):
    for image in tables['product_images']:
        if not image['recommendation_approved']:continue
        owner=guard.get('image_reviews',{}).get(image['image_relation_key'])
        if not owner or owner.get('snapshot_hash')!=image['review_snapshot_hash'] or not isinstance(owner.get('usage_review'),dict):
            raise PipelineError('PUBLICATION_IMAGE_OWNER_REVIEW_REQUIRED')
        evidence=parse(image['evidence_json']);source=parse(evidence['evidence_json'])
        if not source.get('raw_payload_hash') or not source.get('raw_collected_at'):
            raise PipelineError('PUBLICATION_IMAGE_RAW_PROVENANCE_REQUIRED')
        if any(not evidence.get(k) for k in ('bytes','width','height','format')):
            raise PipelineError('PUBLICATION_IMAGE_METADATA_REQUIRED')
        asset,created=db.identity('assets',dict(bucket=image['bucket'],object_key=image['object_key']),dict(
            owner_id=None,kind='PRODUCT_IMAGE',content_type='image/'+evidence['format'].lower().replace('jpg','jpeg'),
            size_bytes=evidence['bytes'],width_px=evidence['width'],height_px=evidence['height'],
            checksum=image['image_checksum'],status='AVAILABLE',usage_policy_json=canonical({'source':'OWNER_REVIEW_REQUIRED'}),
            created_at=at,updated_at=at))
        if (asset['checksum']!=image['image_checksum'] or asset['size_bytes']!=evidence['bytes'] or asset['status']!='AVAILABLE' or
                asset['width_px']!=evidence['width'] or asset['height_px']!=evidence['height']):
            raise Conflict('PUBLICATION_ASSET_CONFLICT')
        link,created=db.identity('product_images',dict(product_id=product['id'],source_code=catalog['source_code'],
            source_product_id=catalog['source_product_id'],image_variant=image['image_slot'],
            raw_document_id=image['raw_document_id'],source_version=image['source_version']),
            dict(asset_id=asset['id'],display_order=image['display_order'],source_url=image['source_url'],
                 raw_payload_hash=source['raw_payload_hash'],raw_collected_at=sql_time(source['raw_collected_at']),linked_at=at,
                 active=False,usage_review_json=canonical(owner['usage_review']),created_at=at,updated_at=at))
        if (link['asset_id']!=asset['id'] or link['raw_document_id']!=image['raw_document_id'] or link['source_url']!=image['source_url'] or
                link['raw_payload_hash']!=source['raw_payload_hash']):raise Conflict('PUBLICATION_IMAGE_LINEAGE_CONFLICT')
        if not created and digest(parse(link['usage_review_json']))!=owner.get('current_review_hash'):
            raise Conflict('PUBLICATION_IMAGE_REVIEW_CHANGED')
        # Existing active/review fields are NEVER overwritten; new links stay inactive
        # until the service owner explicitly activates them.
        if not created and newer:db.update('product_images',dict(id=link['id']),dict(display_order=image['display_order'],updated_at=at))


def _offers(db,product,catalog,tables,guard,at,max_age):
    grouped={}
    for offer in tables['current_offers']:
        if offer['offer_key'] in grouped:raise PipelineError('PUBLICATION_MULTIPLE_CURRENT_CONDITIONS')
        grouped[offer['offer_key']]=offer
    for key,offer in grouped.items():
        proof=parse(offer['evidence_json']);price=proof['price'];identity=proof['identity']
        if not all(identity.get(k) for k in ('source_seller_id','source_offer_id','option_key')):
            raise PipelineError('PUBLICATION_STABLE_OFFER_IDENTITY_REQUIRED')
        stamp=sql_time(offer['observed_at']);amount=money(offer['amount'])
        if not stamp or not 0<=(at-stamp).total_seconds()<=max_age:raise PipelineError('PUBLICATION_PRICE_EXPIRED')
        if offer['purchase_url']:
            matching=[parse(r['evidence_json']) for r in tables['product_links'] if r['usable'] and
                r['offer_key']==key and r['source_url']==offer['purchase_url'] and r['link_kind']=='DIRECT_PURCHASE']
            if not matching or any(not r.get('expires_at') or sql_time(r['expires_at'])<=at for r in matching):
                raise PipelineError('PUBLICATION_PURCHASE_REVIEW_EXPIRED')
        values=dict(product_id=product['id'],source_code=catalog['source_code'],source_product_id=catalog['source_product_id'],
            source_seller_id=identity['source_seller_id'],source_offer_id=identity['source_offer_id'],source_variant_id=identity['option_key'],
            title=None,price_amount=amount,currency=offer['currency'],purchase_url=offer['purchase_url'],availability='UNKNOWN',
            collected_at=stamp,latest_raw_document_id=offer['raw_document_id'],eligible=True,price_basis='REGULAR',
            price_conditions_json=price['condition_json'],option_attributes_json=canonical(dict(option_key=identity['option_key'])),created_at=at,updated_at=at)
        row,created=db.identity('offers',dict(offer_key=key),values)
        if row['product_id']!=product['id'] or any(row[k]!=values[k] for k in ('source_seller_id','source_offer_id','source_variant_id')):
            raise Conflict('PUBLICATION_OFFER_IDENTITY_CONFLICT')
        if not created:
            if row['collected_at']==stamp and (row['price_amount']!=amount or row['currency']!=offer['currency'] or
                parse(row['price_conditions_json'])!=parse(price['condition_json'])):raise Conflict('PUBLICATION_PRICE_EQUAL_TIME_CONFLICT')
            if row['collected_at']<stamp:
                current={k:v for k,v in values.items() if k not in {'created_at','product_id','source_code','source_product_id','source_seller_id','source_offer_id','source_variant_id'}}
                db.update('offers',dict(id=row['id']),current)
    for history in tables['offer_history']:
        if not history['eligible']:continue
        offer=db.get('offers',dict(offer_key=history['offer_key']))
        if not offer or offer['product_id']!=product['id']:raise PipelineError('PUBLICATION_HISTORY_OFFER_UNAVAILABLE')
        key=history['snapshot_key'] if guard['price_history_mode']=='SNAPSHOT_V1' else history['price_effect_key']
        values=dict(offer_id=offer['id'],raw_document_id=history['raw_document_id'],price_amount=money(history['amount']),
            currency=history['currency'],purchase_url=None,availability='UNKNOWN',collected_at=sql_time(history['observed_at']),
            pricing_terms_json=canonical(parse(history['evidence_json'])['price_effect']),created_at=at)
        old,created=db.identity('offer_snapshots',dict(snapshot_key=key),values)
        if not created and any(old[k]!=values[k] for k in ('offer_id','price_amount','currency','collected_at')):
            raise Conflict('PUBLICATION_IMMUTABLE_HISTORY_CONFLICT')
        if not created and parse(old['pricing_terms_json'])!=parse(values['pricing_terms_json']):
            raise Conflict('PUBLICATION_HISTORY_TERMS_CONFLICT')


def publish(plan,db,*,hook=None):
    started=time.monotonic();verify_plan(plan)
    if plan['target']!=db.target:raise Conflict('PUBLICATION_TARGET_MISMATCH')
    db.preflight();identity=dict(plan_id=plan['plan_id'])
    # Durable staging transaction is separate from the atomic service publication.
    with db.transaction():
        saved,_=db.identity('pipeline_publications',identity,dict(plan_hash=digest(plan),status='STAGED',
            gold_versions_json=canonical(plan['gold_versions']),product_count=len(plan['products']),completed_at=None))
        if (saved['plan_hash']!=digest(plan) or parse(saved['gold_versions_json'])!=plan['gold_versions'] or
            saved['product_count']!=len(plan['products'])):raise Conflict('PUBLICATION_PLAN_RECEIPT_CONFLICT')
        for item in plan['products']:
            key=dict(**identity,product_key=item['product_key']);old=db.get('pipeline_publication_staging',key)
            body=canonical(item)
            if old and (old['body_hash']!=digest(item) or parse(old['body_json'])!=item):raise Conflict('PUBLICATION_STAGING_CHANGED')
            if not old:db.identity('pipeline_publication_staging',key,dict(body_hash=digest(item),body_json=body))
    if hook:hook('STAGED')
    with db.transaction():
        receipt=db.get('pipeline_publications',identity)
        if receipt['status']=='COMPLETED':return dict(status='REUSED',publication_id=receipt['id'],plan_id=plan['plan_id'],duration_ms=round((time.monotonic()-started)*1000))
        at=db.one('SELECT UTC_TIMESTAMP(3) AS stamp')['stamp'];ids={}
        for item in plan['products']:
            staged=db.get('pipeline_publication_staging',dict(**identity,product_key=item['product_key']))
            if staged['body_hash']!=digest(item) or parse(staged['body_json'])!=item:raise Conflict('PUBLICATION_STAGING_CHANGED')
            ids[item['product_key']]=_product(db,item,plan,at)
            if hook:hook('PRODUCT_WRITTEN')
        db.update('pipeline_publications',identity,dict(status='COMPLETED',completed_at=at))
        if hook:hook('BEFORE_COMMIT')
    return dict(status='COMPLETED',plan_id=plan['plan_id'],publication_id=receipt['id'],products=len(ids),
                product_ids=ids,duration_ms=round((time.monotonic()-started)*1000))
