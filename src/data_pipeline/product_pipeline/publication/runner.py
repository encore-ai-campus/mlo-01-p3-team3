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
        if previous and not stamp and old and old.get(name)==value:continue
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
    activating=bool(old and old['status']=='PENDING_REVIEW' and plan.get('publication_mode')=='ACTIVATE_REVIEWED')
    if old and plan.get('publication_mode')=='ACTIVATE_REVIEWED':
        approval=guard.get('activation') or {}
        if (approval.get('from_status')!=old['status'] or old['status'] not in {'PENDING_REVIEW','ACTIVE'} or approval.get('product_id')!=old['id'] or
                approval.get('current_snapshot_hash')!=digest(parse(old['gold_snapshot_ref_json'])) or not approval.get('evidence_ref')):
            raise Conflict('PUBLICATION_ACTIVATION_BASELINE_CHANGED')
    if old and ((old['status']!='ACTIVE' and not activating) or old['source_code']!=catalog['source_code'] or old['source_product_id']!=catalog['source_product_id']):
        raise Conflict('PUBLICATION_PRODUCT_STATE_CHANGED')
    version=db.get('pipeline_product_versions',dict(product_key=key))
    old_proofs=parse(version['field_versions_json']) if version else {}
    if old and version is None:raise PipelineError('PUBLICATION_EXISTING_PRODUCT_BASELINE_REQUIRED')
    incoming={name:catalog[name] for name in ('manufacturer','brand','width_mm','depth_mm','height_mm')}
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
    if activating and not newer:raise Conflict('PUBLICATION_ACTIVATION_STALE')
    values.update(extension_values(old,catalog,values,newer=newer))
    first=sql_time(catalog['first_collected_at'])
    if version and version['first_collected_at'] is not None:
        first=min(first,version['first_collected_at']) if first else version['first_collected_at']
    if newer:
        values['first_collected_at']=first
        price=catalog['price_krw']
        if price is not None:
            matching=[r for r in tables['current_offers'] if r['currency']=='KRW' and money(r['amount'])==money(price)]
            if not matching:raise PipelineError('PUBLICATION_REPRESENTATIVE_PRICE_PROOF_REQUIRED')
            price_row=max(matching,key=lambda r:r['observed_at'])
            if not 0<=(at-sql_time(price_row['observed_at'])).total_seconds()<=plan['price_max_age_seconds']:
                raise PipelineError('PUBLICATION_PRICE_EXPIRED')
            more,priceproof=guarded_projection(old,dict(price_krw=money(price)),
                dict(price_krw=dict(observed_at=price_row['observed_at'])),old_proofs)
            values.update(more);proofs.update({k:v for k,v in priceproof.items() if k=='price_krw'})
    if newer:
        values['category_id']=uuid_hex(catalog['service_category_id'])
        if activating:values['status']='ACTIVE'
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
        values['gold_snapshot_ref_json']=product_reference(item,plan,old,values)
    if old:
        if values:db.update('products',dict(id=old['id']),dict(**values,updated_at=at))
        product=old
    else:
        if not incoming['name']:raise PipelineError('PUBLICATION_PRODUCT_NAME_REQUIRED')
        product,_=db.identity('products',dict(product_key=key),dict(**values,
            source_code=catalog['source_code'],source_product_id=catalog['source_product_id'],
            status='ACTIVE',dedup_status='SOURCE_IDENTITY_ONLY',created_at=at,updated_at=at))
    current=dict(field_versions_json=canonical(proofs),first_collected_at=first,
                 latest_as_of=sql_time(plan['as_of']) if newer else version['latest_as_of'],
                 latest_evaluation=catalog['gold_evaluation_key'] if newer else version['latest_evaluation'])
    if version:db.update('pipeline_product_versions',dict(product_key=key),current)
    else:db.identity('pipeline_product_versions',dict(product_key=key),current)
    image_ids=_images(db,product,catalog,tables,guard,at,newer,item,plan)
    if plan.get('publication_mode')=='ACTIVATE_REVIEWED':
        for relation in (guard.get('activation') or {}).get('image_relations',[]):
            if relation not in image_ids:raise PipelineError('PUBLICATION_ACTIVATION_IMAGE_UNAPPROVED')
            image=db.get('product_images',dict(id=image_ids[relation]))
            review=parse(image['usage_review_json'])
            if review.get('decision')!='APPROVED' or 'DISPLAY' not in review.get('scopes',[]):
                raise PipelineError('PUBLICATION_ACTIVATION_IMAGE_UNAPPROVED')
            db.update('product_images',dict(id=image['id']),dict(active=True,updated_at=at))
    if newer:
        representative=catalog['representative_image_key']
        if representative is not None and representative not in image_ids:
            raise PipelineError('PUBLICATION_REPRESENTATIVE_IMAGE_UNAVAILABLE')
        db.update('products',dict(id=product['id']),dict(representative_image_id=image_ids.get(representative)))
    _links(db,product,tables,item,plan,at)
    _offers(db,product,catalog,tables,guard,at,plan['price_max_age_seconds'],item,plan)
    return product['id']


def _images(db,product,catalog,tables,guard,at,newer,item,plan):
    image_ids={}
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
        if plan.get('publication_mode')=='ACTIVATE_REVIEWED' and owner.get('asset_policy_hash')!=digest(parse(asset['usage_policy_json'])):
            raise Conflict('PUBLICATION_ASSET_POLICY_CHANGED')
        link,created=db.identity('product_images',dict(product_id=product['id'],source_code=catalog['source_code'],
            source_product_id=catalog['source_product_id'],image_variant=image['image_slot'],
            raw_document_id=image['raw_document_id'],source_version=image['source_version']),
            dict(asset_id=asset['id'],display_order=image['display_order'],source_url=image['source_url'],
                 raw_payload_hash=source['raw_payload_hash'],raw_collected_at=sql_time(source['raw_collected_at']),linked_at=at,
                 active=False,usage_review_json=canonical(owner['usage_review']),created_at=at,updated_at=at,
                 image_relation_key=image['image_relation_key'],gold_recommendation_approved=image['recommendation_approved'],
                 gold_review_snapshot_hash=image['review_snapshot_hash'],source_evidence_json=image['evidence_json'],
                 gold_snapshot_ref_json=snapshot_ref(item,plan,[('product_images',image)])))
        if (link['asset_id']!=asset['id'] or link['raw_document_id']!=image['raw_document_id'] or link['source_url']!=image['source_url'] or
                link['raw_payload_hash']!=source['raw_payload_hash']):raise Conflict('PUBLICATION_IMAGE_LINEAGE_CONFLICT')
        if not created and digest(parse(link['usage_review_json']))!=owner.get('current_review_hash'):
            raise Conflict('PUBLICATION_IMAGE_REVIEW_CHANGED')
        # Existing active/review fields are NEVER overwritten; new links stay inactive
        # until the service owner explicitly activates them.
        if link['image_relation_key']!=image['image_relation_key']:
            if plan.get('publication_mode')!='ACTIVATE_REVIEWED':raise Conflict('PUBLICATION_IMAGE_RELATION_CHANGED')
            db.update('product_images',dict(id=link['id']),dict(image_relation_key=image['image_relation_key']))
        if not created and newer:db.update('product_images',dict(id=link['id']),dict(display_order=image['display_order'],updated_at=at,
            gold_recommendation_approved=image['recommendation_approved'],gold_review_snapshot_hash=image['review_snapshot_hash'],
            source_evidence_json=image['evidence_json'],gold_snapshot_ref_json=snapshot_ref(item,plan,[('product_images',image)])))
        image_ids[image['image_relation_key']]=link['id']
    return image_ids


def _links(db,product,tables,item,plan,at):
    for link in tables['product_links']:
        key=dict(product_id=product['id'],link_observation_key=link['link_observation_key'],gold_evaluation_key=link['gold_evaluation_key'])
        values={k:link[k] for k in ('link_kind','source_url','offer_key','usable','raw_document_id','evidence_json')}
        old,created=db.identity('product_link_observations',key,dict(**values,
            gold_snapshot_ref_json=snapshot_ref(item,plan,[('product_links',link)]),created_at=at))
        if not created and any((parse(old[k])!=parse(v) if k=='evidence_json' else old[k]!=v) for k,v in values.items()):
            raise Conflict('PUBLICATION_LINK_OBSERVATION_CHANGED')


def _offers(db,product,catalog,tables,guard,at,max_age,item,plan):
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
            price_conditions_json=price['condition_json'],option_attributes_json=canonical(dict(option_key=identity['option_key'])),created_at=at,updated_at=at,
            gold_snapshot_ref_json=snapshot_ref(item,plan,[('current_offers',offer)]))
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
            pricing_terms_json=canonical(parse(history['evidence_json'])['price_effect']),created_at=at,
            gold_snapshot_ref_json=snapshot_ref(item,plan,[('offer_history',history)]))
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
            if plan.get('publication_mode')=='PENDING_REVIEW':
                from .pending import product as pending_product
                ids[item['product_key']]=pending_product(db,item,plan,at)
            else:ids[item['product_key']]=_product(db,item,plan,at)
            if hook:hook('PRODUCT_WRITTEN')
        db.update('pipeline_publications',identity,dict(status='COMPLETED',completed_at=at))
        if hook:hook('BEFORE_COMMIT')
    return dict(status='COMPLETED',plan_id=plan['plan_id'],publication_id=receipt['id'],products=len(ids),
                product_ids=ids,duration_ms=round((time.monotonic()-started)*1000))


def snapshot_ref(item, plan, source_rows):
    catalog = item['group']['tables']['product_catalog'][0]
    return canonical(dict(schema_version=2, plan_id=plan['plan_id'], product_key=item['product_key'],
        export_hash=plan['export_hash'], gold_evaluation_key=catalog['gold_evaluation_key'],
        input_fingerprint=catalog['input_fingerprint'], gold_versions=plan['gold_versions'],
        source_rows=[dict(table=table,row_key=row['row_key'],record_hash=row['record_hash'])
                     for table,row in source_rows]))


def product_reference(item,plan,old,values):
    catalog=item['group']['tables']['product_catalog'][0]
    ref=parse(snapshot_ref(item,plan,[('product_catalog',catalog),('pipeline_evaluations',item['group']['receipt'])]))
    if not old:return canonical(ref)
    retained=False
    for service,source in [('name','display_name'),('model_name','model'),('manufacturer','manufacturer'),('brand','brand'),
                           ('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm'),
                           ('price_krw','price_krw'),('comparison_url','comparison_url')]:
        actual=values.get(service,old.get(service));incoming=catalog.get(source)
        if actual is not None:
            different=incoming is None or (Decimal(str(actual))!=Decimal(str(incoming)) if service.endswith(('_mm','_krw')) else actual!=incoming)
            retained=retained or different
    retained=retained or any(parse(values[name])!=parse(catalog[name]) for name in ('attributes_json','field_lineage_json','source_refs_json'))
    retained=retained or values.get('first_collected_at')!=sql_time(catalog['first_collected_at'])
    if retained:
        previous=parse(old['gold_snapshot_ref_json'])
        refs=[{k:v for k,v in previous.items() if k!='retained_snapshot_refs'},*previous.get('retained_snapshot_refs',[])]
        unique={r['gold_evaluation_key']:r for r in refs if r['gold_evaluation_key']!=ref['gold_evaluation_key']}
        ref['retained_snapshot_refs']=list(unique.values())
        refs=parse(values['source_refs_json'])+parse(old['source_refs_json'])
        values['source_refs_json']=canonical(list({canonical(r):r for r in refs}.values()))
    return canonical(ref)


def attribute_projection(previous, incoming):
    """A failed/newer scrape must not erase a prior confirmed option attribute."""
    if not isinstance(previous,list) or not isinstance(incoming,list):
        raise PipelineError('PUBLICATION_ATTRIBUTE_ARRAY_REQUIRED')
    def key(row):
        return canonical([row.get(k) for k in ('attribute_name','measurement_target','component','option_key')])
    retained = [r for r in previous if r.get('attribute_status')=='CONFIRMED']
    result = list(incoming)
    for old in retained:
        identity=key(old)
        candidates=[r for r in incoming if key(r)==identity and r.get('attribute_status')=='CONFIRMED']
        if not candidates:
            if old not in result: result.append(old)
            continue
        old_time=old.get('observed_at')
        new_time=max((r.get('observed_at') or '' for r in candidates),default='')
        semantic=lambda r: [r.get(k) for k in ('value_type','value_text','value_decimal','value_min','value_max','codes','unit')]
        if old_time and new_time and utc(new_time)==utc(old_time) and any(semantic(r)!=semantic(old) for r in candidates if r.get('observed_at') and utc(r['observed_at'])==utc(old_time)):
            raise Conflict('PUBLICATION_ATTRIBUTE_EQUAL_TIME_CONFLICT')
        if not new_time and any(semantic(r)!=semantic(old) for r in candidates):
            raise PipelineError('PUBLICATION_ATTRIBUTE_TIME_UNAVAILABLE')
        if old_time and (not new_time or utc(new_time)<utc(old_time)):
            result=[r for r in result if key(r)!=identity or r.get('attribute_status')!='CONFIRMED']+[old]
    return result


def extension_values(old, catalog, values, *, newer):
    """Retained scalar values carry their original state and lineage, never new proof."""
    if old and not newer: return {}
    lineage=parse(catalog['field_lineage_json']);statuses=parse(catalog['field_status_json'])
    previous_lineage=parse(old['field_lineage_json']) if old else {}
    previous_status=parse(old['field_status_json']) if old else {}
    for service, source in [('name','display_name'),('model_name','model'),('manufacturer','manufacturer'),
                            ('brand','brand'),('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm')]:
        if old and old.get(service) is not None and service not in values:
            lineage[source]=previous_lineage.get(source)
            statuses[source]=previous_status.get(source,'UNRESOLVED')
    incoming=parse(catalog['attributes_json'])
    attrs=attribute_projection(parse(old['attributes_json']) if old else [], incoming)
    refs=parse(catalog['source_refs_json'])
    retained_scalar=old and any(old.get(service) is not None and service not in values for service in
        ('name','model_name','manufacturer','brand','width_mm','depth_mm','height_mm'))
    if old and (retained_scalar or attrs!=incoming or (old.get('price_krw') is not None and catalog['price_krw'] is None)):
        refs=list({canonical(r):r for r in [*parse(old['source_refs_json']),*refs]}.values())
    return dict(attributes_json=canonical(attrs),field_status_json=canonical(statuses),
        field_lineage_json=canonical(lineage),source_refs_json=canonical(refs),
        catalog_ready=catalog['catalog_ready'],recommendation_ready=catalog['recommendation_ready'],
        comparison_link_ready=catalog['comparison_link_ready'],purchase_link_ready=catalog['purchase_link_ready'],
        quality_reason_codes=canonical(catalog['quality_reason_codes']),publication_status=catalog['publication_status'],
        publication_reason_codes=canonical(catalog['publication_reason_codes']),embedding_json=catalog['embedding_json'])


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
    from ..operations.flow_audit import sql_plan_audit
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
