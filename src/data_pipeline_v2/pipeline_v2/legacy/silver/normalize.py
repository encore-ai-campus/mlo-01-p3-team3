"""Pure Danawa observation normalization with explicit provenance and quality waits."""
import re
from urllib.parse import urlsplit
from uuid import UUID

from ..bronze.contracts import validate_raw
from ..contracts import canonical, digest, event_for, now, utc, valid_product_url
from ..errors import Conflict, PipelineError
from .inputs import image_state, project
from .values import decimal, dimensions, display_money, measurement, money, registered
from .schemas import complete,TIMES

TABLES=('product_observations','product_attributes','price_observations','product_images',
        'link_observations','quality_issues','normalized_offers')
KEYS=dict(product_observations='evaluation_key',product_attributes='attribute_key',
          price_observations='price_observation_key',product_images='image_relation_key',
          link_observations='link_observation_key',quality_issues='issue_key',
          normalized_offers='normalized_offer_key',pipeline_evaluations='evaluation_key')
VERSION='danawa-silver-1.0.4'
POLICY='silver-quality-1'
DIAGNOSTICS={'record_hash','processing_run_id','processed_at','completed_at','image_state_checked_at'}


def semantic_hash(row):
    return digest({k:v for k,v in row.items() if k not in DIAGNOSTICS})


def evidence(doc, path, value):
    return digest(dict(raw_document_id=doc['_id'],source_path=path,source_value_hash=digest(value)))


def source_time(value):
    return (utc(value),'OBSERVED') if value else (None,'UNKNOWN')


def evaluation_input(doc,state,inputs,as_of,raw_verified=False):
    """Identity shared by normalization and observation-specific reuse checks.

    raw_verified: the caller already proved this exact raw passed validate_raw (same content hash);
    the check is pure, so running it again cannot change the result."""
    utc(as_of)
    if not raw_verified:
        validate_raw(doc,event_for(doc,'00000000-0000-4000-8000-000000000001',doc['collected_at']))
    image=image_state(doc,state)
    related=project(doc,inputs,as_of)
    normalizer_version=VERSION+':'+digest(related['rules'])
    gallery_hash=digest(dict(slots=doc['image_refs'], freshness_policy='NOT_EVALUATED'))
    fingerprint=digest(dict(stage='SILVER',raw_document_id=doc['_id'],document_hash=doc['document_hash'],
        image_state_revision=image['revision'],image_state_fingerprint=image['fingerprint'],
        gallery_selection_evidence_hash=gallery_hash,mapping_snapshot_hash=digest(related['mappings']),
        review_snapshot_hash=digest(related['reviews']),link_validation_snapshot_hash=digest(related['links']),
        normalizer_version=normalizer_version,quality_policy_version=POLICY,gold_policy_version=None,contract_version=1))
    eval_key='silver-eval-v1:'+digest(dict(stage='SILVER',raw_document_id=doc['_id'],processing_input_fingerprint=fingerprint))
    return dict(evaluation_key=eval_key,processing_input_fingerprint=fingerprint,image_state=image),related,normalizer_version


def normalize(doc, state, inputs, *, processing_run_id, as_of, processed_at=None, raw_verified=False):
    UUID(processing_run_id)
    identity,related,normalizer_version=evaluation_input(doc,state,inputs,as_of,raw_verified)
    image=identity['image_state'];fingerprint=identity['processing_input_fingerprint'];eval_key=identity['evaluation_key']
    rules=[r for r in related['rules']['rows'] if r['effective_status']=='ACTIVE']
    common=dict(evaluation_key=eval_key,raw_document_id=doc['_id'],document_hash=doc['document_hash'],
        observation_key=doc['observation_key'],source_code=doc['source_code'],source_product_id=doc['source_product_id'],
        product_key='danawa:'+doc['source_product_id'],collection_run_id=doc['collection_run_id'],
        processing_input_fingerprint=fingerprint,normalizer_version=normalizer_version,quality_policy_version=POLICY,
        processing_run_id=processing_run_id,schema_version='silver-1',processed_at=processed_at or now())
    tables={t:[] for t in TABLES}

    def add(table, body, observed_at=None, field_status=None, proof=None):
        observed,time_status=source_time(observed_at)
        row=dict(common,observed_at=observed,time_status=time_status,
                 field_status_json=canonical(field_status or {}),evidence_json=canonical(proof or {}),**body)
        row=complete(table,row)
        for name_ in TIMES & set(row):
            if row[name_] is not None:row[name_]=utc(row[name_])
        row['record_hash']=semantic_hash(row)
        tables[table].append(row)
        return row

    def issue(code,table='product_observations',target=None,field='',raw=None,scope='FIELD',severity='WARNING'):
        body=dict(target_table=table,target_key=target or eval_key,field_name=field,issue_code=code,
                  severity=severity,blocking_scope=scope,raw_value_json=canonical(raw),reason=code)
        body['issue_key']='silver-issue-v1:'+digest(dict(evaluation_key=eval_key,target_key=body['target_key'],
            field_name=field,issue_code=code,evidence_hash=digest(raw)))
        add('quality_issues',body,proof=dict(source_value=raw))

    listings=doc['payload']['listings']; detail=doc['payload'].get('detail') or {}
    successful=bool(detail) and detail.get('status','ok')=='ok' and doc['collector_metadata'].get('detail_status')=='ok'
    selected=detail.get('selected_product') or {}
    if not selected:
        selected=next((r for r in detail.get('json_ld') or [] if isinstance(r,dict) and
            (str(r.get('sku',''))==doc['source_product_id'] or valid_product_url(str(r.get('url','')),doc['source_product_id']))),{})
    if selected and str(selected.get('sku',doc['source_product_id']))!=doc['source_product_id']:
        raise Conflict('SILVER_SOURCE_PRODUCT_ID_CONFLICT')
    apis=[]
    for i, listing in enumerate(listings):
        api=listing.get('api_product') or {}
        if str(listing.get('product_id',doc['source_product_id'])) != doc['source_product_id'] or (
                api.get('productCode') is not None and str(api['productCode'])!=doc['source_product_id']):
            raise Conflict('SILVER_SOURCE_PRODUCT_ID_CONFLICT')
        apis.append(api)
    api=apis[0] if apis else {}
    name_candidates=[]
    if successful and selected.get('name'):
        name_candidates.append(dict(value=selected['name'],priority=0,path='/payload/detail/selected_product/name',time=detail.get('collected_at')))
    for index,(listing,a) in enumerate(zip(listings,apis)):
        if a.get('name',{}).get('full'):
            name_candidates.append(dict(value=a['name']['full'],priority=1,path=f'/payload/listings/{index}/api_product/name/full',time=listing.get('collected_at')))
        if listing.get('product_name'):
            name_candidates.append(dict(value=listing['product_name'],priority=2,path=f'/payload/listings/{index}/product_name',time=listing.get('collected_at')))
    best=[r for r in name_candidates if r['priority']==min((c['priority'] for c in name_candidates),default=-1)]
    distinct={' '.join(str(r['value']).split()) for r in best}
    name=next(iter(distinct)) if len(distinct)==1 else None
    if not name: issue('NAME_CONFLICT' if distinct else 'NAME_MISSING',field='name',raw=name_candidates,scope='CATALOG')
    category_paths=[]
    for a in apis:
        c=a.get('category') or {}
        if c:
            codes,names=c.get('codes',[]),c.get('names',[])
            if len(codes)!=len(names): issue('CATEGORY_PATH_CONFLICT',field='source_category',raw=c,scope='CATALOG')
            category_paths.append(dict(namespace='API_CATEGORY_PATH',codes=[str(x) for x in codes],names=names))
    mapped=[r for r in related['mappings']['rows'] if r['effective_status']=='ACTIVE' and r.get('active') is True]
    unique={(r.get('service_category_id'),r.get('service_category_code')) for r in mapped if not r.get('excluded')}
    mapping_status='EXCLUDED' if any(r.get('excluded') for r in mapped) else ('MAPPED' if len(unique)==1 else ('AMBIGUOUS' if len(unique)>1 else 'UNMAPPED'))
    service_id,service_code=next(iter(unique)) if mapping_status=='MAPPED' else (None,None)
    if mapping_status=='MAPPED':
        if not service_id or not service_code: raise PipelineError('SILVER_MAPPING_ROW_INVALID')
        UUID(service_id)
    else: issue('CATEGORY_'+mapping_status,field='category',raw=related['mappings'],scope='CATALOG')
    dates=api.get('dates') or {}; registered_raw=dates.get('register')
    if not registered_raw:
        registered_raw=next((l.get('registered_month_text') or l.get('registered_month') for l in listings if l.get('registered_month_text') or l.get('registered_month')),detail.get('registered_month'))
    year,month,reg_status=registered(registered_raw)
    if reg_status=='AMBIGUOUS':issue('REGISTERED_MONTH_AMBIGUOUS',field='registered_month',raw=registered_raw)
    pname=api.get('name') or {}
    brand=pname.get('brand') or None
    if not brand and successful and selected.get('brand'):
        brand=selected['brand'].get('name') if isinstance(selected['brand'],dict) else selected['brand']
    def listing_value(field):return next((l[field] for l in listings if l.get(field)),None)
    product=dict(display_name=name,normalized_name=name,manufacturer=pname.get('maker') or
        (detail.get('manufacturer') if successful else None) or listing_value('manufacturer'),
        brand=brand or listing_value('brand'),model=pname.get('model') or listing_value('model'),source_category_path_json=canonical(category_paths),
        collection_category_context_json=canonical(doc['collector_metadata'].get('categories') or []),
        service_category_id=service_id,service_category_code=service_code,mapping_status=mapping_status,
        mapping_version=digest(related['mappings']),registered_raw=registered_raw,registered_year=year,registered_month=month,
        registered_status=reg_status,provider_make_raw=dates.get('make'),provider_make_status='SEMANTICS_UNKNOWN' if dates.get('make') else 'NOT_PROVIDED',
        width_mm=None,depth_mm=None,height_mm=None,dimension_status='NOT_PROVIDED' if successful else 'EXTRACTION_FAILED',raw_collected_at=doc['collected_at'])

    # Every distinct price kind and source time survives; no global minimum selection.
    def offer_identity(seller):
        from .offer_identity import bridge_identity
        candidates=[r for r in rules if r.get('type')=='OFFER_IDENTITY' and r.get('verified_stable') is True
                    and r.get('seller_id_field')=='seller_id' and r.get('offer_id_field')=='source_offer_id']
        if not candidates:return bridge_identity(doc,seller)
        if len(candidates)!=1:return None
        rule=candidates[0]
        seller_id,sale_id=seller.get('seller_id'),seller.get('source_offer_id')
        option=seller.get('option_key') or (rule.get('option_key') if rule.get('option_status')=='NO_OPTION_CONFIRMED' else None)
        if not all(isinstance(v,str) and v for v in (seller_id,sale_id,option,rule.get('version'),rule.get('evidence_ref'))):return None
        return dict(offer_key='offer-v1:'+digest(['danawa',doc['source_product_id'],seller_id,sale_id,option]),
                    source_seller_id=seller_id,source_offer_id=sale_id,option_key=option,
                    option_status='IDENTIFIED' if seller.get('option_key') else 'NO_OPTION_CONFIRMED',rule=rule)
    identities={index:offer_identity(seller) for index,seller in enumerate(detail.get('seller_offers') or [])}

    def price(value,text,kind,path,observed,currency=None,scope='UNCLASSIFIED',seller=None,aliases=None,identity=None):
        source_key=evidence(doc,path,value)
        displayed,display_currency=display_money(text)
        currency=currency or display_currency
        amount,numeric=money(value if value is not None else displayed,currency)
        if displayed is not None and amount is not None:
            displayed_amount,_=decimal(displayed)
            if displayed_amount!=amount:numeric='VALUE_CONFLICT'
        for alias in aliases or []:
            alias_amount,_=display_money(alias.get('value'))
            if alias_amount is not None and amount is not None and decimal(alias_amount)[0]!=amount:
                numeric='VALUE_CONFLICT'
        condition=dict(condition_schema_version=1,price_scope=scope,
            **{k:dict(state='NOT_PROVIDED',value=None) for k in ('discount','membership','coupon','delivery','installation','tax')},
            option=dict(state='UNKNOWN',value=dict(identity_status='UNKNOWN',option_key=None,option_evidence_key=None)),
            unresolved_evidence_key=source_key)
        if 'MEMBER' in kind:
            condition['membership']=dict(state='UNKNOWN',value=dict(required=True,program_code=None))
        if seller and seller.get('shipping_text')=='무료배송':
            condition['delivery']=dict(state='CONFIRMED',value=dict(included=True,fee_amount='0',currency='KRW',fee_basis='FREE_DELIVERY_EXPLICIT'))
        condition_hash=digest(condition)
        option_key=identity['option_key'] if identity else None
        if identity and identity['option_status']!='UNKNOWN':
            condition['option']=dict(state='CONFIRMED',value=dict(identity_status=identity['option_status'],
                option_key=option_key,option_evidence_key=source_key))
            condition_hash=digest(condition)
        key='silver-price-v1:'+digest(dict(evaluation_key=eval_key,source_evidence_key=source_key,
            price_kind=kind,condition_fingerprint=condition_hash,option_evidence_key=source_key if identity else None))
        body=dict(price_observation_key=key,source_evidence_key=source_key,price_kind=kind,
            price_basis='COMPARISON_AGGREGATE' if scope=='COMPARISON_AGGREGATE' else ('SELLER_REPORTED' if seller else 'UNCLASSIFIED'),
            condition_status='UNKNOWN',amount=amount,currency=currency,currency_evidence='SOURCE_EXPLICIT' if currency else None,
            price_text=text,offer_key=identity['offer_key'] if identity else None,option_key=option_key,
            option_evidence_key=source_key if identity else None,condition_fingerprint=condition_hash,
            condition_json=canonical(condition),numeric_status=numeric,option_status=identity['option_status'] if identity else 'UNKNOWN')
        add('price_observations',body,observed,dict(amount=numeric,currency='CONFIRMED' if currency else 'SEMANTICS_UNKNOWN'),
            dict(source_path=path,raw_value=value,aliases=aliases or [],seller=seller))
        if numeric!='CONFIRMED':issue('PRICE_'+numeric,'price_observations',key,'amount',value)
        if currency is None:issue('CURRENCY_UNVERIFIED','price_observations',key,'currency',text)
        if not observed:issue('FIELD_TIME_UNKNOWN','price_observations',key,'observed_at')
        if seller and not identity:issue('OFFER_ID_UNVERIFIED','price_observations',key,'offer_key',seller,scope='PURCHASE')
    kinds={'min':'LISTING_LOWEST','total':'TOTAL_REPORTED','member':'MEMBER_REPORTED',
           'memberTotal':'MEMBER_TOTAL_REPORTED','initial':'INITIAL_UNCLASSIFIED','promotionMinPriceQ':'PROMOTION_UNCLASSIFIED'}
    for index,(listing,a) in enumerate(zip(listings,apis)):
        prices=a.get('price') or {}
        for field,kind in kinds.items():
            if field not in prices:continue
            text=prices.get(field+'Formatted'); aliases=[]
            if field=='min' and listing.get('format')=='action_product' and listing.get('price_kind')=='comparison_lowest':
                aliases=[dict(path=f'/payload/listings/{index}/api_product/price/minFormatted',value=text),
                         dict(path=f'/payload/listings/{index}/price_text',value=listing.get('price_text'))]
                text=listing.get('price_text') or text
            price(prices[field],text,kind,f'/payload/listings/{index}/api_product/price/{field}',listing.get('collected_at'),scope='COMPARISON_AGGREGATE',aliases=aliases)
        if 'min' not in prices and listing.get('price_text'):
            price(None,listing['price_text'],'LISTING_LOWEST' if listing.get('price_kind')=='comparison_lowest' else 'LISTING_UNCLASSIFIED',
                  f'/payload/listings/{index}/price_text',listing.get('collected_at'),scope='COMPARISON_AGGREGATE')
    offers=selected.get('offers') or []
    if isinstance(offers,dict):offers=[offers]
    for index,offer in enumerate(offers):
        if not isinstance(offer,dict):continue
        is_aggregate=offer.get('@type')=='AggregateOffer'
        for field in ('lowPrice','highPrice') if is_aggregate else ('price',):
            if field in offer:price(offer[field],str(offer[field]),'AGGREGATE_REPORTED' if is_aggregate else 'OFFER_REPORTED',
                f'/payload/detail/selected_product/offers/{index}/{field}',detail.get('collected_at'),offer.get('priceCurrency'),
                scope='COMPARISON_AGGREGATE' if is_aggregate else 'UNCLASSIFIED',seller=None if is_aggregate else offer)
    for index,seller in enumerate(detail.get('seller_offers') or []):
        price(seller.get('price_attribute'),seller.get('price_text'),'SELLER_REPORTED',
              f'/payload/detail/seller_offers/{index}',detail.get('collected_at'),scope='SELLER_ITEM',seller=seller,identity=identities[index])

    # Preserve measurement targets/ranges; only owner-confirmed axes and options project to product dimensions.
    projected_axes={}
    specifications=detail.get('specifications') or []
    fallback=None
    if not specifications:
        candidates=[(detail.get('spec_text'),'/payload/detail/spec_text',detail.get('collected_at'))]
        candidates += [(l.get('spec_text'),f'/payload/listings/{i}/spec_text',l.get('collected_at')) for i,l in enumerate(listings)]
        candidates += [((a.get('description') or {}).get('pc',{}).get('raw'),f'/payload/listings/{i}/api_product/description/pc/raw',
                        listings[i].get('collected_at')) for i,a in enumerate(apis)]
        fallback=next((c for c in candidates if c[0]),None)
        if fallback:specifications=[dict(name='UNSTRUCTURED_SPEC',value=fallback[0])]
    for index,spec in enumerate(specifications):
        name_,value=spec.get('name',''),spec.get('value')
        path=fallback[1] if fallback else f'/payload/detail/specifications/{index}'
        raw_spec=fallback[0] if fallback else spec
        spec_time=fallback[2] if fallback else detail.get('collected_at')
        source_key=evidence(doc,path,raw_spec)
        attr_name=next((t for label,t in [('색상','COLOR'),('재질','MATERIAL'),('소재','MATERIAL'),('형태','SHAPE')] if label in name_),'DIMENSION' if measurement(value) or dimensions(value) else 'SPECIFICATION')
        axis_rules=[r for r in rules if r.get('type')=='AXIS_RULE' and r.get('source_name')==name_]
        axis_rule=axis_rules[0] if len(axis_rules)==1 else None
        aliases=[r for r in rules if r.get('type')=='ATTRIBUTE_ALIAS' and r.get('attribute_name')==attr_name and r.get('raw_value')==value]
        target=(axis_rule or {}).get('measurement_target')
        if not target:
            target=next((target for label,target in [('좌판','SEAT'),('포장','PACKAGE'),('매트','MATTRESS'),('프레임','FRAME')] if label in name_),None)
        option_status=(axis_rule or {}).get('option_status','UNKNOWN')
        numeric=measurement(value) or dict(value_decimal=None,value_min=None,value_max=None,unit=None,attribute_status='SEMANTICS_UNKNOWN')
        triple=dimensions(value)
        if triple:
            numeric['attribute_status']='AXIS_UNKNOWN'
            if (axis_rule and axis_rule.get('axes')==['WIDTH','DEPTH','HEIGHT'] and target=='PRODUCT'
                    and option_status=='NO_OPTION_CONFIRMED'):
                for axis,amount in zip(('width_mm','depth_mm','height_mm'),triple):
                    projected_axes.setdefault(axis,set()).add(amount)
                numeric['attribute_status']='CONFIRMED'
        codes=sorted({c for rule in aliases for c in rule.get('codes',[])})
        if attr_name=='SHAPE' and len(codes)>1:numeric['attribute_status']='CONFLICT';codes=[]
        key='silver-attr-v1:'+digest(dict(evaluation_key=eval_key,source_evidence_key=source_key,attribute_name=attr_name,
            measurement_target=target,option_evidence_key=None,component=None))
        add('product_attributes',dict(attribute_key=key,source_evidence_key=source_key,attribute_name=attr_name,
            value_type='MEASUREMENT' if attr_name=='DIMENSION' else 'TEXT',raw_value_json=canonical(raw_spec),value_text=str(value) if value is not None else None,
            codes=codes,measurement_target=target,component=None,option_key=None,option_evidence_key=None,
            option_status=option_status,**numeric),spec_time,proof=dict(source_path=path,axis_rule=axis_rule,aliases=aliases))
        if numeric['attribute_status']=='AXIS_UNKNOWN':issue('DIMENSION_AXIS_UNKNOWN','product_attributes',key,'dimension',spec)
        if target is None and attr_name=='DIMENSION':issue('DIMENSION_TARGET_UNKNOWN','product_attributes',key,'measurement_target',spec)
        if attr_name in {'COLOR','MATERIAL','SHAPE'} and not codes:issue('ATTRIBUTE_UNMAPPED','product_attributes',key,attr_name,spec)
    if projected_axes:
        if any(len(v)>1 for v in projected_axes.values()):
            product['dimension_status']='CONFLICT';issue('DIMENSION_CONFLICT',field='dimensions',raw={k:sorted(v) for k,v in projected_axes.items()})
        else:
            product.update({k:next(iter(v)) for k,v in projected_axes.items()},dimension_status='CONFIRMED')
    elif any(r['attribute_name']=='DIMENSION' for r in tables['product_attributes']):product['dimension_status']='SEMANTICS_UNKNOWN'

    vector={r['image_slot']:r for r in image['vector']}
    for item in doc['image_refs']:
        slot=item['image_slot']; current=vector.get(slot,{})
        reviews=[r for r in related['reviews']['rows'] if r.get('source_version')==item['source_version']
                 and r.get('checksum')==item.get('sha256') and 'RECOMMENDATION' in r.get('scopes',[])]
        decisions={(r['effective_status'] if r['effective_status']!='ACTIVE' else r.get('decision')) for r in reviews}
        review=next(iter(decisions)) if len(decisions)==1 else ('CONFLICT' if decisions else 'PENDING')
        if review=='APPROVED' and any(not r.get('evidence_ref') for r in reviews):
            raise PipelineError('SILVER_REVIEW_EVIDENCE_REQUIRED')
        key='silver-image-v1:'+digest(dict(evaluation_key=eval_key,raw_document_id=doc['_id'],image_slot=slot,source_version=item['source_version']))
        availability=current.get('availability','UNCONFIRMED')
        provider_versions={str(a['image']['version']) for a in apis if (a.get('image') or {}).get('url') and (a.get('image') or {}).get('url') in
            {item['source_url'],item.get('observed_url')} and (a.get('image') or {}).get('version') is not None}
        add('product_images',dict(image_relation_key=key,image_slot=slot,source_version=item['source_version'],
            source_url=item['source_url'],observed_url=item.get('observed_url'),role=item['role'],origin=item.get('origin'),
            display_order=item['order'],provider_image_version=next(iter(provider_versions)) if len(provider_versions)==1 else None,
            image_checksum=item.get('sha256'),bucket=item.get('bucket'),object_key=item.get('object_key'),
            asset_ref_json=canonical(dict(type='PIPELINE_OBJECT_REF',bucket=item.get('bucket'),object_key=item.get('object_key'),sha256=item.get('sha256'))),
            **{k:item.get(k) for k in ('bytes','width','height','format','extension')},acquisition_status=item['acquisition_state'],
            availability=availability,validation_status='LOCAL_BYTES_VERIFIED' if item['acquisition_state']=='VERIFIED' else 'UNCONFIRMED',
            image_state_revision=image['revision'],image_state_fingerprint=image['fingerprint'],image_state_checked_at=image['checked_at'],
            review_snapshot_hash=digest(related['reviews']),usage_scope_json=canonical(sorted({s for r in reviews for s in r.get('scopes',[])})),review_status=review),
            detail.get('collected_at'),proof=dict(raw_image_ref=item,remote_state=current,review_rows=reviews,
                raw_payload_hash=doc['payload_hash'],raw_collected_at=doc['collected_at'],freshness='NOT_EVALUATED'))
        if availability!='AVAILABLE':issue('IMAGE_'+availability,'product_images',key,'availability',current,scope='RECOMMENDATION')
        if review!='APPROVED':issue('REVIEW_'+review,'product_images',key,'review_status',reviews,scope='RECOMMENDATION')
    if not doc['image_refs']:issue('IMAGE_MISSING',field='images',scope='RECOMMENDATION')

    def link(url,path,kind,observed,identity=None):
        source_key=evidence(doc,path,url)
        try:
            parsed=urlsplit(str(url))
            structurally_valid=(valid_product_url(str(url),doc['source_product_id']) if kind=='COMPARISON' else
                                parsed.scheme=='https' and bool(parsed.hostname) and not parsed.username and not parsed.password)
        except ValueError:structurally_valid=False
        rows_=[r for r in related['links']['rows'] if r.get('url')==url and r.get('kind')==kind]
        status='INVALID' if not structurally_valid else 'NOT_CHECKED'
        if structurally_valid and rows_:
            states={r.get('status') if r['effective_status']=='ACTIVE' else r['effective_status'] for r in rows_}
            status=next(iter(states)) if len(states)==1 else 'CONFLICT'
            if status=='VALID' and any(r.get('checked_at') and utc(r['checked_at'])>utc(as_of) for r in rows_):status='NOT_YET_VERIFIED'
            if status=='VALID' and any(not r.get('checked_at') or not r.get('expires_at') or not r.get('evidence_ref') for r in rows_):
                raise PipelineError('SILVER_LINK_EVIDENCE_REQUIRED')
        key='silver-link-v1:'+digest(dict(evaluation_key=eval_key,source_evidence_key=source_key,link_kind=kind,
            source_url=url,offer_key=identity['offer_key'] if identity else None,option_evidence_key=None))
        add('link_observations',dict(link_observation_key=key,source_evidence_key=source_key,source_url=url,link_kind=kind,
            structure_status='VALID' if structurally_valid else 'INVALID',link_status=status,comparison_url=url if kind=='COMPARISON' else None,
            offer_key=identity['offer_key'] if identity else None,option_evidence_key=None,checked_at=rows_[0].get('checked_at') if rows_ else None,
            expires_at=rows_[0].get('expires_at') if rows_ else None,validation_snapshot_hash=digest(related['links']),
            validation_evidence_json=canonical(rows_)),observed,proof=dict(source_path=path))
        if status!='VALID':issue('LINK_'+status,'link_observations',key,'url',url,scope='COMPARISON' if kind=='COMPARISON' else 'PURCHASE')
    url=detail.get('url') or next((l.get('product_url') for l in listings if l.get('product_url')),None)
    if url:link(url,'/payload/detail/url','COMPARISON',detail.get('collected_at'))
    for index,offer in enumerate(offers):
        if isinstance(offer,dict) and offer.get('url'):
            try:is_comparison=valid_product_url(offer['url'],doc['source_product_id'])
            except ValueError:is_comparison=False
            link(offer['url'],f'/payload/detail/selected_product/offers/{index}/url',
                 'COMPARISON' if is_comparison else 'DIRECT_PURCHASE',detail.get('collected_at'))
    for index,seller in enumerate(detail.get('seller_offers') or []):
        if seller.get('url'):
            try:is_bridge=urlsplit(seller['url']).hostname=='prod.danawa.com'
            except ValueError:is_bridge=False
            link(seller['url'],f'/payload/detail/seller_offers/{index}/url',
                 'SELLER_BRIDGE' if is_bridge else 'DIRECT_PURCHASE',detail.get('collected_at'),identities[index])
    for identity in {i['offer_key']:i for i in identities.values() if i}.values():
        key='silver-offer-v1:'+digest(dict(evaluation_key=eval_key,offer_key=identity['offer_key']))
        add('normalized_offers',dict(normalized_offer_key=key,offer_key=identity['offer_key'],
            source_seller_id=identity['source_seller_id'],source_offer_id=identity['source_offer_id'],option_key=identity['option_key'],
            identity_rule_version=identity['rule']['version'],identity_evidence_json=canonical(identity),
            price_observation_keys=sorted(r['price_observation_key'] for r in tables['price_observations'] if r['offer_key']==identity['offer_key']),
            link_observation_keys=sorted(r['link_observation_key'] for r in tables['link_observations'] if r['offer_key']==identity['offer_key'])),detail.get('collected_at'))
    field_status=dict(name='CONFIRMED' if name else 'CONFLICT' if distinct else 'NOT_PROVIDED',mapping=mapping_status,
        attributes='CONFIRMED' if specifications else 'NOT_PROVIDED' if successful else 'EXTRACTION_FAILED',
        images='CONFIRMED' if doc['image_refs'] else 'NOT_PROVIDED' if successful else 'EXTRACTION_FAILED',dimensions=product['dimension_status'])
    field_status.update({k:'CONFIRMED' if product[k] else 'NOT_PROVIDED' if successful else 'EXTRACTION_FAILED'
                         for k in ('manufacturer','brand','model')})
    field_status.update(registered_month=reg_status,provider_make=product['provider_make_status'])
    add('product_observations',product,best[0].get('time') if best else None,field_status,
        dict(name_candidates=name_candidates,raw_capture=doc['source_capture'],detail_status=doc['collector_metadata'].get('detail_status'),
             listing_image_candidates=[dict(url=a.get('image',{}).get('url'),version=a.get('image',{}).get('version')) for a in apis]))
    for table in TABLES:
        unique={}
        for row in tables[table]:
            key=row[KEYS[table]]
            if key in unique and unique[key]['record_hash']!=row['record_hash']:raise Conflict('SILVER_OUTPUT_KEY_CONFLICT')
            unique[key]=row
        tables[table]=[unique[k] for k in sorted(unique)]
    reasons=sorted({r['issue_code'] for r in tables['quality_issues']})
    blocked=any(r['blocking_scope'] in {'CATALOG','RECOMMENDATION','COMPARISON','PURCHASE'} for r in tables['quality_issues'])
    return dict(evaluation_key=eval_key,processing_input_fingerprint=fingerprint,tables=tables,
        status='QUALITY_WAIT' if blocked else 'OUTPUT_COMMITTED',quality_reason_codes=reasons,
        image_state=image,related_hashes={k:digest(v) for k,v in related.items()})
