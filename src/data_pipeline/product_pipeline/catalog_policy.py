"""Document-rule review with explicit operator internal-project authorization.

The operator subsequently authorized internal image use on 2026-10-19. This
records that operational decision, not a claim of external permission. Models,
embeddings and indexes are not executed or marked complete here.
"""
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid5
from .contracts import canonical, digest, now, utc
from .errors import PipelineError, Conflict
from .files import atomic_json, parse
from . import review

VERSION='brd-internal-project-rules-v2'
ACTOR='pipeline:'+VERSION
CRITERIA='repo://docs/BRD_3차.md#완제품-가구·생활용품;docs/design/02-data-pipeline.md#수집과-원천-보존'
POLICY=dict(price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400,representative_price_kinds=[])
IMAGE_AUTHORIZATION='operator://2026-10-19/internal-project-image-use-approval'
IMAGE_SCOPES=['STORAGE','DISPLAY','EMBEDDING','RECOMMENDATION','ANALYSIS']
FAMILIES={
 'BED':('침대·프레임·매트리스',{'침대','침대프레임','2층/벙커침대','매트리스/토퍼','토퍼매트리스','모션베드'}),
 'SOFA':('소파',{'소파','패브릭소파','가죽소파','좌식소파','유아소파','소파베드'}),
 'CHAIR':('의자',{'의자','학생/사무용의자','게이밍의자','간이/접이식의자','다용도/야외의자','상업용의자','아동/유아의자','안락의자/빈백','좌식/자세교정의자','회의실/학원의자','식탁의자','스툴','소파스툴'}),
 'DESK':('책상',{'책상','모션데스크','컴퓨터/게이밍책상','학원/사무용책상'}),
 'TABLE':('테이블·식탁',{'거실테이블','상/테이블','식탁','화장대/콘솔'}),
 'STORAGE':('수납 가구',{'수납장','책장','선반','서랍장','TV거실장','주방수납장','신발장','옷장','붙박이장','드레스룸','렌지대','행거'}),
 'MIRROR':('전신·벽걸이 거울',{'벽/전신거울'}),
 'RUG':('러그·카페트',{'러그/카페트'}),
 'CURTAIN':('커튼·블라인드',{'블라인드','거실커튼','가리개커튼','암막커튼','창문커튼','주방/바란스커튼'}),
 'BEDDING':('침구',{'침구','침구세트','이불/담요','이불베개세트','이불커버','구스이불','구스침구세트','패드/토퍼','패드/토퍼세트','베개/베개커버','베개커버','경추베개','바디필로우','매트리스커버','매트커버세트'}),
}
PROFILE_HASH=digest(dict(version=VERSION,criteria=CRITERIA,policy=POLICY,
    categories={k:dict(name=v[0],source_names=sorted(v[1])) for k,v in FAMILIES.items()},
    image_authorization=IMAGE_AUTHORIZATION,image_scopes=IMAGE_SCOPES,image_binding='raw-ref-and-majority-state-v1'))


def image_request(link):
    checksum=link.get('checksum') or ''
    if (link.get('asset_status') not in {'AVAILABLE','PENDING_REVIEW'} or link.get('verified_source') is not True
            or len(checksum)!=64 or any(c not in '0123456789abcdef' for c in checksum)
            or not link.get('source_version') or not link.get('raw_document_id')
            or any(not link.get(k) or link[k]<=0 for k in ('size_bytes','width_px','height_px'))):
        return None
    existing=parse(link['usage_review_json'])
    if existing.get('decision') not in {None,'NOT_REVIEWED','PENDING','APPROVED'}:return None
    return dict(kind='IMAGE',payload=dict(image_id=str(UUID(link['id'])),source_version=link['source_version'],
        checksum=checksum,scopes=list(IMAGE_SCOPES),permission_evidence_ref=IMAGE_AUTHORIZATION,
        asset_policy=dict(decision='APPROVED',evidence_ref=IMAGE_AUTHORIZATION,
            allowed_scopes=list(IMAGE_SCOPES),denied_scopes=['GENERATION'])))


def category_id(code):
    return str(uuid5(UUID('b348c680-e3a4-58c6-9b1a-429088032a1d'), 'service-category:'+code))


def classify(normal):
    name=normal.get('name')
    categories=normal.get('categories') or []
    if not isinstance(name,str) or not name.strip() or not categories:return None
    selected=set()
    for row in categories:
        label=row.get('category_name')
        candidates={code for code,(_,labels) in FAMILIES.items() if label in labels}
        if len(candidates)!=1:return None
        selected.update(candidates)
    if len(selected)!=1:return None
    code=next(iter(selected))
    if any(word in name for word in ('부품','교체용','교체 부품','수리용','리필','원단','커튼봉','브라켓','바퀴세트','의자다리','소파다리')):return None
    if code not in {'BEDDING','CURTAIN','RUG'} and any(word in name for word in ('커버','보호필름','쿠션솜')):return None
    return code


def _commit(db, requests, output, phase, evidence, *, stamp):
    requests=list({(r['kind'],review.subject(r['kind'],r['payload'])):r for r in requests}.values())
    pending=[];managed=0
    with db.read_snapshot():
        for request in requests:
            key=review.subject(request['kind'],request['payload'])
            old=db.one('SELECT * FROM pipeline_review_events WHERE kind=%s AND subject_key=%s ORDER BY revision DESC LIMIT 1',
                       (request['kind'],key))
            if old:
                body=parse(old['body_json'])
                if body['actor_ref']!=ACTOR:
                    managed+=1;continue
                if (body['item']['decision']=='APPROVED' and body['item']['payload']==request['payload']
                        and utc(body['reviewed_at'])<=stamp<utc(body['expires_at'])):continue
            pending.append(request)
    created=0
    for offset in range(0,len(pending),500):
        bundle=review.draft(db,pending[offset:offset+500])
        end=datetime.fromisoformat(stamp.replace('Z','+00:00'))+timedelta(days=30)
        bundle.update(status='REVIEWED',actor_ref=ACTOR,criteria_ref=CRITERIA,
            evidence_ref=evidence,reviewed_at=stamp,expires_at=utc(end.isoformat()))
        for item in bundle['items']:item['decision']='APPROVED'
        bundle=review.seal(bundle)
        path=output/(phase+'-'+bundle['content_hash']+'.json')
        atomic_json(path,bundle)
        result=review.apply(db,bundle,at=stamp)
        created+=result['created']
        atomic_json(output/(phase+'-'+bundle['content_hash']+'-receipt.json'),result)
    return dict(created=created,owner_managed=managed)


def product_query(limit, *, after=None, product_keys=None):
    if type(limit) is not int or not 1<=limit<=500:raise PipelineError('CATALOG_REVIEW_BATCH_LIMIT')
    sql="SELECT product_key,source_refs_json FROM products WHERE source_code=%s AND status IN (%s,%s)"
    args=['danawa','PENDING_REVIEW','ACTIVE']
    if product_keys is not None:
        if not isinstance(product_keys,list) or not 1<=len(product_keys)<=100 or len(set(product_keys))!=len(product_keys) or any(not isinstance(k,str) or not k.startswith('danawa:') for k in product_keys):
            raise PipelineError('CATALOG_REVIEW_BATCH_LIMIT')
        sql+=' AND product_key IN ('+','.join(['%s']*len(product_keys))+')';args.extend(sorted(product_keys))
    elif after:sql+=' AND product_key>%s';args.append(after)
    return sql+' ORDER BY product_key LIMIT %s',(*args,limit)


def apply_catalog_policy(db, mongo, output, *, limit=500, product_keys=None):
    from pathlib import Path
    from .files import lock
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    with lock(output/'catalog-review.lock'):
        return _apply_catalog_policy(db,mongo,output,limit=limit,product_keys=product_keys)


def _apply_catalog_policy(db, mongo, output, *, limit=500, product_keys=None):
    """Bounded live-server registration; never edits normalized/raw source data."""
    if not 1<=limit<=500:raise PipelineError('CATALOG_REVIEW_BATCH_LIMIT')
    from bson import ObjectId
    from .catalog_input import CatalogInputs
    from .mongo import wire_document
    from pathlib import Path
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    review.preflight(db)
    stamp=now();reader=CatalogInputs(mongo['danawa_products'])
    cursor_path=output/'cursor.json'
    after=None
    if cursor_path.exists():
        from .files import read_json
        cursor=read_json(cursor_path)
        if cursor['profile_hash']==PROFILE_HASH:after=cursor.get('after')
    sql,args=product_query(limit,after=after,product_keys=product_keys)
    products=db.all(sql,args)
    db.connection.rollback()
    if not products and after and product_keys is None:
        atomic_json(cursor_path,dict(profile_hash=PROFILE_HASH,after=None))
        return dict(status='CATALOG_REVIEW_SCAN_WRAPPED',created=0,processed=0,image_permissions='OPERATOR_INTERNAL_PROJECT_APPROVAL')
    categories={};mappings={};accepted=[];waiting=[];bindings=[];image_requests=[]
    for product in products:
        raw_ids=sorted({r['raw_document_id'] for r in parse(product['source_refs_json'])})
        families=set();paths=set();proofs=[];image_bindings={}
        for raw_id in raw_ids:
            raw=mongo['raw_products'].find_one(dict(_id=ObjectId(raw_id)))
            if raw is None:raise PipelineError('CATALOG_REVIEW_RAW_UNCONFIRMED')
            doc=wire_document(raw);proof=reader.get(doc);family=classify(proof['normalized_document'])
            state=mongo['ingestion_observations'].find_one(dict(observation_key=doc['observation_key'])) or {}
            vector={r['image_slot']:r for r in state.get('image_state_vector',[])}
            for ref in doc.get('image_refs',[]):
                acquired=vector.get(ref['image_slot'],{})
                if (ref.get('acquisition_state')=='VERIFIED' and acquired.get('availability')=='AVAILABLE'
                        and acquired.get('verification')=='sha256-bytes-v1'
                        and acquired.get('checksum')==ref.get('sha256')
                        and acquired.get('bucket')==ref.get('bucket') and acquired.get('object_key')==ref.get('object_key')):
                    image_bindings[(raw_id,ref['source_version'])]=ref
            if family is None:families.add('UNRESOLVED')
            else:families.add(family)
            for listing in doc['payload']['listings']:
                path=(listing.get('api_product') or {}).get('category',{}).get('codes',[])
                if path:paths.add(tuple(str(c) for c in path))
            proofs.append(dict(raw_document_id=raw_id,normalized_hash=proof['normalized_hash']))
        if len(families)!=1 or 'UNRESOLVED' in families or not paths:
            waiting.append(dict(product_key=product['product_key'],reason='MIXED_PARTS_OR_UNMAPPED_CATEGORY'));continue
        family=next(iter(families));identifier=category_id(family)
        category=db.get('categories',dict(code=family),locking=False)
        db.connection.rollback()
        if category:identifier=str(UUID(category['id']))
        categories[family]=dict(kind='CATEGORY',payload=dict(category_id=identifier,code=family,
            name=FAMILIES[family][0],active=True,recognition_enabled=True))
        conflicting=False
        for path in paths:
            prior=mappings.get(path)
            if prior and prior['payload']['service_category_id']!=identifier:
                conflicting=True;break
            existing=db.one('SELECT category_id FROM category_mappings WHERE source_code=%s AND source_category=%s',
                            ('danawa',canonical(list(path))))
            if existing and str(UUID(existing['category_id']))!=identifier:conflicting=True;break
        db.connection.rollback()
        if conflicting:
            waiting.append(dict(product_key=product['product_key'],reason='EXISTING_OR_MIXED_MAPPING'));continue
        for path in paths:mappings[path]=dict(kind='MAPPING',payload=dict(source_code='danawa',source_category_codes=list(path),
            service_category_id=identifier,mapping_version=VERSION,active=True,excluded=False))
        accepted.append(dict(kind='PRODUCT',payload=dict(product_key=product['product_key'],service_category_id=identifier,
            active=True,supported=True,applicable_attributes=[],raw_document_ids=raw_ids)))
        bindings.append(dict(product_key=product['product_key'],sources=proofs))
        images=db.all('SELECT i.id,i.source_version,i.raw_document_id,i.usage_review_json,a.checksum,a.status AS asset_status,'
            'a.size_bytes,a.width_px,a.height_px,a.bucket,a.object_key FROM product_images i JOIN assets a ON a.id=i.asset_id '
            'JOIN products p ON p.id=i.product_id WHERE p.product_key=%s ORDER BY i.id',(product['product_key'],))
        db.connection.rollback()
        for image in images:
            ref=image_bindings.get((image['raw_document_id'],image['source_version']),{})
            image['verified_source']=bool(ref) and all(image[k]==ref.get(v) for k,v in
                [('checksum','sha256'),('size_bytes','bytes'),('width_px','width'),('height_px','height'),('bucket','bucket'),('object_key','object_key')])
            request=image_request(image)
            if request and image['raw_document_id'] in raw_ids:image_requests.append(request)
    evidence=dict(contract_version=1,profile_hash=PROFILE_HASH,criteria_ref=CRITERIA,
        operator_request='operator://2026-10-19/pipeline-managed-catalog-approval',as_of=stamp,
        image_permissions='OPERATOR_INTERNAL_PROJECT_APPROVAL',image_authorization=IMAGE_AUTHORIZATION,
        bindings=bindings,waiting=waiting)
    evidence_hash=digest(evidence);atomic_json(output/('evidence-'+evidence_hash+'.json'),evidence)
    evidence_ref='server-review://'+db.target+'/'+evidence_hash
    results={}
    for kind,requests in [('categories',categories.values()),('mappings',mappings.values()),
                          ('policy',[dict(kind='POLICY',payload=POLICY)]),('products',accepted),('images',image_requests)]:
        results[kind]=_commit(db,requests,output,kind,evidence_ref,stamp=stamp)
    if product_keys is None:atomic_json(cursor_path,dict(profile_hash=PROFILE_HASH,after=products[-1]['product_key'] if products else None))
    result=dict(status='CATALOG_DECISIONS_REGISTERED',processed=len(products),eligible=len(accepted),waiting=len(waiting),
        created=sum(r['created'] for r in results.values()),phases=results,profile_hash=PROFILE_HASH,
        evidence_ref=evidence_ref,image_permissions='OPERATOR_INTERNAL_PROJECT_APPROVAL')
    atomic_json(output/('result-'+evidence_hash+'.json'),result)
    return result
