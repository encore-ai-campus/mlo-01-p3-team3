"""Explicit owner decisions, immutable audit and conservative pipeline exports.

The ingestion publisher cannot use these write operations. No implicit approval,
network rights inference, DDL, model calls or index activation occurs here.
"""
from uuid import uuid4

from .contracts import canonical,digest,now,utc
from .errors import Conflict,PipelineError
from .files import parse
from .publication.policy import uuid_hex

from .review_policy import KINDS,SCOPES,seal,subject,validate


def preflight(db):
    db.preflight()
    actor=db.one('SELECT CURRENT_USER() AS actor')['actor']
    if 'pipeline_publisher' in actor.lower():raise PipelineError('REVIEW_SEPARATE_OWNER_ACCOUNT_REQUIRED')
    columns={r['name'] for r in db.all('SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',
        (db.settings['database'],'pipeline_review_events'))}
    required={'id','kind','subject_key','revision','review_hash','body_json','actor_ref','actor_db_user','recorded_at'}
    if columns!=required:raise PipelineError('REVIEW_EXPLICIT_MIGRATION_REQUIRED')
    return actor


def enforce_expiry(db,product_keys,*,as_of):
    actor=db.one('SELECT CURRENT_USER() AS actor')['actor']
    if 'pipeline_publisher' in actor.lower():raise PipelineError('REVIEW_SEPARATE_OWNER_ACCOUNT_REQUIRED')
    from .review_repository import expire_permissions
    with db.review_lock(),db.transaction():return expire_permissions(db,product_keys,as_of)


def latest(db,kind,key):
    return db.one('SELECT * FROM pipeline_review_events WHERE kind=%s AND subject_key=%s ORDER BY revision DESC LIMIT 1 FOR SHARE',(kind,key))


from .review_repository import current,_write


def apply(db,bundle,*,at=None):
    validate(bundle,at=at)
    if bundle['target']!=db.target:raise Conflict('REVIEW_TARGET_CHANGED')
    actor=preflight(db);created=reused=0
    with db.review_lock(),db.transaction():
        prepared=[]
        for item in bundle['items']:
            body=dict(item=item,bundle_id=bundle['bundle_id'],actor_ref=bundle['actor_ref'],evidence_ref=bundle['evidence_ref'],
                criteria_ref=bundle['criteria_ref'],reviewed_at=utc(bundle['reviewed_at']),expires_at=utc(bundle['expires_at']))
            checksum=digest(body)
            saved=db.one('SELECT review_hash,body_json FROM pipeline_review_events WHERE id=%s FOR SHARE',(uuid_hex(item['id']),))
            if saved:
                if saved['review_hash']!=checksum or parse(saved['body_json'])!=body:raise Conflict('REVIEW_ID_REUSED_WITH_DIFFERENT_BODY')
                reused+=1;continue
            previous=latest(db,item['kind'],item['subject_key'])
            if ((previous['review_hash'] if previous else None)!=item.get('expected_previous_hash') or
                    item['revision']!=(previous['revision']+1 if previous else 1)):raise Conflict('REVIEW_REVISION_CHANGED')
            state=current(db,item)
            if (digest(state) if state else None)!=item.get('expected_current_hash'):raise Conflict('REVIEW_SUBJECT_CHANGED')
            prepared.append((item,body,checksum,state))
        # Validate the entire before-state before updating a shared asset through
        # multiple image links in the same atomic review bundle.
        policies={}
        for item,body,checksum,state in prepared:
            if item['kind']=='IMAGE' and item['decision']=='APPROVED':
                asset=state['asset_id'];policy=item['payload']['asset_policy']
                if asset in policies and policies[asset]!=policy:raise Conflict('REVIEW_SHARED_ASSET_POLICY_CONFLICT')
                policies[asset]=policy
        for item,body,checksum,state in prepared:
            _write(db,item,bundle,state)
            db.execute('INSERT INTO pipeline_review_events(id,kind,subject_key,revision,review_hash,body_json,actor_ref,actor_db_user,recorded_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))',
                (uuid_hex(item['id']),item['kind'],item['subject_key'],item['revision'],checksum,canonical(body),bundle['actor_ref'],actor))
            created+=1
        if created:
            from .review_repository import invalidate
            invalidate(db,prepared,bundle['bundle_id'])
    return dict(status='APPLIED',created=created,reused=reused,bundle_hash=bundle['content_hash'])


def draft(db,requests):
    """Requests are identities/proposals only. Every returned decision is PENDING."""
    if not 1<=len(requests)<=1000:raise PipelineError('REVIEW_BUNDLE_LIMIT_OR_TARGET')
    items=[]
    with db.read_snapshot():
        for request in requests:
            kind,p=request['kind'],request['payload'];key=subject(kind,p)
            previous=db.one('SELECT review_hash,revision FROM pipeline_review_events WHERE kind=%s AND subject_key=%s ORDER BY revision DESC LIMIT 1',(kind,key))
            item=dict(id=str(uuid4()),kind=kind,subject_key=key,payload=p)
            state=current(db,item,locking=False)
            items.append(dict(item,revision=previous['revision']+1 if previous else 1,
                expected_previous_hash=previous['review_hash'] if previous else None,expected_current_hash=digest(state) if state else None,decision='PENDING'))
    return seal(dict(contract_version=1,status='DRAFT',bundle_id=str(uuid4()),target=db.target,
        actor_ref=None,evidence_ref=None,criteria_ref=None,reviewed_at=None,expires_at=None,items=items))


def export_inputs(db,silver_groups,*,as_of,evaluation_selection=None,product_keys=None):
    """Derive mapping-version bindings from confirmed Silver, never guessed hashes."""
    from .silver.inputs import KINDS as SILVER_KINDS,input_snapshot
    from .gold.build import validate_group
    as_of=utc(as_of);bindings={};candidates=[]
    for group in silver_groups:
        validate_group(group)
        candidates.append(dict(receipt=group['receipt'],images=group['tables']['product_images']))
        for row in group['tables']['product_observations']:
            bindings.setdefault(row['product_key'],[]).append(row)
    scope=set(product_keys) if product_keys is not None else set(bindings) if bindings else None
    if scope is not None and (not scope or len(scope)>100):raise PipelineError('REVIEW_PRODUCT_SCOPE_LIMIT')
    if scope:
        keys=sorted(scope);marks=','.join(['%s']*len(keys))
        rows=db.all("SELECT e.* FROM pipeline_review_events e WHERE (e.kind IN ('CATEGORY','MAPPING','POLICY') OR (e.kind='PRODUCT' AND e.subject_key IN ("+marks+")) OR (e.kind='IMAGE' AND e.subject_key IN (SELECT i.id FROM product_images i JOIN products p ON p.id=i.product_id WHERE p.product_key IN ("+marks+")))) AND e.revision=(SELECT MAX(x.revision) FROM pipeline_review_events x WHERE x.kind=e.kind AND x.subject_key=e.subject_key)",tuple(keys+keys))
    else:
        rows=db.all('SELECT e.* FROM pipeline_review_events e JOIN (SELECT kind,subject_key,MAX(revision) AS revision FROM pipeline_review_events GROUP BY kind,subject_key) r ON r.kind=e.kind AND r.subject_key=e.subject_key AND r.revision=e.revision')
    decisions=[parse(r['body_json']) for r in rows if r['kind'] in KINDS];references=digest(sorted(r['review_hash'] for r in rows if r['kind'] in KINDS))
    category_decisions={b['item']['payload']['code']:b for b in decisions if b['item']['kind']=='CATEGORY'}
    mappings=[];reviews=[];states=[];policy=None;blocked=[];review_images={}
    for body in decisions:
        item=body['item'];p=item['payload'];approved=item['decision']=='APPROVED'
        effective=utc(body['reviewed_at'])<=as_of<utc(body['expires_at'])
        evidence='mysql-review://'+db.target+'/'+item['id']
        if item['kind']=='MAPPING':
            category=db.get('categories',dict(id=uuid_hex(p['service_category_id'])),locking=False) if p.get('service_category_id') else None
            category_review=category_decisions.get(category['code']) if category else None
            category_live=not category_review or (category_review['item']['decision']=='APPROVED' and
                utc(category_review['reviewed_at'])<=as_of<utc(category_review['expires_at']))
            mappings.append(dict(source_code=p['source_code'],source_category_codes=p['source_category_codes'],
                service_category_id=p['service_category_id'],service_category_code=category['code'] if category else None,
                active=bool(approved and p['active'] and (p['excluded'] or category and category['active'] and category_live)),excluded=p['excluded'],
                valid_from=body['reviewed_at'],expires_at=body['expires_at'],evidence_ref=evidence))
        elif item['kind']=='IMAGE':
            image=db.get('product_images',dict(id=uuid_hex(p['image_id'])),locking=False)
            if not image:raise Conflict('REVIEW_IMAGE_SOURCE_CHANGED')
            image_product=image['source_code']+':'+image['source_product_id']
            if scope is not None and image_product not in scope:continue
            review_images.setdefault(image_product,[]).append(image)
            usage=parse(image['usage_review_json'])
            if usage.get('audit_event_id')!=item['id']:raise Conflict('REVIEW_CURRENT_IMAGE_DECISION_CHANGED')
            reviews.append(dict(source_code=image['source_code'],source_product_id=image['source_product_id'],
                source_version=p['source_version'],checksum=p['checksum'],scopes=p['scopes'],decision=item['decision'],
                valid_from=body['reviewed_at'],checked_at=body['reviewed_at'],expires_at=body['expires_at'],
                evidence_ref=p.get('permission_evidence_ref') or evidence,audit_event_id=item['id'],
                permission_snapshot_hash=usage['permission_snapshot_hash']))
        elif item['kind']=='PRODUCT':
            source=bindings.get(p['product_key'],[])
            if not source or not {r['raw_document_id'] for r in source}<=set(p['raw_document_ids']):
                blocked.append(dict(product_key=p['product_key'],reason='UNREVIEWED_OR_MISSING_SILVER_SOURCE'));continue
            applicable=[r for r in source if r.get('mapping_status')=='MAPPED' and r.get('service_category_id')==p['service_category_id']]
            if not applicable:
                blocked.append(dict(product_key=p['product_key'],reason='MAPPING_REEVALUATION_REQUIRED'));continue
            for version in sorted({r['mapping_version'] for r in applicable}):
                states.append(dict(product_key=p['product_key'],active=bool(approved and p['active']),supported=bool(approved and p['supported']),
                    service_category_id=p['service_category_id'],mapping_version=version,applicable_attributes=p['applicable_attributes'],
                    checked_at=body['reviewed_at'],expires_at=body['expires_at'],evidence_ref=evidence,audit_event_id=item['id']))
        elif item['kind']=='POLICY' and approved and effective:
            policy=dict(version='owner-review-'+item['id'],evidence_ref=evidence,price_history_mode=p['price_history_mode'],
                price_max_age_seconds=p['price_max_age_seconds'],representative_price_kinds=p.get('representative_price_kinds',[]),bootstrap_single_gallery=True)
    silver={k:input_snapshot(k) for k in SILVER_KINDS}
    for kind,values in [('mappings',mappings),('reviews',reviews)]:
        # PRODUCT/POLICY decisions change Gold, not Silver normalization.
        # Bind each Silver snapshot to its actual rows; unrelated ledger events
        # must not invalidate all existing Silver evaluations.
        values=sorted(values,key=canonical)
        silver[kind]=input_snapshot(kind,values,'review-ledger-v3','mysql-review://'+db.target+'/'+digest(values))
    gold={k:dict(status='NOT_PROVIDED',rows=[],evidence_ref=None) for k in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations')}
    if states:gold['product_states']=dict(status='PROVIDED',rows=states,evidence_ref='mysql-review://'+db.target+'/'+references)
    selected={};explicit={}
    if evaluation_selection is not None:
        # A <=100-product window can include more than 100 historical raw IDs.
        if not isinstance(evaluation_selection,list) or len(evaluation_selection)>20000:raise PipelineError('REVIEW_SELECTION_LIMIT')
        for choice in evaluation_selection:
            raw=choice.get('raw_document_id')
            if not raw or raw in explicit or not choice.get('evaluation_key') or not choice.get('evidence_ref'):
                raise Conflict('REVIEW_EXPLICIT_SELECTION_INVALID')
            matches=[c for c in candidates if c['receipt']['raw_document_id']==raw and c['receipt']['product_key']==choice.get('product_key')
                and c['receipt']['evaluation_key']==choice['evaluation_key']]
            if len(matches)!=1:raise Conflict('REVIEW_EXPLICIT_SELECTION_NOT_CONFIRMED')
            explicit[raw]=matches[0]
    for candidate in candidates:
        receipt=candidate['receipt'];refs=parse(receipt['input_snapshot_refs_json'])
        if receipt['raw_document_id'] in explicit:continue
        if not all(refs.get(k)==v['content_hash'] for k,v in silver.items()):continue
        selected.setdefault(receipt['raw_document_id'],[]).append(candidate)
    for raw,candidate in explicit.items():selected[raw]=[candidate]
    selections=[];galleries={}
    for raw_id,values in selected.items():
        if len(values)!=1:
            blocked.append(dict(raw_document_id=raw_id,reason='EXPLICIT_EVALUATION_SELECTION_REQUIRED'));continue
        candidate=values[0];r=candidate['receipt'];evidence='silver-receipt://'+r['evaluation_key']
        selections.append(dict(product_key=r['product_key'],raw_document_id=raw_id,evaluation_key=r['evaluation_key'],evidence_ref=evidence))
        if candidate['images'] and all(i['availability']=='AVAILABLE' for i in candidate['images']):
            galleries.setdefault(r['product_key'],[]).append(dict(product_key=r['product_key'],evaluation_key=r['evaluation_key'],evidence_ref=evidence))
    if selections:gold['evaluation_selection']=dict(status='PROVIDED',rows=selections,evidence_ref='mysql-review://'+db.target+'/'+references)
    unique=[v[0] for v in galleries.values() if len(v)==1]
    if unique:gold['gallery_selection']=dict(status='PROVIDED',rows=unique,evidence_ref='silver://unique-confirmed-gallery')
    gold.update(contract_version=1,policy=policy or dict(version='owner-policy-not-provided-v1',evidence_ref='pipeline://owner-policy-not-provided',
        price_history_mode='UNRESOLVED',price_max_age_seconds=86400,representative_price_kinds=[],bootstrap_single_gallery=True))
    gold['policy']['review_set_hash']=references
    from .gold.identity import MODE
    gold['policy']['evaluation_identity']=MODE
    from .review_repository import read_set
    event_index={(r['kind'],r.get('subject_key',parse(r['body_json'])['item'].get('subject_key'))):r for r in rows if r['kind'] in KINDS}
    gold['review_read_sets']={key:read_set(db,key,values,event_index,review_images.get(key,[])) for key,values in bindings.items()}
    gold=seal(gold)
    from .silver.inputs import validate_inputs
    from .gold.contracts import validate_inputs as validate_gold
    validate_inputs(silver);validate_gold(gold)
    return dict(silver=silver,gold=gold,blocked=blocked,review_set_hash=references,as_of=as_of)


def _approved(db,kind,key,at):
    event=latest(db,kind,key)
    if not event:raise PipelineError('REVIEW_APPROVAL_NOT_PROVIDED')
    body=parse(event['body_json'])
    if body['item']['decision']!='APPROVED' or not utc(body['reviewed_at'])<=utc(at)<utc(body['expires_at']):
        raise PipelineError('REVIEW_APPROVAL_MISSING_OR_EXPIRED')
    return body


def approve_guard(db,group,versions,*,price_history_mode,as_of,owner_inputs):
    """Owner-only guard + audit; source and review inputs must already agree."""
    from .publication.plan import validate_group
    validate_group(group,versions);actor=preflight(db);catalog=group['tables']['product_catalog'][0]
    key=catalog['product_key']
    with db.review_lock(),db.transaction():
        from .gold.contracts import validate_inputs
        validate_inputs(owner_inputs)
        reads=owner_inputs.get('review_read_sets',{}).get(key)
        owner_read_set=None
        if reads is not None:
            from .review_repository import verify_read_set
            owner_read_set=verify_read_set(db,reads)
        else:
            events=db.all('SELECT review_hash FROM pipeline_review_events e WHERE kind IN (\'CATEGORY\',\'MAPPING\',\'PRODUCT\',\'IMAGE\',\'POLICY\') AND revision=(SELECT MAX(x.revision) FROM pipeline_review_events x WHERE x.kind=e.kind AND x.subject_key=e.subject_key) FOR SHARE')
            if owner_inputs['policy'].get('review_set_hash')!=digest(sorted(r['review_hash'] for r in events)):
                raise Conflict('REVIEW_GOLD_OWNER_INPUTS_CHANGED')
        product=db.get('products',dict(product_key=key))
        if not product or product['status'] not in {'PENDING_REVIEW','ACTIVE','INACTIVE'}:raise PipelineError('REVIEW_PRODUCT_STATE_REQUIRED')
        decision=_approved(db,'PRODUCT',key,as_of);policy=_approved(db,'POLICY','publication',as_of)
        p=decision['item']['payload']
        if not any(r.get('product_key')==key and r.get('audit_event_id')==decision['item']['id'] for r in owner_inputs['product_states']['rows']):
            raise Conflict('REVIEW_GOLD_PRODUCT_DECISION_CHANGED')
        category=db.get('categories',dict(id=uuid_hex(p['service_category_id'])))
        if (not p['active'] or not p['supported'] or not category or not category['active'] or
                category['id']!=uuid_hex(catalog['service_category_id']) or not catalog['catalog_ready'] or
                policy['item']['payload']['price_history_mode']!=price_history_mode):
            raise PipelineError('REVIEW_GOLD_REEVALUATION_REQUIRED')
        raw_ids={r['raw_document_id'] for r in parse(catalog['source_refs_json'])}
        if not raw_ids<=set(p['raw_document_ids']):raise Conflict('REVIEW_GOLD_SOURCE_NOT_REVIEWED')
        reviews={};active=[];expiry=[decision['expires_at'],policy['expires_at']]
        for image in group['tables']['product_images']:
            if not image['recommendation_approved']:continue
            link=db.get('product_images',dict(product_id=product['id'],source_code=catalog['source_code'],
                source_product_id=catalog['source_product_id'],image_variant=image['image_slot'],
                raw_document_id=image['raw_document_id'],source_version=image['source_version']))
            if not link:raise PipelineError('REVIEW_IMAGE_LINK_NOT_PRESERVED')
            review=_approved(db,'IMAGE',link['id'],as_of);usage=parse(link['usage_review_json'])
            payload=review['item']['payload'];evidence=parse(image['evidence_json'])
            proof=parse(evidence['evidence_json'])
            source_reviews=proof.get('review_rows',[])
            asset=db.get('assets',dict(id=link['asset_id']));file_policy=parse(asset['usage_policy_json'])
            if (usage.get('audit_event_id')!=review['item']['id'] or usage.get('source_version')!=image['source_version'] or
                    usage.get('checksum')!=image['image_checksum'] or payload['checksum']!=asset['checksum'] or asset['status']!='AVAILABLE' or
                    not any(r.get('audit_event_id')==review['item']['id'] and r.get('permission_snapshot_hash')==usage.get('permission_snapshot_hash') for r in source_reviews)):
                raise Conflict('REVIEW_GOLD_IMAGE_REVIEW_CHANGED')
            if (file_policy.get('decision')!='APPROVED' or not file_policy.get('evidence_ref') or
                    not set(usage['scopes'])<=set(file_policy.get('allowed_scopes',[])) or
                    set(file_policy.get('denied_scopes',[]))&set(usage['scopes'])):
                raise PipelineError('REVIEW_SHARED_FILE_RESTRICTION')
            reviews[image['image_relation_key']]=dict(snapshot_hash=image['review_snapshot_hash'],usage_review=usage,current_review_hash=digest(usage),asset_policy_hash=digest(file_policy))
            if 'DISPLAY' in usage['scopes']:active.append(image['image_relation_key'])
            expiry.append(review['expires_at'])
        guard=dict(contract_version=1,evidence_ref='mysql-review://'+db.target+'/'+decision['item']['id'],
            gold_evaluation_key=catalog['gold_evaluation_key'],input_fingerprint=catalog['input_fingerprint'],
            category_id=p['service_category_id'],product_active=True,expires_at=min(map(utc,expiry)),price_history_mode=price_history_mode,image_reviews=reviews,
            activation=dict(from_status=product['status'],product_id=product['id'],current_snapshot_hash=digest(parse(product['gold_snapshot_ref_json'])),
                evidence_ref='mysql-review://'+db.target+'/'+decision['item']['id'],image_relations=sorted(active)))
        if owner_read_set is not None:guard['owner_read_set']=owner_read_set
        old=db.get('pipeline_publication_guards',dict(product_key=key))
        if old and parse(old['guard_json'])==guard:return dict(guard=guard,guard_hash=digest(guard),status='REUSED')
        revision=old['revision']+1 if old else 1
        if old:db.execute('UPDATE pipeline_publication_guards SET revision=%s,guard_json=%s WHERE product_key=%s',(revision,canonical(guard),key))
        else:db.execute('INSERT INTO pipeline_publication_guards(id,product_key,revision,guard_json) VALUES(%s,%s,%s,%s)',(uuid4().hex,key,revision,canonical(guard)))
        previous=latest(db,'GUARD',key);review_revision=previous['revision']+1 if previous else 1
        body=dict(guard=guard,guard_hash=digest(guard),guard_revision=revision,actor_ref=decision['actor_ref'])
        db.execute('INSERT INTO pipeline_review_events(id,kind,subject_key,revision,review_hash,body_json,actor_ref,actor_db_user,recorded_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))',
            (uuid4().hex,'GUARD',key,review_revision,digest(body),canonical(body),decision['actor_ref'],actor))
    return dict(guard=guard,guard_hash=digest(guard),status='APPROVED')
