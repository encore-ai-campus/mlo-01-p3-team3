"""Owner-ledger dependency reads and targeted guard invalidation."""
from .contracts import canonical, digest, utc
from .files import parse
from .errors import Conflict,PipelineError
from uuid import uuid4
from .publication.policy import uuid_hex
from .publication.values import sql_time


def dependency_key(kind, subject):
    return digest(dict(kind=kind, subject_key=subject))


def read_set(db, product_key, observations, events, images=()):
    subjects={('PRODUCT',product_key),('POLICY','publication')}
    categories={r.get('service_category_code') for r in observations if r.get('service_category_code')}
    paths={(r['source_code'],canonical(p['codes'])) for r in observations
           for p in parse(r['source_category_path_json'])}
    for code,path in paths:subjects.add(('MAPPING',code+':'+digest(parse(path))))
    related=[]
    for identity in subjects:
        event=events.get(identity)
        if not event:continue
        p=parse(event['body_json'])['item']['payload']
        if p.get('service_category_id'):
            category=db.get('categories',dict(id=p['service_category_id'].replace('-','')),locking=False)
            if category:categories.add(category['code'])
    for image in images:
        subjects.add(('IMAGE',image['id']))
        asset=db.get('assets',dict(id=image['asset_id']),locking=False)
        if asset:related.append(dict(kind='ASSET',subject_key=asset['id'],review_hash=digest(
            dict(status=asset['status'],usage_policy=parse(asset['usage_policy_json'])))))
    subjects.update(('CATEGORY',code) for code in categories)
    for kind,key in sorted(subjects):
        event=events.get((kind,key))
        related.append(dict(kind=kind,subject_key=key,review_hash=event['review_hash'] if event else None))
    return sorted({dependency_key(r['kind'],r['subject_key']):r for r in related}.values(),key=canonical)


def verify_read_set(db, reads):
    if not isinstance(reads,list) or not reads:raise Conflict('REVIEW_PRODUCT_READ_SET_REQUIRED')
    for item in reads:
        kind,key=item['kind'],item['subject_key']
        if kind=='ASSET':
            asset=db.get('assets',dict(id=key))
            actual=digest(dict(status=asset['status'],usage_policy=parse(asset['usage_policy_json']))) if asset else None
        else:
            row=db.one('SELECT review_hash FROM pipeline_review_events WHERE kind=%s AND subject_key=%s ORDER BY revision DESC LIMIT 1 FOR SHARE',(kind,key))
            actual=row['review_hash'] if row else None
        if actual!=item['review_hash']:raise Conflict('REVIEW_GOLD_OWNER_INPUTS_CHANGED')
    return {dependency_key(r['kind'],r['subject_key']):r['review_hash'] for r in reads}


def invalidate(db, prepared, bundle_id):
    keys={dependency_key(item['kind'],item['subject_key']) for item,_,_,_ in prepared}
    keys.update(dependency_key('ASSET',state['asset_id']) for item,_,_,state in prepared if item['kind']=='IMAGE' and state)
    paths=['$.owner_read_set."'+key+'"' for key in sorted(keys)]
    # Legacy guards retain the conservative contract until explicitly replaced.
    db.execute("UPDATE pipeline_publication_guards SET revision=revision+1,guard_json=JSON_SET(guard_json,'$.product_active',false,'$.invalidated_by',%s) WHERE JSON_EXTRACT(guard_json,'$.owner_read_set') IS NULL OR JSON_CONTAINS_PATH(guard_json,'one',"+
        ','.join(['%s']*len(paths))+")",(bundle_id,*paths))


def current(db,item,*,locking=True):
    kind,p=item['kind'],item['payload']
    lock=' FOR UPDATE' if locking else ''
    if kind=='CATEGORY':return db.one('SELECT id,code,name,active,recognition_enabled FROM categories WHERE code=%s'+lock,(p['code'],))
    if kind=='MAPPING':return db.one('SELECT id,source_code,source_category,category_id,mapping_version,active FROM category_mappings WHERE source_code=%s AND source_category=%s'+lock,
        (p['source_code'],canonical(p['source_category_codes'])))
    if kind=='PRODUCT':
        row=db.one('SELECT id,product_key,source_code,source_product_id,name,status,category_id,source_refs_json,gold_snapshot_ref_json FROM products WHERE product_key=%s'+lock,(p['product_key'],))
    elif kind=='IMAGE':
        row=db.one('SELECT i.id,i.product_id,i.source_code,i.source_product_id,i.source_version,i.source_url,i.raw_document_id,i.raw_payload_hash,i.asset_id,i.active,i.usage_review_json,a.checksum,a.status AS asset_status,a.usage_policy_json FROM product_images i JOIN assets a ON a.id=i.asset_id WHERE i.id=%s'+lock,(uuid_hex(p['image_id']),))
    else:return None
    if row:
        for key in ('source_refs_json','gold_snapshot_ref_json','usage_review_json','usage_policy_json'):
            if key in row:row[key]=parse(row[key]) if isinstance(row[key],str) else row[key]
    return row


def _write(db,item,bundle,state):
    kind,p=item['kind'],item['payload'];approved=item['decision']=='APPROVED';stamp=sql_time(bundle['reviewed_at'])
    if kind=='CATEGORY':
        values=(p['name'],p['active'] and approved,p['recognition_enabled'] and approved,uuid_hex(p['category_id']))
        if state:
            if state['id']!=uuid_hex(p['category_id']):raise Conflict('REVIEW_CATEGORY_ID_CHANGED')
            db.execute('UPDATE categories SET name=%s,active=%s,recognition_enabled=%s WHERE id=%s',values)
        elif approved:db.execute('INSERT INTO categories(id,code,name,active,recognition_enabled) VALUES(%s,%s,%s,%s,%s)',
            (uuid_hex(p['category_id']),p['code'],p['name'],p['active'],p['recognition_enabled']))
        if state and (item['decision'] in {'REJECTED','REVOKED'} or not p['active']):
            db.execute("UPDATE products SET status='INACTIVE',catalog_ready=0,recommendation_ready=0,comparison_link_ready=0,purchase_link_ready=0,updated_at=%s WHERE category_id=%s",(stamp,state['id']))
    elif kind=='MAPPING':
        if p['excluded'] and not p.get('service_category_id'):
            if state:db.execute('UPDATE category_mappings SET active=0,updated_at=%s WHERE id=%s',(stamp,state['id']))
            return
        category=db.get('categories',dict(id=uuid_hex(p['service_category_id'])))
        if not category or (approved and p['active'] and not category['active']):raise PipelineError('REVIEW_ACTIVE_CATEGORY_REQUIRED')
        active=approved and p['active'] and not p['excluded']
        if state:db.execute('UPDATE category_mappings SET category_id=%s,mapping_version=%s,active=%s,updated_at=%s WHERE id=%s',
            (category['id'],p['mapping_version'],active,stamp,state['id']))
        elif approved:db.execute('INSERT INTO category_mappings(id,source_code,source_category,category_id,mapping_version,active,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',
            (uuid4().hex,p['source_code'],canonical(p['source_category_codes']),category['id'],p['mapping_version'],active,stamp,stamp))
    elif kind=='PRODUCT':
        if not state:raise PipelineError('REVIEW_PRODUCT_NOT_FOUND')
        actual={r['raw_document_id'] for r in state['source_refs_json']}
        if set(p['raw_document_ids'])!=actual:raise Conflict('REVIEW_PRODUCT_SOURCE_CHANGED')
        category=db.get('categories',dict(id=uuid_hex(p['service_category_id'])))
        if approved and (not category or not category['active']):raise PipelineError('REVIEW_ACTIVE_CATEGORY_REQUIRED')
        if item['decision'] in {'REJECTED','REVOKED'} or (approved and (not p['active'] or not p['supported'])):
            db.execute("UPDATE products SET status='INACTIVE',catalog_ready=0,recommendation_ready=0,comparison_link_ready=0,purchase_link_ready=0,updated_at=%s WHERE id=%s",(stamp,state['id']))
            db.execute('UPDATE product_images SET active=0,updated_at=%s WHERE product_id=%s',(stamp,state['id']))
        # The later confirmed Gold/guard transaction performs activation.
    elif kind=='IMAGE':
        if not state or p['source_version']!=state['source_version'] or p['checksum']!=state['checksum']:
            raise Conflict('REVIEW_IMAGE_SOURCE_CHANGED')
        usage=dict(decision=item['decision'],scopes=p['scopes'],source_version=p['source_version'],checksum=p['checksum'],
            permission_evidence_ref=p.get('permission_evidence_ref'),evidence_ref=bundle['evidence_ref'],reviewer_ref=bundle['actor_ref'],
            checked_at=utc(bundle['reviewed_at']),expires_at=utc(bundle['expires_at']),audit_event_id=item['id'])
        usage['permission_snapshot_hash']=digest(usage)
        db.execute('UPDATE product_images SET usage_review_json=%s,active=%s,updated_at=%s WHERE id=%s',
            (canonical(usage),state['active'] if approved else False,stamp,state['id']))
        if not approved:
            db.execute('UPDATE products SET recommendation_ready=0,representative_image_id=NULL,updated_at=%s WHERE representative_image_id=%s',(stamp,state['id']))
        if approved:
            # This explicit owner payload concerns shared-file restrictions, not other product-link approvals.
            db.execute('UPDATE assets SET status=%s,usage_policy_json=%s,updated_at=%s WHERE id=%s',
                ('AVAILABLE',canonical(p['asset_policy']),stamp,state['asset_id']))


def expire_permissions(db,product_keys,at):
    """Apply existing expiry decisions; never create a new approval or review."""
    from .contracts import utc
    keys=sorted(set(product_keys))
    if not 1<=len(keys)<=100:raise PipelineError('REVIEW_PRODUCT_SCOPE_LIMIT')
    marks=','.join(['%s']*len(keys));stamp=sql_time(at)
    images=db.all('SELECT i.id,i.product_id FROM product_images i JOIN products p ON p.id=i.product_id WHERE p.product_key IN ('+marks+')',tuple(keys))
    events=db.all("SELECT e.kind,e.subject_key,e.body_json FROM pipeline_review_events e WHERE ((e.kind='PRODUCT' AND e.subject_key IN ("+marks+")) OR (e.kind='IMAGE' AND e.subject_key IN (SELECT i.id FROM product_images i JOIN products p ON p.id=i.product_id WHERE p.product_key IN ("+marks+"))) OR (e.kind='CATEGORY' AND e.subject_key IN (SELECT c.code FROM categories c JOIN products p ON p.category_id=c.id WHERE p.product_key IN ("+marks+")))) AND e.revision=(SELECT MAX(x.revision) FROM pipeline_review_events x WHERE x.kind=e.kind AND x.subject_key=e.subject_key)",tuple(keys+keys+keys))
    expired=0
    image_ids={r['id'] for r in images}
    for event in events:
        body=parse(event['body_json'])
        if utc(body['expires_at'])>utc(at):continue
        if event['kind']=='PRODUCT':
            if event['subject_key'] not in keys:continue
            product=db.get('products',dict(product_key=event['subject_key']))
            if product and product['status']=='ACTIVE':
                db.execute("UPDATE products SET status='INACTIVE',catalog_ready=0,recommendation_ready=0,comparison_link_ready=0,purchase_link_ready=0,updated_at=%s WHERE id=%s",(stamp,product['id']));expired+=1
                db.execute('UPDATE product_images SET active=0,updated_at=%s WHERE product_id=%s',(stamp,product['id']))
        elif event['kind']=='IMAGE' and event['subject_key'] in image_ids:
            db.execute('UPDATE product_images SET active=0,updated_at=%s WHERE id=%s',(stamp,event['subject_key']))
            db.execute('UPDATE products SET recommendation_ready=0,representative_image_id=NULL,updated_at=%s WHERE representative_image_id=%s',(stamp,event['subject_key']));expired+=1
        elif event['kind']=='CATEGORY':
            db.execute('UPDATE categories SET active=0,recognition_enabled=0 WHERE code=%s',(event['subject_key'],))
            db.execute("UPDATE products SET status='INACTIVE',catalog_ready=0,recommendation_ready=0,comparison_link_ready=0,purchase_link_ready=0,updated_at=%s WHERE category_id=%s",(stamp,uuid_hex(body['item']['payload']['category_id'])));expired+=1
    return dict(expired=expired)

