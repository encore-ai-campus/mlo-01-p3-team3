"""Adapters for the existing Mongo ledger, review owner and production stages.

No crawler, embedding generation, runtime DDL or independent Spark writer.
"""
from collections import defaultdict
from pathlib import Path
from ..contracts import digest,canonical,now,utc
from ..files import atomic_json,read_json,parse,sha_file
from ..errors import Conflict,PipelineError
from .policy import snapshot

PROJECTION={'observation_key':1,'raw_confirmed':1,'image_state_revision':1,'image_state_fingerprint':1,'incremental_change':1,
    'document._id':1,'document.source_code':1,'document.source_product_id':1,'document.document_hash':1,
    'document.payload.listings.api_product.category.codes':1}


def publication_hash(product):
    return digest({k:product.get(k) for k in ('product_key','status','gold_snapshot_ref_json',
        'catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready')}) if product else None


class ProductionChanges:
    def __init__(self,collection,db,state_root,work=None,options=None):
        self.collection,self.db,self.root=collection,db,Path(state_root)
        self.work,self.options=work,options
        self.events=[];self.event_hashes={}

    def _reviews(self):
        with self.db.read_snapshot():
            self.events=self.db.all("SELECT e.kind,e.subject_key,e.review_hash,e.body_json FROM pipeline_review_events e JOIN (SELECT kind,subject_key,MAX(revision) AS revision FROM pipeline_review_events WHERE kind IN ('CATEGORY','MAPPING','PRODUCT','IMAGE','POLICY') GROUP BY kind,subject_key) r ON r.kind=e.kind AND r.subject_key=e.subject_key AND r.revision=e.revision")
            categories=self.db.all('SELECT id,code,active,recognition_enabled FROM categories')
        self.categories={r['id']:r for r in categories}
        self.event_hashes={r['kind']+':'+r['subject_key']:r['review_hash'] for r in self.events}
        self.event_hashes.update({'CATEGORY_STATE:'+r['code']:digest(r) for r in categories})
        self.event_map={r['kind']+':'+r['subject_key']:{**r,'body':parse(r['body_json'])} for r in self.events}
        self.category_events={e['body']['item']['payload']['category_id'].replace('-',''):identity
            for identity,e in self.event_map.items() if e['kind']=='CATEGORY'}
        path=self.root/'review-discovery.json'
        previous=read_json(path) if path.exists() else {}
        return {k for k in set(previous)|set(self.event_hashes) if previous.get(k)!=self.event_hashes.get(k)}

    def _query(self,keys):
        if keys is None:return {'raw_confirmed':True}
        return {'raw_confirmed':True,'$or':[{'document.source_code':k.split(':',1)[0],
            'document.source_product_id':k.split(':',1)[1]} for k in keys]}

    def read_products(self,keys=None,*,at=None):
        from ..silver.normalize import VERSION as SILVER_VERSION
        from ..gold.identity import VERSION as GOLD_VERSION
        at=at or now();groups=defaultdict(list);paths=defaultdict(set);pending=defaultdict(set)
        for row in self.collection.find(self._query(keys),PROJECTION):
            doc=row['document'];key=doc['source_code']+':'+doc['source_product_id']
            groups[key].append(dict(observation_key=row['observation_key'],raw_document_id=str(doc['_id']),
                document_hash=doc['document_hash'],image_state_revision=row['image_state_revision'],
                image_state_fingerprint=row['image_state_fingerprint']))
            if 'incremental_change' in row:pending[key].add(row['observation_key'])
            for listing in doc.get('payload',{}).get('listings',[]):
                codes=((listing.get('api_product') or {}).get('category') or {}).get('codes',[])
                if codes:paths[key].add(canonical([str(c) for c in codes]))
        if keys is not None and set(groups)!=set(keys):raise Conflict('INCREMENTAL_SOURCE_PRODUCT_MISSING')
        # One compact SQL read per bounded page, not one query per image/event.
        product_keys=sorted(groups)
        for offset in range(0,len(product_keys),100):
            selected=product_keys[offset:offset+100];marks=','.join(['%s']*len(selected))
            with self.db.read_snapshot():
                products={p['product_key']:p for p in self.db.all('SELECT product_key,status,gold_snapshot_ref_json,catalog_ready,recommendation_ready,comparison_link_ready,purchase_link_ready FROM products WHERE product_key IN ('+marks+')',tuple(selected))}
                images=self.db.all('SELECT i.id,i.source_code,i.source_product_id,i.asset_id,a.status,a.usage_policy_json FROM product_images i JOIN products p ON p.id=i.product_id JOIN assets a ON a.id=i.asset_id WHERE p.product_key IN ('+marks+')',tuple(selected))
            product_images=defaultdict(list)
            for image in images:product_images[image['source_code']+':'+image['source_product_id']].append(image)
            for key in selected:
                deps={};deadlines=[]
                def add(name,value,stage):deps[name]=dict(hash=digest(value),stage=stage)
                add('NORMALIZER',SILVER_VERSION,'silver');add('GOLD_RULES',GOLD_VERSION,'gold')
                subjects={'PRODUCT:'+key,'POLICY:publication'}
                subjects.update('MAPPING:'+key.split(':',1)[0]+':'+digest(parse(path)) for path in paths[key])
                category_ids=set()
                for identity in subjects:
                    event=self.event_map.get(identity)
                    p=event['body']['item']['payload'] if event else {}
                    if p.get('service_category_id'):category_ids.add(p['service_category_id'].replace('-',''))
                subjects.update('IMAGE:'+r['id'] for r in product_images[key])
                subjects.update(self.category_events[c] for c in category_ids if c in self.category_events)
                for category_id in category_ids:
                    category=self.categories.get(category_id)
                    if category:add('CATEGORY_STATE:'+category['code'],category,'silver')
                for identity in sorted(subjects):
                    event=self.event_map.get(identity);effective=None
                    if event:
                        body=event['body'];start,end=utc(body['reviewed_at']),utc(body['expires_at'])
                        effective=start<=utc(at)<end
                        deadlines.extend(t for t in (start,end) if t>utc(at))
                    add(identity,dict(review_hash=event['review_hash'],effective=effective) if event else None,
                        'gold' if identity.startswith(('PRODUCT:','POLICY:')) else 'silver')
                for image in product_images[key]:
                    add('ASSET:'+image['asset_id'],dict(status=image['status'],policy=parse(image['usage_policy_json'])),'gold')
                value=snapshot(key,groups[key],deps,next_due_at=min(deadlines,default=None),publication_hash=publication_hash(products.get(key)))
                value['pending_observation_keys']=sorted(pending[key]);yield value

    def snapshots(self,*,full,due_keys,state,at):
        changed=self._reviews()
        values=self.read_products(at=at) if full else self._changed_products(changed,due_keys,state,at)
        if self.work is None:yield from values;return
        from ..spark_session import release_spark
        spark=frame=inventory=None;pending=[];count=0
        def ready(batch):
            nonlocal spark,frame,inventory
            from pyspark.sql import functions as F
            if spark is None:
                version=self.work.pinned(self.options,self.work.LAKE+'/bronze',['products'])['products']
                spark=self.work.session(self.options)
                frame=spark.read.format('delta').option('versionAsOf',version).load(self.work.LAKE+'/bronze/products').select('observation_key')
                if full:inventory={r['observation_key'] for r in frame.toLocalIterator()}
            keys={r['observation_key'] for v in batch for r in v['observations']}
            available=inventory if full else {r['observation_key'] for r in frame.filter(F.col('observation_key').isin(sorted(keys))).toLocalIterator()}
            for value in batch:
                deferred=self.root/'deferred'/(digest(value['product_key'])+'.json')
                if not {r['observation_key'] for r in value['observations']}<=available:
                    atomic_json(deferred,dict(product_key=value['product_key']));continue
                deferred.unlink(missing_ok=True);yield value
        try:
            for value in values:
                size=len(value['observations'])
                if pending and count+size>100:yield from ready(pending);pending=[];count=0
                pending.append(value);count+=size
            if pending:yield from ready(pending)
        finally:
            if spark is not None:release_spark(spark)

    def _changed_products(self,changed,due_keys,state,at):
        keys=set(due_keys)|set(state.affected(changed))
        for row in self.collection.find({'raw_confirmed':True,'incremental_change':{'$exists':True}},
                {'document.source_code':1,'document.source_product_id':1}).hint('pipeline_incremental_pending_v1'):
            keys.add(row['document']['source_code']+':'+row['document']['source_product_id'])
        # Explicit product/image decisions also work before a derived index exists.
        for event in self.events:
            identity=event['kind']+':'+event['subject_key']
            if identity not in changed:continue
            if event['kind']=='PRODUCT':keys.add(event['subject_key'])
            elif event['kind']=='IMAGE':
                with self.db.read_snapshot():image=self.db.get('product_images',dict(id=event['subject_key']),locking=False)
                if image:keys.add(image['source_code']+':'+image['source_product_id'])
        keys=sorted(keys)
        for start in range(0,len(keys),100):yield from self.read_products(keys[start:start+100],at=at)

    def acknowledge(self,captures):
        for capture in captures:
            pending=set(capture['snapshot'].get('pending_observation_keys',[]))
            for row in capture['snapshot']['observations']:
                if row['observation_key'] not in pending:continue
                self.collection.update_one({'observation_key':row['observation_key'],
                    'image_state_revision':row['image_state_revision'],'image_state_fingerprint':row['image_state_fingerprint']},
                    {'$unset':{'incremental_change':''}})

    def discovery_completed(self):atomic_json(self.root/'review-discovery.json',self.event_hashes)


class ProductionStages:
    def __init__(self,work,changes,config,options):
        self.work,self.changes,self.config,self.options=work,changes,config,options

    def current(self,captures):
        self.changes._reviews()
        actual={s['product_key']:s for s in self.changes.read_products([c['product_key'] for c in captures])}
        return all(actual[c['product_key']]['content_hash']==c['snapshot']['content_hash'] for c in captures)

    def freeze(self,directory,captures):
        from ..windows import seal_window
        from ..silver.selection import seal
        from ..gold.selection import published_choices
        from ..review import export_inputs
        from ..spark_session import release_spark
        frozen=directory/'incremental-freeze.json'
        if frozen.exists():
            marker=read_json(frozen)
            if marker['captures_hash']!=digest(captures) or any(sha_file(directory/k)!=v for k,v in marker['files'].items()):
                raise Conflict('INCREMENTAL_FROZEN_INPUT_CHANGED')
            return marker['first_stage']
        expected=[r for c in captures for r in c['snapshot']['observations']]
        keys=sorted(c['product_key'] for c in captures)
        version=self.work.pinned(self.options,self.work.LAKE+'/bronze',['products'])['products']
        spark=self.work.session(self.options)
        try:
            from pyspark.sql import functions as F
            frame=spark.read.format('delta').option('versionAsOf',version).load(self.work.LAKE+'/bronze/products')
            rows=[r.asDict() for r in frame.filter(F.col('observation_key').isin([r['observation_key'] for r in expected]))
                .select('observation_key','raw_document_id','document_hash','source_code','source_product_id').limit(101).collect()]
        finally:release_spark(spark)
        if len(rows)!=len(expected):
            from ..operations.queue import RetryLater
            raise RetryLater('INCREMENTAL_WAITING_FOR_BRONZE')
        if {(r['observation_key'],r['raw_document_id'],r['document_hash']) for r in rows}!={(r['observation_key'],r['raw_document_id'],r['document_hash']) for r in expected}:
            raise Conflict('INCREMENTAL_BRONZE_SOURCE_CHANGED')
        selection=seal_window(version,None,rows)
        atomic_json(directory/'selection.json',selection)
        with self.changes.db.read_snapshot():
            side=export_inputs(self.changes.db,[],as_of=now(),product_keys=keys)['silver']
            prior=published_choices(self.changes.db,keys)
        atomic_json(directory/'silver-inputs.json',side)
        stage=captures[0]['first_stage'];by_raw={r['raw_document_id']:r for r in prior}
        if stage=='gold' and all(r['raw_document_id'] in by_raw for r in expected):
            choices=[dict(product_key=r['source_code']+':'+r['source_product_id'],raw_document_id=r['raw_document_id'],
                observation_key=r['observation_key'],evaluation_key=by_raw[r['raw_document_id']]['evaluation_key'],
                evidence_ref='silver-receipt://'+by_raw[r['raw_document_id']]['evaluation_key']) for r in rows]
            atomic_json(directory/'silver-state/evaluation-selection.json',seal(selection,choices))
        else:stage='silver'
        # Marker last: a crash cannot turn partially prepared inputs into success.
        names=['selection.json','silver-inputs.json']
        if stage=='gold':names.append('silver-state/evaluation-selection.json')
        atomic_json(frozen,dict(first_stage=stage,selection_hash=selection['content_hash'],captures_hash=digest(captures),
            files={name:sha_file(directory/name) for name in names}))
        return stage

    def run(self,stage,directory):
        import os
        previous=os.environ.get('PIPELINE_CONTINUOUS');os.environ['PIPELINE_CONTINUOUS']='1'
        try:
            if stage=='publish':
                from ..publication.mysql import MySQL
                from ..review import enforce_expiry
                owner_root=self.work.BASE/'mysql-review-owner'
                settings=read_json(owner_root/'mysql-review.json')
                name=settings.get('password_env','PIPELINE_REVIEW_MYSQL_PASSWORD')
                os.environ[name]=read_json(owner_root/'owner.private.json')['password']
                owner=MySQL(settings)
                try:enforce_expiry(owner,read_json(directory/'selection.json')['product_keys'],as_of=now())
                finally:owner.close()
            result=self.work.run_at(stage,directory)
            if stage=='gold' and not (directory/'silver-result.json').exists():
                atomic_json(directory/'silver-result.json',dict(status='COMPLETED',stage='silver_reuse',
                    verification='PINNED_SILVER_RECEIPTS_VERIFIED_BY_GOLD',gold_result_sha256=sha_file(directory/'gold-result.json')))
            return result
        finally:
            if previous is None:os.environ.pop('PIPELINE_CONTINUOUS',None)
            else:os.environ['PIPELINE_CONTINUOUS']=previous

    def verify(self,directory,captures):
        from ..publication.audit import audit_reference
        from ..publication.source import GoldSource
        from ..spark_session import release_spark
        from ..files import sha_file
        roots=[directory];activation=read_json(directory/'activation-result.json')
        if activation.get('execution_root'):
            target=Path(activation['execution_root']).resolve()
            if not target.is_relative_to(directory.resolve()):raise Conflict('INCREMENTAL_ACTIVATION_ROOT_CHANGED')
            roots.append(target)
        choices={};proof_files=[];spark=self.work.session(self.options)
        try:
            for root in roots:
                runs=sorted((root/'gold-state/runs').glob('*.json'),key=lambda p:p.stat().st_mtime)
                if not runs:raise Conflict('INCREMENTAL_GOLD_RECEIPT_MISSING')
                gold=read_json(runs[-1]);proof_files.append(runs[-1])
                if gold.get('status')!='COMPLETED' or not gold.get('source_drained'):raise Conflict('INCREMENTAL_GOLD_INCOMPLETE')
                source=GoldSource(spark,gold['export_manifest_ref'],self.work.LAKE+'/gold',self.work.LAKE+'/checkpoints/gold')
                for page in source.manifest['selection_pages']:
                    for choice in source.read(page)['selections']:choices[choice['product_key']]=choice
        finally:release_spark(spark)
        artifacts=[dict(path=str(p),sha256=sha_file(p)) for p in sorted(set(directory.rglob('*-result.json'))|set(proof_files))]
        receipts={}
        with self.changes.db.read_snapshot():
            for capture in captures:
                key=capture['product_key'];product=self.changes.db.get('products',dict(product_key=key),locking=False)
                if not product:raise Conflict('INCREMENTAL_MYSQL_PRODUCT_MISSING')
                audit_reference(self.changes.db,product['gold_snapshot_ref_json'])
                choice=choices[key]
                future=[d for d in (choice.get('next_due_at'),capture['snapshot'].get('next_due_at')) if d and utc(d)>utc(now())]
                published=parse(product['gold_snapshot_ref_json'])['gold_evaluation_key']==choice['evaluation_key']
                status='PUBLISHED' if published and product['status']=='ACTIVE' else 'LAST_GOOD_RETAINED' if product['status']=='ACTIVE' else 'QUALITY_WAIT'
                receipts[key]=dict(status=status,
                    publication_hash=publication_hash(product),next_due_at=min(future,default=None),artifacts=artifacts,
                    plan_id=parse(product['gold_snapshot_ref_json'])['plan_id'])
        return receipts
