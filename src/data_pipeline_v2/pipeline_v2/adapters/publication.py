"""Direct publication: no guard registration, review task or second ETL pass."""
from pathlib import Path
from datetime import datetime
from uuid import UUID
import time
import json
from decimal import Decimal
from contextlib import nullcontext

from ..domain.contracts import canonical, digest, seal, verified, stamp, Invalid
from .files import read, write, immutable, locked
from .sql_audit import service_rows, verify_service


class FilePublisher:
    def __init__(self, root):
        self.root=Path(root).resolve()
        self.identity=dict(kind='FILE_PUBLICATION',root=str(self.root))
        self.writes=0

    def publish(self, delivery):
        if delivery['target']!=self.identity:raise Invalid('PUBLICATION_TARGET_CHANGED')
        key=delivery['job_id'];path=self.root/'receipts'/(key+'.json')
        with locked(self.root/'publisher.lock'):
            if path.exists():
                receipt=verified(read(path));self.audit(delivery,receipt);return receipt
            product=delivery['product']
            previous=self.root/'current'/(digest(product['product_key'])+'.json')
            if product.get('withdrawn'):
                # Every retained source version was retracted: hide, never delete history.
                outcome='WITHDRAWN' if previous.exists() else 'QUALITY_WAIT'
                if previous.exists():
                    live=verified(read(self.root/'versions'/(verified(read(previous))['job_id']+'.json')))['source_refs']
                    if any(r['_id'] not in set(product['retracted_raw_document_ids']) for r in live):
                        # The current version holds evidence this withdrawal did not see.
                        receipt=seal(dict(job_id=key,product_hash=digest(product),resolution='LAST_GOOD_RETAINED'))
                        immutable(path,receipt);return receipt
                    immutable(self.root/'withdrawn'/(key+'.json'),seal(dict(product_key=product['product_key'],
                        retracted=product['retracted_raw_document_ids'],previous=verified(read(previous))['job_id'])))
                    previous.unlink()
                receipt=seal(dict(job_id=key,product_hash=digest(product),resolution=outcome))
                immutable(path,receipt);return receipt
            stale=False
            if previous.exists():
                previous_id=verified(read(previous))['job_id']
                old=verified(read(self.root/'versions'/(previous_id+'.json')))
                retracted=set(product.get('retracted_raw_document_ids') or [])
                kept=[r['collected_at'] for r in old['source_refs'] if r['_id'] not in retracted]
                stale=bool(kept) and max(r['collected_at'] for r in product['source_refs']) < max(kept)
            if stale:
                outcome='LAST_GOOD_RETAINED'
            elif product['catalog_ready']:
                immutable(self.root/'versions'/(key+'.json'),seal(product));self.writes+=1
                write(self.root/'current'/(digest(product['product_key'])+'.json'),seal(dict(job_id=key)))
                outcome='PUBLISHED'
            else:
                previous=self.root/'current'/(digest(product['product_key'])+'.json')
                if previous.exists():
                    previous_id=verified(read(previous))['job_id']
                    verified(read(self.root/'versions'/(previous_id+'.json')))
                outcome='LAST_GOOD_RETAINED' if previous.exists() else 'QUALITY_WAIT'
            receipt=seal(dict(job_id=key,product_hash=digest(product),resolution=outcome))
            immutable(path,receipt);return receipt

    def audit(self, delivery, receipt):
        verified(receipt)
        if receipt['job_id']!=delivery['job_id'] or receipt['product_hash']!=digest(delivery['product']):
            raise Invalid('PUBLICATION_RECEIPT_CHANGED')
        if receipt['resolution']=='PUBLISHED' and verified(read(self.root/'versions'/(delivery['job_id']+'.json'))) != seal(delivery['product']):
            raise Invalid('PUBLICATION_OUTPUT_CHANGED')
        if receipt['resolution']=='WITHDRAWN':
            verified(read(self.root/'withdrawn'/(delivery['job_id']+'.json')))
        if receipt['resolution']=='LAST_GOOD_RETAINED':
            prior=verified(read(self.root/'current'/(digest(delivery['product_key'])+'.json')))
            verified(read(self.root/'versions'/(prior['job_id']+'.json')))

    def close(self):pass


FIELD_SOURCES=[('name','display_name'),('model_name','model'),('manufacturer','manufacturer'),('brand','brand'),
    ('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm'),('comparison_url','comparison_url')]


def identity(db, absent, table, key, values):
    """Business-key insert; skips the lookup when `absent` and the transport supports it."""
    if absent and hasattr(db,'identity_absent'):
        return db.identity_absent(table,key,values)
    return db.identity(table,key,values)


def offer_prices(group):
    """Stable seller observations of the selected offers, kept as price history."""
    identities={o['identity']['offer_key'] for o in group['offers']}
    return [p for p in group['price_observations'] if p.get('offer_key') in identities
            and p['currency']=='KRW' and p['numeric_status']=='CONFIRMED']


def price_snapshot_key(price):
    return 'direct-price:'+digest(dict(offer=price['offer_key'],raw=price['raw_document_id'],
        condition=price['condition_fingerprint'],amount=price['amount']))


class StaleInput(Invalid):
    pass


def sql_time(value):
    return datetime.fromisoformat(value.replace('Z','+00:00')).replace(tzinfo=None) if value else None


class MySQLPublisher:
    def __init__(self, config, fence=lambda:None,timing=None):
        self.config=config;self.db=None;self.checked=0;self.remaining=0
        self.fence=fence
        self.timing=timing;self.sql_metrics={'sql_queries':0}
        self.identity=dict(kind='MYSQL_DIRECT_V2',target_id=config['target_id'],database=config['database'])

    def _stage(self,name,scope_id,job_id=None):
        return self.timing.measure(name,scope_id,job_id=job_id,counters=self.sql_metrics) if self.timing else nullcontext()

    def _database(self,scope_id='database-connect',job_id=None):
        from .mysql import DirectMySQL
        with self._stage('mysql.connect_and_schema_preflight',scope_id,job_id):
            if self.db is None:
                self.db=DirectMySQL(self.config);self.db.query_metrics=self.sql_metrics
                self.db.timing=self.timing
            if self.remaining<=0 or time.monotonic()-self.checked>=60:
                self.db.preflight();self.checked=time.monotonic();self.remaining=100
        return self.db

    def publish(self, delivery):
        if delivery['target']!=self.identity:raise Invalid('PUBLICATION_TARGET_CHANGED')
        self.fence()
        key=delivery['job_id'];group=delivery['product'];body_hash=digest(group);scope_id=delivery.get('scope_id','job-'+key)
        self._active_scope,self._active_job=scope_id,key
        db=self._database(scope_id,key)
        with self._stage('mysql.staging_transaction',scope_id,key):
          with db.transaction():
            record,_=db.identity('pipeline_publications',dict(plan_id=key),dict(plan_hash=body_hash,
                status='STAGED',gold_versions_json=canonical(dict(contract_version=3,proofs=delivery['gold']['proofs'])),product_count=1,completed_at=None))
            if record['plan_hash']!=body_hash:raise Invalid('PUBLICATION_PLAN_CONFLICT')
            prior=db.get('pipeline_publication_staging',dict(plan_id=key,product_key=group['product_key']))
            if prior and prior['body_hash']!=body_hash:raise Invalid('PUBLICATION_STAGING_CONFLICT')
            if not prior:db.identity('pipeline_publication_staging',dict(plan_id=key,product_key=group['product_key']),
                dict(body_hash=body_hash,body_json=canonical(group)))
            self.fence()
        with self._stage('mysql.service_transaction',scope_id,key):
          with db.transaction():
            saved=db.get('pipeline_publications',dict(plan_id=key))
            if saved['status']=='COMPLETED':
                receipt=seal(dict(job_id=key,product_hash=body_hash,publication_id=saved['id'],resolution=json.loads(saved['gold_versions_json'])['resolution']))
                self.audit(delivery,receipt);return receipt
            at=db.one('SELECT UTC_TIMESTAMP(3) AS stamp')['stamp']
            old=db.get('products',dict(product_key=group['product_key']))
            resolution='LAST_GOOD_RETAINED' if old and not group['catalog_ready'] else 'QUALITY_WAIT'
            if group['catalog_ready']:
                with self._stage('mysql.product_projection',scope_id,key):
                    self._product(db,group,key,at,old);resolution='PUBLISHED'
            with self._stage('mysql.service_readback_audit',scope_id,key):
                records=service_rows(db,group,key) if resolution=='PUBLISHED' else []
            db.update('pipeline_publications',dict(plan_id=key),dict(status='COMPLETED',completed_at=at,
                gold_versions_json=canonical(dict(contract_version=3,proofs=delivery['gold']['proofs'],resolution=resolution,service_rows=records))))
            receipt=seal(dict(job_id=key,product_hash=body_hash,publication_id=saved['id'],resolution=resolution))
            self.fence()
        self.remaining-=1
        return receipt

    def publish_many(self, deliveries):
        """One bounded staging/service pair, with keyed prefetch and per-product atomic relations."""
        if not deliveries:
            return []
        if len(deliveries) > 100:
            raise Invalid('MYSQL_PUBLICATION_BATCH_LIMIT')
        if any(d['target'] != self.identity for d in deliveries):
            raise Invalid('PUBLICATION_TARGET_CHANGED')
        scope = deliveries[0].get('scope_id','sql-batch')
        self._active_scope = scope
        db = self._database(scope)
        ordered = sorted(deliveries,key=lambda d:d['product_key'])
        self.fence()
        with self._stage('mysql.staging_batch_transaction',scope):
            with db.transaction():
                db.prefetch('pipeline_publications',[dict(plan_id=d['job_id']) for d in ordered])
                db.prefetch('pipeline_publication_staging',[dict(plan_id=d['job_id'],product_key=d['product_key']) for d in ordered])
                for delivery in ordered:
                    key,group = delivery['job_id'],delivery['product']
                    body_hash = digest(group)
                    record,_ = db.identity('pipeline_publications',dict(plan_id=key),dict(plan_hash=body_hash,
                        status='STAGED',gold_versions_json=canonical(dict(contract_version=3,proofs=delivery['gold']['proofs'])),product_count=1,completed_at=None))
                    if record['plan_hash'] != body_hash:
                        raise Invalid('PUBLICATION_PLAN_CONFLICT')
                    prior = db.get('pipeline_publication_staging',dict(plan_id=key,product_key=group['product_key']))
                    if prior and prior['body_hash'] != body_hash:
                        raise Invalid('PUBLICATION_STAGING_CONFLICT')
                    if not prior:
                        db.identity('pipeline_publication_staging',dict(plan_id=key,product_key=group['product_key']),
                            dict(body_hash=body_hash,body_json=canonical(group)))
                self.fence()
        receipts = {}
        with self._stage('mysql.service_batch_transaction',scope):
            with db.transaction():
                db.prefetch('pipeline_publications',[dict(plan_id=d['job_id']) for d in ordered])
                db.prefetch('products',[dict(product_key=d['product_key']) for d in ordered])
                db.prefetch('pipeline_product_versions',[dict(product_key=d['product_key']) for d in ordered])
                ready = [d['product'] for d in ordered if d['product']['catalog_ready'] and not d['product'].get('withdrawn')]
                db.prefetch('categories',[dict(id=UUID(g['category_id']).hex) for g in ready])
                db.prefetch('assets',[dict(bucket=i['bucket'],object_key=i['object_key']) for g in ready for i in g['images']])
                db.prefetch('offers',[dict(offer_key=o['identity']['offer_key']) for g in ready for o in g['offers']])
                db.prefetch('offer_snapshots',[dict(snapshot_key=price_snapshot_key(p)) for g in ready for p in offer_prices(g)])
                at = db.one('SELECT UTC_TIMESTAMP(3) AS stamp')['stamp']
                for delivery in ordered:
                    key,group = delivery['job_id'],delivery['product']
                    self._active_job = key
                    saved = db.get('pipeline_publications',dict(plan_id=key))
                    body_hash = digest(group)
                    if saved['status'] == 'COMPLETED':
                        receipt = seal(dict(job_id=key,product_hash=body_hash,publication_id=saved['id'],
                            resolution=json.loads(saved['gold_versions_json'])['resolution']))
                        self.audit(delivery,receipt)
                        receipts[key] = receipt
                        continue
                    old = db.get('products',dict(product_key=group['product_key']))
                    resolution = 'LAST_GOOD_RETAINED' if old and not group['catalog_ready'] else 'QUALITY_WAIT'
                    if group.get('withdrawn'):
                        resolution = self._withdraw(db,group,key,at,old)
                    elif group['catalog_ready']:
                        try:
                            self._product(db,group,key,at,old)
                            resolution = 'PUBLISHED'
                        except StaleInput:
                            # An older finite batch never overwrites a newer published version.
                            resolution = 'LAST_GOOD_RETAINED'
                    with self._stage('mysql.service_readback',scope,key):
                        records = service_rows(db,group,key) if resolution == 'PUBLISHED' else []
                    db.update('pipeline_publications',dict(plan_id=key),dict(status='COMPLETED',completed_at=at,
                        gold_versions_json=canonical(dict(contract_version=3,proofs=delivery['gold']['proofs'],resolution=resolution,service_rows=records))))
                    receipts[key] = seal(dict(job_id=key,product_hash=body_hash,publication_id=saved['id'],resolution=resolution))
                self.fence()
        self.remaining -= len(deliveries)
        return [receipts[d['job_id']] for d in deliveries]

    def _product(self, db, group, key, at, old):
        from product_pipeline.publication.policy import guarded_projection, attribute_projection
        category=db.get('categories',dict(id=UUID(group['category_id']).hex))
        if not category or not category['active'] or category['code']!=group['category_code']:raise Invalid('CATEGORY_CHANGED')
        baseline=db.get('pipeline_product_versions',dict(product_key=group['product_key']))
        if old and not baseline:raise Invalid('EXISTING_PRODUCT_BASELINE_REQUIRED')
        incoming={k:group[k] for k in ('manufacturer','brand','width_mm','depth_mm','height_mm')}
        incoming.update(name=group['display_name'],model_name=group['model'],comparison_url=group['comparison_url'])
        lineage=dict(group['lineage']);lineage.update(name=lineage['display_name'],model_name=lineage['model'])
        comparable=dict(old or {})
        for field in ('width_mm','depth_mm','height_mm'):
            if incoming.get(field) is not None:incoming[field]=format(Decimal(str(incoming[field])).normalize(),'f')
            if comparable.get(field) is not None:comparable[field]=format(Decimal(str(comparable[field])).normalize(),'f')
        previous=json.loads(baseline['field_versions_json']) if baseline else {}
        retracted=set(group.get('retracted_raw_document_ids') or [])
        prior_lineage=json.loads(old['field_lineage_json']) if old else {}
        cleared=set()
        if retracted:
            # Explicitly retracted source versions lose their field proofs and stored values.
            previous={f:p for f,p in previous.items() if p.get('raw_document_id') not in retracted}
            for name,source in FIELD_SOURCES:
                if (prior_lineage.get(source) or {}).get('raw_document_id') in retracted:
                    cleared.add(name);comparable[name]=None
        # A new normalization policy may correct the same raw evidence. A different
        # raw document at equal event time still conflicts rather than winning by arrival.
        for field,proof in list(previous.items()):
            evidence=lineage.get(field) or {}
            if (proof.get('raw_document_id')==evidence.get('raw_document_id') and proof.get('policy_hash')
                    and proof['policy_hash']!=group['normalization_policy_hash']):
                previous.pop(field)
        values,proofs=guarded_projection(comparable,incoming,lineage,previous)
        for name in cleared:
            values.setdefault(name,None)
        for field in values:
            if field in proofs:
                proofs[field].update(raw_document_id=(lineage.get(field) or {}).get('raw_document_id'),policy_hash=group['normalization_policy_hash'])
        reference_body=dict(schema_version=3,contract='direct-catalog-1',plan_id=key,product_key=group['product_key'],source_refs=group['source_refs'],
            normalization_policy_hash=group['normalization_policy_hash'])
        if old:
            prior_ref=json.loads(old['gold_snapshot_ref_json'])
            retained=[{k:v for k,v in prior_ref.items() if k!='retained_snapshot_refs'},*prior_ref.get('retained_snapshot_refs',[])]
            reference_body['retained_snapshot_refs']=list({r['plan_id']:r for r in retained}.values())
        reference=canonical(reference_body)
        incoming_latest=sql_time(max(r['collected_at'] for r in group['source_refs']))
        if retracted and old:
            # A retraction lowers the bar only by the evidence it removes. Evidence it did not see stays protected.
            kept=[sql_time(r['collected_at']) for r in json.loads(old['source_refs_json']) if r['_id'] not in retracted]
            latest=not kept or incoming_latest>=max(kept)
        else:
            latest=not baseline or incoming_latest>=baseline['latest_as_of']
        if not latest:raise StaleInput('STALE_PRODUCT_INPUT')
        prior_attributes=[r for r in (json.loads(old['attributes_json']) if old else []) if r.get('raw_document_id') not in retracted]
        if old and prior_ref.get('normalization_policy_hash') and prior_ref['normalization_policy_hash']!=group['normalization_policy_hash']:
            corrected={r['raw_document_id'] for r in group['attributes']}
            prior_attributes=[r for r in prior_attributes if r.get('raw_document_id') not in corrected]
        attributes=attribute_projection(prior_attributes,group['attributes'])
        source_refs=list({canonical(r):r for r in [*(json.loads(old['source_refs_json']) if old else []),*group['source_refs']]
            if r['_id'] not in retracted}.values())
        stored_lineage=dict(group['lineage'])
        if old:
            for name,source in FIELD_SOURCES:
                if name in cleared and values.get(name) is None:stored_lineage[source]=None
                elif old.get(name) is not None and name not in values:stored_lineage[source]=prior_lineage.get(source)
        statuses={dict(name='display_name',model_name='model').get(k,k):'CONFIRMED' if values.get(k,(old or {}).get(k)) is not None else 'UNKNOWN' for k in incoming}
        values.update(category_id=category['id'],status='ACTIVE',
            dedup_status='SOURCE_IDENTITY_ONLY',attributes_json=canonical(attributes),
            field_status_json=canonical(statuses),
            field_lineage_json=canonical(stored_lineage),source_refs_json=canonical(source_refs),price_krw=(old or {}).get('price_krw'),
            first_collected_at=min(filter(None,[sql_time(group['first_collected_at']),baseline['first_collected_at'] if baseline else None])),
            catalog_ready=True,recommendation_ready=False,comparison_link_ready=bool(values.get('comparison_url',(old or {}).get('comparison_url'))),purchase_link_ready=False,
            quality_reason_codes=canonical(group['quality_reason_codes']),publication_status='DIRECT_PUBLISHED',
            publication_reason_codes='[]',embedding_json=(old or {}).get('embedding_json','null'),gold_snapshot_ref_json=reference,
            representative_image_id=(old or {}).get('representative_image_id'),updated_at=at)
        created=False
        if old:
            db.update('products',dict(id=old['id']),values);product=dict(old,**values)
        else:
            product,created=db.identity('products',dict(product_key=group['product_key']),dict(values,source_code=group['source_code'],
                source_product_id=group['source_product_id'],created_at=at))
        images=[]
        with self._stage('mysql.asset_and_image_links',getattr(self,'_active_scope','sql-projection'),key):
          for image in group['images']:
            asset,_=db.identity('assets',dict(bucket=image['bucket'],object_key=image['object_key']),dict(owner_id=None,
                kind='PRODUCT_IMAGE',content_type='image/'+image['format'].lower().replace('jpg','jpeg'),size_bytes=image['bytes'],
                width_px=image['width'],height_px=image['height'],checksum=image['sha256'],status='AVAILABLE',
                usage_policy_json=canonical(dict(workflow='DIRECT_ETL',policy_version='direct-catalog-1')),created_at=at,updated_at=at))
            if asset['checksum']!=image['sha256'] or asset['size_bytes']!=image['bytes']:raise Invalid('ASSET_CONTENT_CONFLICT')
            relation='direct-image:'+digest(dict(product_key=group['product_key'],raw=image['raw_document_id'],slot=image['image_slot'],version=image['source_version']))
            linked,_=identity(db,created,'product_images',dict(product_id=product['id'],source_code=group['source_code'],
                source_product_id=group['source_product_id'],image_variant=image['image_slot'],raw_document_id=image['raw_document_id'],
                source_version=image['source_version']),dict(asset_id=asset['id'],display_order=image['order'],source_url=image['source_url'],
                raw_payload_hash=image['raw_payload_hash'],raw_collected_at=sql_time(image['raw_collected_at']),linked_at=at,active=True,
                usage_review_json=canonical(dict(workflow='DIRECT_ETL',review_required=False)),created_at=at,updated_at=at,
                image_relation_key=relation,gold_recommendation_approved=False,gold_review_snapshot_hash=None,
                source_evidence_json=canonical(image),gold_snapshot_ref_json=reference))
            if linked['asset_id']!=asset['id']:raise Invalid('IMAGE_ASSET_CONFLICT')
            if not linked['active']:db.update('product_images',dict(id=linked['id']),dict(active=True,updated_at=at))
            if image['image_slot']=='representative':images.append(linked['id'])
        if len(images)>1:raise Invalid('REPRESENTATIVE_IMAGE_CONFLICT')
        if images:db.update('products',dict(id=product['id']),dict(representative_image_id=images[0]))
        if retracted:
            self._deactivate_images(db,product,retracted,at,keep=images)
        with self._stage('mysql.offer_and_price_rows',getattr(self,'_active_scope','sql-projection'),key):
            self._offers(db,product,group,reference,at)
        with self._stage('mysql.link_observation_rows',getattr(self,'_active_scope','sql-projection'),key):
            self._links(db,product,group,key,reference,at,absent=created)
        current=dict(field_versions_json=canonical(proofs),first_collected_at=values['first_collected_at'],
            latest_as_of=sql_time(max(r['collected_at'] for r in group['source_refs'])),latest_evaluation=key)
        if baseline:db.update('pipeline_product_versions',dict(product_key=group['product_key']),current)
        else:db.identity('pipeline_product_versions',dict(product_key=group['product_key']),current)

    def _offers(self, db, product, group, reference, at):
        for offer in group['offers']:
            identity,price=offer['identity'],offer['price'];observed=sql_time(price['observed_at'])
            amount=int(price['amount'])
            values=dict(product_id=product['id'],source_code=group['source_code'],source_product_id=group['source_product_id'],
                source_seller_id=identity['source_seller_id'],source_offer_id=identity['source_offer_id'],source_variant_id=identity['option_key'],
                title=None,price_amount=amount,currency='KRW',purchase_url=None,availability='UNKNOWN',collected_at=observed,
                latest_raw_document_id=price['raw_document_id'],eligible=False,price_basis='UNKNOWN',
                price_conditions_json=price['condition_json'],option_attributes_json=canonical(dict(option_key=identity['option_key'])),
                gold_snapshot_ref_json=reference,created_at=at,updated_at=at)
            saved,created=db.identity('offers',dict(offer_key=identity['offer_key']),values)
            if saved['product_id']!=product['id']:raise Invalid('OFFER_PRODUCT_CONFLICT')
            if not created and saved['collected_at']==observed and saved['price_amount']!=amount:raise Invalid('OFFER_EQUAL_TIME_CONFLICT')
            if not created and (saved['collected_at']<observed or
                    saved['latest_raw_document_id'] in set(group.get('retracted_raw_document_ids') or [])):
                db.update('offers',dict(id=saved['id']),{k:v for k,v in values.items() if k not in {'product_id','created_at'}})
        # All stable seller observations survive as history, not just the selected current price.
        for price in offer_prices(group):
            offer=db.get('offers',dict(offer_key=price['offer_key']))
            key=price_snapshot_key(price)
            values=dict(offer_id=offer['id'],raw_document_id=price['raw_document_id'],price_amount=int(price['amount']),currency='KRW',
                purchase_url=None,availability='UNKNOWN',collected_at=sql_time(price['observed_at']),pricing_terms_json=price['condition_json'],
                gold_snapshot_ref_json=reference,created_at=at)
            saved,created=db.identity('offer_snapshots',dict(snapshot_key=key),values)
            if not created and any(saved[k]!=values[k] for k in ('offer_id','raw_document_id','price_amount','currency','pricing_terms_json')):
                raise Invalid('PRICE_HISTORY_CONFLICT')

    def _deactivate_images(self, db, product, retracted, at, keep=()):
        for raw in sorted(retracted):
            # Multi-row UPDATE by product and retracted raw version; other evidence stays active.
            db.update('product_images',dict(product_id=product['id'],raw_document_id=raw),dict(active=False,updated_at=at))
        current=db.get('products',dict(id=product['id']))
        if current and current['representative_image_id'] and current['representative_image_id'] not in keep:
            image=db.get('product_images',dict(id=current['representative_image_id']))
            if image and image['raw_document_id'] in retracted:
                db.update('products',dict(id=product['id']),dict(representative_image_id=None))

    def _withdraw(self, db, group, key, at, old):
        """No valid evidence remains after explicit retraction: hide the product, keep its history."""
        if not old:
            return 'QUALITY_WAIT'
        retracted=set(group['retracted_raw_document_ids'])
        if any(r['_id'] not in retracted for r in json.loads(old['source_refs_json'])):
            # The published version holds evidence this withdrawal did not see; it never hides newer data.
            return 'LAST_GOOD_RETAINED'
        db.update('products',dict(id=old['id']),dict(status='INACTIVE',catalog_ready=False,recommendation_ready=False,
            publication_status='SOURCE_RETRACTED',publication_reason_codes=canonical(['SOURCE_RETRACTED']),updated_at=at))
        self._deactivate_images(db,old,set(group['retracted_raw_document_ids']),at)
        baseline=db.get('pipeline_product_versions',dict(product_key=group['product_key']))
        if baseline:
            db.update('pipeline_product_versions',dict(product_key=group['product_key']),dict(latest_evaluation=key))
        return 'WITHDRAWN'

    def _links(self, db, product, group, key, reference, at, absent=False):
        for link in group['links']:
            values=dict(link_kind=link['link_kind'],source_url=link['source_url'],offer_key=link['offer_key'],
                usable=bool(link['link_kind']=='COMPARISON' and link['source_url']==group['comparison_url']),
                raw_document_id=link['raw_document_id'],evidence_json=link['evidence_json'],gold_snapshot_ref_json=reference,created_at=at)
            # A product created in this transaction has no link rows yet.
            saved,_=identity(db,absent,'product_link_observations',dict(product_id=product['id'],link_observation_key=link['link_observation_key'],gold_evaluation_key=key),values)
            if saved['source_url']!=values['source_url']:raise Invalid('LINK_OBSERVATION_CONFLICT')

    def audit(self, delivery, receipt):
        verified(receipt);db=self.db if self.db is not None else self._database(delivery.get('scope_id','job-'+delivery['job_id']),delivery['job_id'])
        if receipt['job_id']!=delivery['job_id'] or receipt['product_hash']!=digest(delivery['product']):
            raise Invalid('SQL_RECEIPT_CHANGED')
        saved=db.get('pipeline_publications',dict(plan_id=delivery['job_id']),locking=False)
        staged=db.get('pipeline_publication_staging',dict(plan_id=delivery['job_id'],product_key=delivery['product_key']),locking=False)
        if (not saved or saved['status']!='COMPLETED' or saved['plan_hash']!=receipt['product_hash'] or not staged
                or staged['body_hash']!=digest(delivery['product']) or digest(json.loads(staged['body_json']))!=staged['body_hash']
                or saved['id']!=receipt['publication_id']
                or json.loads(saved['gold_versions_json']).get('resolution')!=receipt['resolution']
                or json.loads(saved['gold_versions_json']).get('proofs')!=delivery['gold']['proofs']):
            raise Invalid('SQL_PUBLICATION_AUDIT_FAILED')
        if receipt['resolution']=='WITHDRAWN':
            product=db.get('products',dict(product_key=delivery['product_key']),locking=False)
            baseline=db.get('pipeline_product_versions',dict(product_key=delivery['product_key']),locking=False)
            if (not product or (baseline and baseline['latest_evaluation']==delivery['job_id']
                    and (product['status']!='INACTIVE' or product['catalog_ready']))):
                raise Invalid('SQL_WITHDRAWAL_AUDIT_FAILED')
        if receipt['resolution']=='PUBLISHED':
            product=db.get('products',dict(product_key=delivery['product_key']),locking=False)
            if not product or product['source_product_id']!=delivery['product']['source_product_id']:raise Invalid('SQL_PRODUCT_AUDIT_FAILED')
            baseline=db.get('pipeline_product_versions',dict(product_key=delivery['product_key']),locking=False)
            if not baseline:raise Invalid('SQL_PRODUCT_VERSION_MISSING')
            if baseline['latest_evaluation']==delivery['job_id']:
                verify_service(db,json.loads(saved['gold_versions_json'])['service_rows'])

    def close(self):
        if self.db is not None:self.db.close()
