"""Read-only exact-scope reconciliation. Counts alone never prove correspondence."""
from pathlib import Path

from ..contracts import canonical, digest, registration_identity
from ..errors import Conflict, PipelineError
from ..files import atomic_json, confined, parse, read_json, rows, sha_file
from .flow import verified


def prepared(directory, export_hash, target):
    from ..publication.plan import load_plan
    ready = verified(read_json(Path(directory)/'ready.json'))
    if (ready.get('stage') != 'PUBLICATION_PLANS_READY' or ready.get('export_hash') != export_hash
            or ready.get('target') != target): raise Conflict('FLOW_PUBLICATION_READY_CHANGED')
    keys=set()
    for name, checksum in ready['plans'].items():
        plan=load_plan(confined(directory, name))
        if digest(plan)!=checksum or plan['export_hash']!=export_hash or plan['target']!=target:
            raise Conflict('FLOW_PUBLICATION_PLAN_CHANGED')
        for item in plan['products']:
            if item['product_key'] in keys: raise Conflict('FLOW_DUPLICATE_PLANNED_PRODUCT')
            keys.add(item['product_key'])
    for item in ready['blocked']:
        if item['product_key'] in keys: raise Conflict('FLOW_PLANNED_AND_BLOCKED')
        keys.add(item['product_key'])
    return dict(plans=len(ready['plans']), blocked=len(ready['blocked']), products=len(keys))


def compare_sorted(expected, actual, key, fields):
    """Streaming equality for explicit sorted scope; useful without Spark too."""
    left=iter(expected);right=iter(actual);count=0;previous=None;sentinel=object()
    while True:
        a=next(left,sentinel);b=next(right,sentinel)
        if a is sentinel and b is sentinel:return count
        if a is sentinel or b is sentinel:raise Conflict('FLOW_AUDIT_MEMBERSHIP_MISMATCH')
        if a[key]!=b[key] or (previous is not None and a[key]<=previous):
            raise Conflict('FLOW_AUDIT_KEY_MISMATCH')
        if any(a.get(k)!=b.get(k) for k in fields):raise Conflict('FLOW_AUDIT_CONTENT_MISMATCH')
        previous=a[key];count+=1


def frame_audit(expected, actual, key, fields):
    """Limit actual to requested scope, then externally sort/stream on Spark."""
    selected=actual.join(expected.select(key).distinct(), key, 'left_semi')
    names=[key,*fields]
    return compare_sorted((r.asDict() for r in expected.select(*names).orderBy(key).toLocalIterator()),
        (r.asDict() for r in selected.select(*names).orderBy(key).toLocalIterator()),key,fields)


def sql_plan_audit(plan, db):
    """Check durable SQL receipts, product identities and immutable image/price links.

    Current values/approval fields may legitimately change later; do not equate
    an old plan to the current product projection or overwrite owner decisions.
    """
    from ..publication.plan import verify_plan
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
            from ..publication.runner import money
            offer=db.get('offers',dict(offer_key=row['offer_key']))
            if (not saved or not offer or saved['offer_id']!=offer['id']
                    or (plan['price_history_mode']=='SNAPSHOT_V1' and saved['raw_document_id']!=row['raw_document_id'])
                    or saved['price_amount']!=money(row['amount']) or saved['currency']!=row['currency']
                    or parse(saved['pricing_terms_json'])!=parse(row['evidence_json'])['price_effect']):
                raise Conflict('FLOW_SQL_PRICE_HISTORY_CHANGED')
            history+=1
    return dict(products=len(plan['products']),images=images,offers=offers,history=history)


def live_audit(context, p, source, bronze, silver, gold):
    from ..cli import _env
    from ..mongo import MongoLedger, wire_document
    from ..objects import S3Objects
    from ..publication.mysql import MySQL
    from ..publication.source import GoldSource
    from ..publication.plan import load_plan
    from ..gold.source import SilverSource
    from .flow_runtime import spark_session
    from bson import ObjectId
    root=Path(context['root']);audit_root=Path(context.get('audit_directory',str(root)))
    outputs=context['outputs'];expected_file=audit_root/'audit-expected.jsonl'
    endpoint=_env(source.s3_endpoint_env)
    if endpoint!='aws' and not endpoint.startswith('https://'):raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
    objects=S3Objects.from_settings(source,endpoint)
    ledger=MongoLedger(source,_env(source.mongo_uri_env))
    image_count=0;image_total=0;count=0
    try:
        with expected_file.open('w',encoding='utf-8') as stream:
            for row in rows(Path(context['flow']['snapshot'])/'observations.jsonl'):
                saved=ledger.get(row['observation_key']);doc=saved['document']
                if (saved['identity']!=registration_identity(row) or saved['target_fingerprint']!=source.fingerprint
                        or doc['observation_key']!=row['observation_key']):
                    raise Conflict('FLOW_MONGO_REGISTRATION_CHANGED')
                from ..bronze.contracts import validate_raw
                validate_raw(doc,saved['event'])
                actual=ledger.raw.find_one({'_id':ObjectId(doc['_id'])})
                if actual is None or wire_document(actual)!=doc or not saved.get('event_confirmed'):
                    raise Conflict('FLOW_MONGO_RAW_OR_EVENT_UNCONFIRMED')
                vector={r['image_slot']:r for r in saved['image_state_vector']}
                for image in doc['image_refs']:
                    image_total+=1
                    state=vector.get(image['image_slot'])
                    if state and state['availability']=='AVAILABLE':
                        if (state['checksum']!=image['sha256'] or state['object_key']!=image['object_key'] or state['bucket']!=source.image_bucket
                                or not objects._exists(image['object_key'],image['sha256'],image['bytes'])):
                            raise Conflict('FLOW_IMAGE_OBJECT_MISSING_OR_CHANGED')
                        image_count+=1
                event=saved['event']
                stream.write(canonical(dict(observation_key=doc['observation_key'],raw_document_id=doc['_id'],
                    document_hash=doc['document_hash'],payload_hash=doc['payload_hash'],event_id=event['event_id'],
                    event_hash=digest(event),product_key=doc['source_code']+':'+doc['source_product_id']))+'\n');count+=1
    finally:ledger.close()
    if not count:raise PipelineError('FLOW_AUDIT_EMPTY_SNAPSHOT')
    spark=spark_session();db=None
    try:
        expected=spark.read.schema('observation_key string, raw_document_id string, document_hash string, payload_hash string, event_id string, event_hash string, product_key string').json(str(expected_file))
        def delta(path,version):return spark.read.format('delta').option('versionAsOf',version).load(path)
        raw=delta(bronze.bronze_path,outputs['BRONZE']['versions']['raw'])
        raw_count=frame_audit(expected,raw,'raw_document_id',['observation_key','document_hash','payload_hash'])
        events=delta(bronze.events_path,outputs['BRONZE']['versions']['events'])
        event_count=frame_audit(expected,events,'event_id',['raw_document_id','document_hash','event_hash'])
        # Validate every pinned Silver receipt and its exact output hashes using
        # the same bounded reader as Gold. Multiple evaluations are legitimate.
        reader=SilverSource(spark,gold,outputs['SILVER']['versions'])
        silver_receipts=reader.frames['pipeline_evaluations']
        from pyspark.sql import functions as F
        confirmed=silver_receipts.select('raw_document_id','document_hash').distinct()
        if expected.join(confirmed,['raw_document_id','document_hash'],'left_anti').limit(1).count():
            raise Conflict('FLOW_SILVER_COVERAGE_MISSING')
        products=expected.select('product_key').distinct()
        for item in products.orderBy('product_key').toLocalIterator():
            for group in reader.groups(item['product_key']):
                # SilverSource validates manifests/hashes in groups().
                if not group['tables']:raise Conflict('FLOW_SILVER_OUTPUT_MISSING')
        gold_reader=GoldSource(spark,outputs['GOLD']['export_manifest_ref'],gold.gold_root,gold.checkpoint_path)
        seen=set();gold_raw=set()
        for group in gold_reader.groups():
            catalog=group['tables']['product_catalog'][0];seen.add(catalog['product_key'])
            for ref in parse(catalog['source_refs_json']):gold_raw.add((ref['raw_document_id'],ref['document_hash']))
        # Gold scope may include older fixed lake products; every requested raw
        # observation still must be present in its preserved source references.
        for row in expected.select('raw_document_id','document_hash','product_key').toLocalIterator():
            if row['product_key'] not in seen or (row['raw_document_id'],row['document_hash']) not in gold_raw:
                raise Conflict('FLOW_GOLD_COVERAGE_MISSING')
        directory=Path(outputs['PREPARE']['directory']);ready=read_json(directory/'ready.json')
        prepared(directory,gold_reader.manifest['content_hash'],ready['target'])
        planned=set();blocked={r['product_key'] for r in ready['blocked']};sql_counts=dict(products=0,images=0,offers=0,history=0)
        if ready['plans']:
            db=MySQL(read_json(p['mysql_config']));db.preflight()
            # Read-only row-locking audit; get() uses FOR UPDATE, so each bounded
            # transaction protects SQL evidence only and never writes service rows.
            for name in ready['plans']:
                with db.transaction():
                    plan=load_plan(confined(directory,name));result=sql_plan_audit(plan,db)
                    planned.update(r['product_key'] for r in plan['products'])
                    for key in sql_counts:sql_counts[key]+=result[key]
        if planned & blocked or planned | blocked != seen:raise Conflict('FLOW_PUBLICATION_SCOPE_MISMATCH')
        report=dict(status='VERIFIED',scope='FIXED_SNAPSHOT_AND_GOLD_EXPORT',observations=count,
            bronze_raw=raw_count,bronze_event_receipts=event_count,available_image_relations=image_count,
            image_relations=image_total,unavailable_image_relations=image_total-image_count,
            gold_products=len(seen),blocked_products=len(blocked),sql=sql_counts,
            kafka_broker_retention_checked=False)
        # Bronze receipt proves consumption, NOT that Kafka still retains bytes.
        atomic_json(audit_root/'reconciliation.json',report)
        return {**report,'artifacts':[dict(path=str(audit_root/'reconciliation.json'),sha256=sha_file(audit_root/'reconciliation.json'))]}
    finally:
        if db:db.close()
        spark.stop()
