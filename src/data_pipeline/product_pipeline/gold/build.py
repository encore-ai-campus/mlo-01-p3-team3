"""Pure per-product Gold reduction: evidence history and conservative current values."""
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from ..contracts import canonical, digest, now, utc
from ..errors import Conflict, PipelineError
from ..files import parse
from ..silver.normalize import KEYS as SILVER_KEYS, semantic_hash as silver_hash
from .contracts import VERSION, TABLES, INPUT_TABLES, active, semantic_hash, validate_inputs, validate_versions


def validate_group(group):
    """Only complete Silver receipt outputs are admitted, including QUALITY_WAIT."""
    receipt, tables = group['receipt'], group['tables']
    if (receipt.get('record_hash') != silver_hash(receipt) or receipt.get('stage') != 'SILVER'
            or receipt.get('status') not in {'OUTPUT_COMMITTED', 'QUALITY_WAIT'}):
        raise Conflict('GOLD_SILVER_RECEIPT_INVALID')
    manifest = parse(receipt['output_manifest_json'])
    if set(manifest) != set(INPUT_TABLES) - {'pipeline_evaluations'} or set(tables) != set(manifest):
        raise Conflict('GOLD_SILVER_MANIFEST_INVALID')
    for table, proof in manifest.items():
        values = tables[table]
        actual = {r[SILVER_KEYS[table]]: r['record_hash'] for r in values}
        if len(actual) != len(values) or actual != proof['records']:
            raise Conflict('GOLD_SILVER_OUTPUT_MISSING')
        for row in values:
            if (row['record_hash'] != silver_hash(row)
                    or any(row.get(k) != receipt.get(k) for k in
                           ('evaluation_key', 'product_key', 'raw_document_id', 'document_hash'))):
                raise Conflict('GOLD_SILVER_CONTENT_CONFLICT')
    if len(tables['product_observations']) != 1:
        raise Conflict('GOLD_SILVER_PRODUCT_GRAIN')


def choose(rows, fields, as_of=None):
    """Equal-time differing values block that timestamp; older confirmed values survive."""
    by_time = defaultdict(list)
    for row in rows:
        if (row.get('observed_at') and row.get('time_status') == 'OBSERVED'
                and (as_of is None or utc(row['observed_at'])<=utc(as_of))):
            by_time[utc(row['observed_at'])].append(row)
    conflicts = []
    for stamp in sorted(by_time, reverse=True):
        candidates = by_time[stamp]
        if len({digest({k:r.get(k) for k in fields}) for r in candidates}) > 1:
            conflicts.append(stamp)
            continue
        return min(candidates, key=lambda r:r['record_hash']), conflicts
    return None, conflicts


def build(groups, inputs, versions, *, processing_run_id, as_of, processed_at=None):
    validate_inputs(inputs); validate_versions(versions); utc(as_of)
    if not groups: raise PipelineError('GOLD_NO_CONFIRMED_SILVER_INPUT')
    for group in groups: validate_group(group)
    product_keys = {g['receipt']['product_key'] for g in groups}
    if len(product_keys) != 1: raise Conflict('GOLD_PRODUCT_GRAIN_MIXED')
    product_key = next(iter(product_keys))
    owner = {k:[r for r in inputs[k]['rows'] if r['product_key'] == product_key]
             for k in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations')}
    # A state re-evaluation has no ordering implied by its hash or processing clock.
    by_raw = defaultdict(list)
    for group in groups: by_raw[group['receipt']['raw_document_id']].append(group)
    selected = []
    for raw_id, candidates in by_raw.items():
        choices = {r.get('evaluation_key') for r in owner['evaluation_selection'] if r.get('raw_document_id') == raw_id}
        if len(choices) > 1: raise Conflict('GOLD_EVALUATION_SELECTION_CONFLICT')
        if choices:
            candidates = [g for g in candidates if g['receipt']['evaluation_key'] in choices]
        if len(candidates) != 1: raise PipelineError('GOLD_EVALUATION_SELECTION_REQUIRED')
        selected += candidates
    selected.sort(key=lambda g:(g['receipt']['raw_document_id'],g['receipt']['evaluation_key']))
    tables = {t:[] for t in TABLES}
    related = dict(policy=inputs['policy'], **owner,
                   input_statuses={k:inputs[k]['status'] for k in owner})
    # Time affects only effective semantic states, never a random execution ID.
    effective_states = [(r, active(r,as_of)) for r in owner['product_states']]
    effective_embeddings = [(r, active(r,as_of)) for r in owner['embeddings']]
    price_expiries = {}
    for group in selected:
        for row in group['tables']['price_observations']:
            if row.get('observed_at'):
                age=(datetime.fromisoformat(utc(as_of).replace('Z','+00:00'))-
                     datetime.fromisoformat(utc(row['observed_at']).replace('Z','+00:00'))).total_seconds()
                price_expiries[row['price_observation_key']] = 0 <= age <= inputs['policy']['price_max_age_seconds']
    link_effective = {r['link_observation_key']:active(r,as_of) for g in selected for r in g['tables']['link_observations']}
    def review_active(row):
        reviews=parse(row['evidence_json']).get('review_rows',[])
        return bool(reviews) and all(r.get('decision')=='APPROVED' and r.get('effective_status')=='ACTIVE'
            and r.get('evidence_ref') and (not r.get('expires_at') or utc(as_of)<utc(r['expires_at']))
            and (not r.get('valid_from') or utc(r['valid_from'])<=utc(as_of)) for r in reviews)
    review_effective={r['image_relation_key']:review_active(r) for g in selected for r in g['tables']['product_images']}
    fingerprint=digest(dict(version=VERSION,product_key=product_key,versions=versions,related=related,metric_date=utc(as_of)[:10],
        silver_receipts=sorted(g['receipt']['record_hash'] for g in selected),
        state_effective=effective_states,embedding_effective=effective_embeddings,
        price_effective=price_expiries,link_effective=link_effective,review_effective=review_effective,
        confirmation_effective=[active(r,as_of) for r in owner['price_confirmations']]))
    gold_key='gold-eval-v1:'+fingerprint
    common=dict(gold_evaluation_key=gold_key,product_key=product_key,gold_policy_version=VERSION,
        processing_run_id=processing_run_id,processed_at=processed_at or now(),
        input_fingerprint=fingerprint,silver_versions_json=canonical(versions))
    reasons=[]
    def add(table, identity, body):
        row=dict(common,row_key='gold-'+table+'-v1:'+digest(dict(evaluation=gold_key,identity=identity)),**body)
        row['record_hash']=semantic_hash(row);tables[table].append(row);return row
    def issue(code, field, evidence=None):
        reasons.append(code)
        add('quality_issues',[code,field,digest(evidence)],dict(issue_code=code,field_name=field,evidence_json=canonical(evidence)))
    products=[g['tables']['product_observations'][0] for g in selected]
    all_attrs=[r for g in selected for r in g['tables']['product_attributes']]
    projection={};lineage={}
    for field in ('display_name','manufacturer','brand','model','width_mm','depth_mm','height_mm'):
        status_name='name' if field=='display_name' else 'dimensions' if field.endswith('_mm') else field
        valid=[r for r in products if r.get(field) is not None and parse(r['field_status_json']).get(status_name)=='CONFIRMED']
        if field.endswith('_mm'):
            from ..silver.values import dimensions
            axis=('width_mm','depth_mm','height_mm').index(field)
            valid=[dict(r,**{field:dimensions(r['value_text'])[axis]}) for r in all_attrs
                if r['attribute_name']=='DIMENSION' and r['attribute_status']=='CONFIRMED'
                and r['measurement_target']=='PRODUCT' and r['option_status']=='NO_OPTION_CONFIRMED'
                and dimensions(r['value_text'])]
        chosen,conflicts=choose(valid,[field],as_of)
        projection[field]=chosen[field] if chosen else None
        lineage[field]=dict(raw_document_id=chosen['raw_document_id'],evaluation_key=chosen['evaluation_key'],
                            observed_at=chosen['observed_at']) if chosen else None
        if field in {'manufacturer','brand','model'}:
            # Silver's common observed_at is the name's source time, not proof of these fields' times.
            values={r[field] for r in valid}
            projection[field]=next(iter(values)) if len(values)==1 else None
            lineage[field]=dict(time_status='FIELD_TIME_UNAVAILABLE',observed_at=None,
                sources=[dict(raw_document_id=r['raw_document_id'],evaluation_key=r['evaluation_key']) for r in valid])
            conflicts=[]
            if len(values)>1:issue('FIELD_TIME_UNVERIFIED',field,sorted(values))
        if isinstance(lineage[field],dict):
            lineage[field]['source_statuses']=[dict(raw_document_id=r['raw_document_id'],
                status=parse(r['field_status_json']).get(status_name)) for r in products]
        if conflicts: issue('TIMESTAMP_VALUE_CONFLICT',field,conflicts)
    mapped=[r for r in products if r.get('mapping_status')=='MAPPED' and r.get('service_category_id')]
    # Category changes without an owner selection remain ambiguous, regardless of arrival order.
    categories={(r['service_category_id'],r['service_category_code']) for r in mapped}
    category=next(iter(categories)) if len(categories)==1 else (None,None)
    if any(r.get('mapping_status')=='EXCLUDED' for r in products):category=(None,None)
    if category[0] is None:issue('CATEGORY_UNRESOLVED','category',sorted(categories))
    mapping_versions={r['mapping_version'] for r in mapped if (r['service_category_id'],r['service_category_code'])==category}
    valid_states=[r for r,ok in effective_states if ok and r.get('service_category_id')==category[0]
                  and r.get('mapping_version') in mapping_versions]
    state_values={(r.get('active'),r.get('supported')) for r in valid_states}
    state_ok=state_values=={(True,True)} and all(type(r.get('active')) is bool and type(r.get('supported')) is bool for r in valid_states)
    if not state_ok:issue('PRODUCT_STATE_UNVERIFIED','product_state',valid_states)
    catalog_ready=bool(projection['display_name'] and category[0] and state_ok)
    # Gallery selection is explicit; file counts, hashes and upload revision are not clocks.
    galleries={g['receipt']['evaluation_key']:g['tables']['product_images'] for g in selected if g['tables']['product_images']}
    gallery_choices={r.get('evaluation_key') for r in owner['gallery_selection']}
    gallery=None
    if len(gallery_choices)==1:
        gallery=galleries.get(next(iter(gallery_choices)))
        if gallery is None:raise Conflict('GOLD_GALLERY_SELECTION_UNKNOWN')
    elif not gallery_choices and len(galleries)==1 and inputs['policy']['bootstrap_single_gallery']:
        gallery=next(iter(galleries.values()))
    elif len(gallery_choices)>1:raise Conflict('GOLD_GALLERY_SELECTION_CONFLICT')
    if gallery is None:issue('GALLERY_ORDER_UNVERIFIED','images',sorted(galleries))
    image_rows=[]
    for r in sorted(gallery or [],key=lambda r:(r['display_order'],r['image_slot'])):
        valid=(r['availability']=='AVAILABLE' and r['validation_status']=='LOCAL_BYTES_VERIFIED'
               and r['review_status']=='APPROVED' and review_effective[r['image_relation_key']])
        image_rows.append(add('product_images',r['image_relation_key'],dict(
            image_relation_key=r['image_relation_key'],raw_document_id=r['raw_document_id'],source_version=r['source_version'],
            image_slot=r['image_slot'],display_order=r['display_order'],source_url=r['source_url'],
            image_checksum=r['image_checksum'],bucket=r['bucket'],object_key=r['object_key'],
            recommendation_approved=valid,review_snapshot_hash=r['review_snapshot_hash'],
            evidence_json=canonical(r))))
    primary=next((r for r in image_rows if r['recommendation_approved']),None)
    if primary is None:issue('IMAGE_RECOMMENDATION_UNREADY','images')
    valid_links=[]
    for r in (r for g in selected for r in g['tables']['link_observations']):
        valid=r['structure_status']=='VALID' and r['link_status']=='VALID' and link_effective[r['link_observation_key']]
        add('product_links',r['link_observation_key'],dict(link_observation_key=r['link_observation_key'],
            link_kind=r['link_kind'],source_url=r['source_url'],offer_key=r['offer_key'],
            usable=valid,raw_document_id=r['raw_document_id'],evidence_json=canonical(r)))
        if valid:valid_links.append(r)
    comparison_rows=[r for r in valid_links if r['link_kind']=='COMPARISON']
    comparison,_=choose(comparison_rows,['source_url'],as_of)
    comparison_ready=comparison is not None
    if not comparison_ready:issue('COMPARISON_LINK_UNREADY','comparison_url')
    offers={r['offer_key']:r for g in selected for r in g['tables']['normalized_offers']}
    eligible=defaultdict(list)
    for group in selected:
        for r in group['tables']['price_observations']:
            condition=parse(r['condition_json'])
            confirmations=[c for c in owner['price_confirmations'] if c.get('source_evidence_key')==r['source_evidence_key']
                and c.get('raw_document_id')==r['raw_document_id'] and active(c,as_of)]
            if len(confirmations)>1:raise Conflict('GOLD_PRICE_CONFIRMATION_CONFLICT')
            basis=r['price_basis']
            if confirmations:
                confirmation=confirmations[0]
                if (confirmation.get('option_key')!=r['option_key'] or confirmation.get('price_kind')!=r['price_kind']
                        or confirmation.get('price_basis')!='REGULAR' or not isinstance(confirmation.get('condition'),dict)):
                    raise Conflict('GOLD_PRICE_CONFIRMATION_MISMATCH')
                condition=confirmation['condition'];basis='REGULAR'
            components=('discount','membership','coupon','delivery','installation','tax','option')
            conditions_ok=(condition.get('condition_schema_version')==1
                and condition.get('unresolved_evidence_key') is None
                and all(condition.get(k,{}).get('state') in {'CONFIRMED','NOT_APPLICABLE'}
                    and (condition[k]['state']=='NOT_APPLICABLE' or condition[k].get('value') is not None) for k in components)
                and condition.get('option',{}).get('state')=='CONFIRMED'
                and isinstance(condition['option'].get('value'),dict)
                and condition['option']['value'].get('option_key')==r['option_key'])
            usable=(r.get('offer_key') in offers and r['numeric_status']=='CONFIRMED'
                    and r['currency']=='KRW' and r['amount'] is not None and Decimal(r['amount'])>0
                    and basis=='REGULAR' and conditions_ok
                    and r['price_kind'] in inputs['policy']['representative_price_kinds']
                    and price_expiries.get(r['price_observation_key'],False))
            effect=dict(offer_key=r['offer_key'],observed_at=r['observed_at'],amount=r['amount'],currency=r['currency'],
                        price_basis=basis,price_kind=r['price_kind'],condition=condition,option_key=r['option_key'])
            effect_key='price-effect-v2:'+digest(effect)
            proof=dict(source_evidence_key=r['source_evidence_key'],price_kind=r['price_kind'],price_basis=r['price_basis'],
                amount=r['amount'],currency=r['currency'],condition_fingerprint=r['condition_fingerprint'],
                observed_at=r['observed_at'],option_evidence_key=r['option_evidence_key'])
            snapshot_key='snapshot-v1:'+digest(dict(offer_key=r['offer_key'],raw_document_id=r['raw_document_id'],fingerprint=digest(proof)))
            if r.get('offer_key'):
                add('offer_history',[r['raw_document_id'],r['price_observation_key']],dict(
                    offer_key=r['offer_key'],snapshot_key=snapshot_key,price_effect_key=effect_key,
                    amount=r['amount'],currency=r['currency'],observed_at=r['observed_at'],
                    raw_document_id=r['raw_document_id'],source_evidence_key=r['source_evidence_key'],
                    eligible=usable,condition_fingerprint=digest(condition),
                    evidence_json=canonical(dict(silver_price=r,confirmation=confirmations,price_effect=effect))))
            if usable:eligible[(r['offer_key'],digest(condition),r['price_kind'])].append(
                dict(r,condition_fingerprint=digest(condition),condition_json=canonical(condition),price_basis=basis))
    current=[]
    for identity, candidates in sorted(eligible.items()):
        chosen,conflicts=choose(candidates,['amount','currency','price_basis','condition_fingerprint'],as_of)
        if conflicts:issue('TIMESTAMP_VALUE_CONFLICT','offer',dict(identity=identity,times=conflicts))
        if chosen:
            current.append(chosen)
            def confirmed_destination(link):
                proofs=parse(link['validation_evidence_json'])
                return bool(proofs) and all(p.get('final_url_confirmed') is True
                    and p.get('option_key')==chosen['option_key'] for p in proofs)
            direct=[r for r in valid_links if r['link_kind']=='DIRECT_PURCHASE' and r['offer_key']==identity[0]
                    and confirmed_destination(r)]
            link,_=choose(direct,['source_url'],as_of)
            add('current_offers',identity,dict(offer_key=identity[0],condition_fingerprint=identity[1],price_kind=identity[2],
                amount=chosen['amount'],currency=chosen['currency'],observed_at=chosen['observed_at'],
                raw_document_id=chosen['raw_document_id'],purchase_url=link['source_url'] if link else None,
                purchase_ready=bool(link),evidence_json=canonical(dict(price=chosen,identity=offers[identity[0]]))))
    # Representative price requires one compatible condition/option, never a cross-condition min.
    compatible={(r['condition_fingerprint'],r['option_key']) for r in current}
    representative=None
    if len(compatible)==1:
        for kind in inputs['policy']['representative_price_kinds']:
            candidates=[r for r in current if r['price_kind']==kind]
            if candidates:
                amounts={r['amount'] for r in candidates}
                if len(amounts)==1: representative=min(candidates,key=lambda r:r['price_observation_key'])
                break
    if representative is None:issue('REPRESENTATIVE_PRICE_UNRESOLVED','price')
    embeddings=[r for r,ok in effective_embeddings if ok and r.get('status')=='READY'
        and r.get('embedding_usage_approved') is True and r.get('index_active') is True
        and primary and r.get('image_relation_key')==primary['image_relation_key']
        and r.get('checksum')==primary['image_checksum'] and r.get('source_version')==primary['source_version']
        and type(r.get('dimensions')) is int and r['dimensions']>0
        and all(r.get(k) for k in ('embedding_id','vector_ref','model_version','preprocessing_version','index_version','silver_confirmed_ref'))]
    embedding=embeddings[0] if len(embeddings)==1 else None
    recommendation_ready=bool(catalog_ready and comparison_ready and primary and embedding)
    if embedding is None:issue('EMBEDDING_INPUT_UNREADY','embedding')
    publication_reasons=['MYSQL_DDL_AND_GUARD_NOT_VERIFIED']
    if inputs['policy']['price_history_mode']=='UNRESOLVED':publication_reasons.append('PRICE_HISTORY_POLICY_UNRESOLVED')
    source_refs=[dict(raw_document_id=g['receipt']['raw_document_id'],evaluation_key=g['receipt']['evaluation_key'],
                      document_hash=g['receipt']['document_hash']) for g in selected]
    attrs=[r for g in selected for r in g['tables']['product_attributes']]
    source_product=products[0]
    add('product_catalog',product_key,dict(**projection,source_code=source_product['source_code'],
        source_product_id=source_product['source_product_id'],
        first_collected_at=min((r['raw_collected_at'] for r in products if r.get('raw_collected_at')),default=None),
        field_status_json=canonical({k:'CONFIRMED' if v is not None else 'UNRESOLVED' for k,v in projection.items()}),
        service_category_id=category[0],service_category_code=category[1],
        price_krw=representative['amount'] if representative else None,comparison_url=comparison['source_url'] if comparison else None,
        catalog_ready=catalog_ready,recommendation_ready=recommendation_ready,comparison_link_ready=comparison_ready,
        purchase_link_ready=any(r['purchase_ready'] for r in tables['current_offers']),
        representative_image_key=primary['image_relation_key'] if primary else None,
        embedding_json=canonical(embedding),attributes_json=canonical(attrs),field_lineage_json=canonical(lineage),
        source_refs_json=canonical(source_refs),quality_reason_codes=sorted(set(reasons)),
        publication_status='BLOCKED',publication_reason_codes=publication_reasons))
    # One per-product contribution. A downstream aggregate must DISTINCT product/observation/file grains.
    applicability={canonical(r.get('applicable_attributes')) for r in valid_states if isinstance(r.get('applicable_attributes'),list)}
    applicable=parse(next(iter(applicability))) if len(applicability)==1 else None
    if applicable is not None and (len(set(applicable))!=len(applicable) or any(k not in projection for k in applicable)):
        raise PipelineError('GOLD_APPLICABILITY_RULE_INVALID')
    add('quality_daily',product_key,dict(metric_date=utc(as_of)[:10],service_category_id=category[0],
        product_count=1,observation_count=len(by_raw),evaluation_count=len(selected),
        image_relation_count=len(image_rows),unique_file_count=len({r['image_checksum'] for r in image_rows if r['image_checksum']}),
        catalog_ready_count=int(catalog_ready),recommendation_ready_count=int(recommendation_ready),
        applicable_attribute_count=len(applicable) if applicable is not None else None,
        confirmed_applicable_attribute_count=sum(projection[k] is not None for k in applicable) if applicable is not None else None,
        applicability_status='CONFIRMED_OWNER_INPUT' if applicable is not None else 'NOT_PROVIDED',
        evidence_json=canonical(dict(raw_ids=sorted(by_raw),checksums=sorted({r['image_checksum'] for r in image_rows if r['image_checksum']}),
            source_quality_issues=[r for g in selected for r in g['tables']['quality_issues']]))))
    return dict(evaluation_key=gold_key,input_fingerprint=fingerprint,tables=tables,
                status='OUTPUT_COMMITTED' if catalog_ready else 'QUALITY_WAIT',reason_codes=sorted(set(reasons)))
