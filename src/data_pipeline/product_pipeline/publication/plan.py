"""Immutable, bounded publication plans from complete fixed-version Gold groups."""
from pathlib import Path
from uuid import UUID

from ..contracts import canonical, digest, utc
from ..errors import Conflict, PipelineError
from ..files import atomic_json, parse, read_json
from ..gold.contracts import TABLES, semantic_hash
from .schema import CONTRACT_HASH, validate_columns


def validate_group(group,versions):
    validate_columns(group)
    receipt=group['receipt'];tables=group['tables']
    if receipt.get('record_hash')!=semantic_hash(receipt) or receipt.get('status') not in {'OUTPUT_COMMITTED','QUALITY_WAIT'}:
        raise Conflict('PUBLISH_GOLD_RECEIPT_INVALID')
    proofs=parse(receipt['output_manifest_json'])
    if set(proofs)!=set(TABLES) or set(tables)!=set(TABLES):raise Conflict('PUBLISH_GOLD_TABLES_INVALID')
    for table,proof in proofs.items():
        rows=tables[table]
        if type(proof['version']) is not int or not 0<=proof['version']<=versions[table]:raise Conflict('PUBLISH_GOLD_VERSION_INVALID')
        if len({r['row_key'] for r in rows})!=len(rows) or {r['row_key']:r['record_hash'] for r in rows}!=proof['records']:
            raise Conflict('PUBLISH_GOLD_OUTPUT_INCOMPLETE')
        for row in rows:
            if row['record_hash']!=semantic_hash(row) or row['gold_evaluation_key']!=receipt['gold_evaluation_key'] or row['product_key']!=receipt['product_key']:
                raise Conflict('PUBLISH_GOLD_ROW_CHANGED')
    if len(tables['product_catalog'])!=1:raise Conflict('PUBLISH_PRODUCT_GRAIN_INVALID')


def make_plan(groups, versions, *, export_hash, as_of, guard_hashes, target, price_history_mode, price_max_age_seconds, max_products=100, pending_category_id=None, activate_pending=False):
    utc(as_of)
    pending=pending_category_id is not None
    if type(activate_pending) is not bool or (activate_pending and pending):raise PipelineError('PUBLISH_MODE_INVALID')
    if pending:
        try:pending_category_id=str(UUID(pending_category_id))
        except (ValueError,TypeError) as exc:raise PipelineError('PUBLISH_PENDING_CATEGORY_UUID_REQUIRED') from exc
    if not pending:
        if price_history_mode not in {'SNAPSHOT_V1','PRICE_EFFECT_V2'}:raise PipelineError('PUBLISH_HISTORY_POLICY_UNRESOLVED')
        if type(price_max_age_seconds) is not int or price_max_age_seconds<=0:raise PipelineError('PUBLISH_PRICE_FRESHNESS_POLICY_REQUIRED')
    if set(versions)!=set(TABLES)|{'pipeline_evaluations'} or any(type(v) is not int or v<0 for v in versions.values()):
        raise PipelineError('PUBLISH_FIXED_GOLD_VERSIONS_REQUIRED')
    if not groups or not 1<=len(groups)<=max_products or max_products>1000:raise PipelineError('PUBLISH_BATCH_LIMIT')
    products=[];seen=set()
    for group in groups:
        validate_group(group,versions)
        catalog=group['tables']['product_catalog'][0];key=catalog['product_key']
        if key in seen:raise Conflict('PUBLISH_DUPLICATE_PRODUCT')
        seen.add(key)
        if not pending:
            if catalog['catalog_ready'] is not True:raise PipelineError('PUBLISH_CATALOG_QUALITY_WAIT')
            try:UUID(catalog['service_category_id'])
            except (ValueError,TypeError) as exc:raise PipelineError('PUBLISH_CATEGORY_UUID_REQUIRED') from exc
            if key not in guard_hashes or len(guard_hashes[key])!=64:raise PipelineError('PUBLISH_OWNER_GUARD_REQUIRED')
        products.append(dict(product_key=key,guard_hash=None if pending else guard_hashes[key],group=group))
    if not isinstance(target,str) or len(target)!=64 or not isinstance(export_hash,str) or len(export_hash)!=64:
        raise PipelineError('PUBLISH_TARGET_AND_EXPORT_HASH_REQUIRED')
    body=dict(contract_version=2,stage='MYSQL_PUBLICATION_PLAN',schema_contract_hash=CONTRACT_HASH,target=target,price_history_mode=price_history_mode,
              price_max_age_seconds=price_max_age_seconds,
              export_hash=export_hash,gold_versions=versions,as_of=utc(as_of),products=sorted(products,key=lambda r:r['product_key']))
    if pending:body.update(publication_mode='PENDING_REVIEW',pending_category_id=pending_category_id)
    if activate_pending:body['publication_mode']='ACTIVATE_REVIEWED'
    if len(canonical(body).encode())>32*1024*1024:raise PipelineError('PUBLISH_PLAN_TOO_LARGE')
    body['plan_id']=digest(body);return body


def verify_plan(plan):
    if (plan.get('stage')!='MYSQL_PUBLICATION_PLAN' or plan.get('schema_contract_hash')!=CONTRACT_HASH or
            plan.get('plan_id')!=digest({k:v for k,v in plan.items() if k!='plan_id'})):
        raise Conflict('PUBLISH_PLAN_CHANGED')
    rebuilt=make_plan([r['group'] for r in plan['products']],plan['gold_versions'],export_hash=plan['export_hash'],
                      as_of=plan['as_of'],guard_hashes={r['product_key']:r['guard_hash'] for r in plan['products']},
                      target=plan['target'],price_history_mode=plan['price_history_mode'],
                      price_max_age_seconds=plan['price_max_age_seconds'],max_products=1000,
                      pending_category_id=plan.get('pending_category_id'),activate_pending=plan.get('publication_mode')=='ACTIVATE_REVIEWED')
    if rebuilt!=plan:raise Conflict('PUBLISH_PLAN_NONCANONICAL')
    return plan


def load_plan(path):return verify_plan(read_json(path))
