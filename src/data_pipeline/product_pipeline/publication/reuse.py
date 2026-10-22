"""Reuse a complete publication only after current owner and SQL audits."""
from ..contracts import now
from ..files import parse
from ..errors import Conflict,PipelineError
from ..gold.identity import VERSION
from ..review_repository import verify_read_set
from .plan import make_plan
from .audit import audit_reference,audit_publication
from .values import sql_time


def current_publication(db,group,owner_inputs,*,price_history_mode,price_max_age_seconds):
    if group['receipt'].get('gold_policy_version')!=VERSION:return None
    key=group['receipt']['product_key']
    with db.review_lock(),db.transaction():
        verify_read_set(db,owner_inputs.get('review_read_sets',{}).get(key))
        old=db.get('products',dict(product_key=key))
        if not old or old['status']!='ACTIVE':return None
        ref=parse(old['gold_snapshot_ref_json'])
        if ref['gold_evaluation_key']!=group['receipt']['gold_evaluation_key']:return None
        if 'publication_as_of' not in ref:return None
        prior=audit_reference(db,ref)
        if prior['receipt']['record_hash']!=group['receipt']['record_hash']:raise Conflict('PUBLICATION_REUSE_RECEIPT_CHANGED')
        staged=db.get('pipeline_publication_staging',dict(plan_id=ref['plan_id'],product_key=key))
        item=parse(staged['body_json'])
        from .repository import _check_guard
        try:_check_guard(db,item,group['tables']['product_catalog'][0],sql_time(now()))
        except PipelineError as exc:
            if exc.code in {'PUBLICATION_OWNER_GUARD_CHANGED','PUBLICATION_OWNER_GUARD_INVALID_OR_EXPIRED','PUBLICATION_CATEGORY_CHANGED'}:return None
            raise
        plan=make_plan([prior],ref['gold_versions'],export_hash=ref['export_hash'],as_of=ref['publication_as_of'],
            guard_hashes={key:item['guard_hash']},target=db.target,price_history_mode=price_history_mode,
            price_max_age_seconds=price_max_age_seconds,activate_pending=ref['publication_mode']=='ACTIVATE_REVIEWED')
        if plan['plan_id']!=ref['plan_id']:return None
        audit_publication(plan,db)
        return dict(status='REUSED',plan_id=plan['plan_id'])
