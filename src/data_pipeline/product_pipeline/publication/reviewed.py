"""Pipeline-managed activation of owner-reviewed, pinned Gold products.

Does not create PRODUCT/IMAGE/MAPPING/POLICY decisions. Guard registration and
activation use the separate reviewer identity and existing immutable decisions.
"""
from pathlib import Path
from ..contracts import digest, now
from ..errors import Conflict, PipelineError
from ..files import atomic_json
from ..review import approve_guard
from .plan import make_plan, load_plan
from .runner import publish
from .audit import audit_publication

WAIT_REASONS = {'REVIEW_APPROVAL_NOT_PROVIDED', 'REVIEW_APPROVAL_MISSING_OR_EXPIRED',
                'REVIEW_GOLD_REEVALUATION_REQUIRED', 'REVIEW_IMAGE_LINK_NOT_PRESERVED'}


def publish_reviewed(source, db, output, *, progress=None):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    inputs=source.read(source.manifest['input_refs']['inputs'])
    counts=dict(activated=0,waiting=0)
    seen=set();waiting_keys=[]
    groups=source.groups_batched(100) if hasattr(source,'groups_batched') else source.groups()
    for group in groups:
        catalog=group['tables']['product_catalog'][0];key=catalog['product_key']
        if key in seen:raise Conflict('PUBLISH_EXPORT_DUPLICATE_PRODUCT')
        seen.add(key)
        identity=digest(dict(product_key=key,export_hash=source.manifest['content_hash'],target=db.target))
        path=output/(identity+'.json')
        reason=None
        if not catalog['catalog_ready']:
            reason='GOLD_CATALOG_QUALITY_WAIT'
        else:
            from .reuse import current_publication
            reused=current_publication(db,group,inputs,price_history_mode=source.price_history_mode,
                price_max_age_seconds=source.price_max_age_seconds)
            if reused:
                counts['activated']+=1
                record=dict(status='ACTIVATION_CONFIRMED',product_key=key,published=reused,reused=True)
                atomic_json(output/('receipt-'+identity+'.json'),record)
                if progress:progress(record)
                continue
        if reason is None and path.exists():
            plan=load_plan(path)
            if (plan['export_hash']!=source.manifest['content_hash'] or plan['target']!=db.target or
                    plan['products'][0]['group']!=group or plan.get('publication_mode')!='ACTIVATE_REVIEWED'):
                raise Conflict('REVIEW_ACTIVATION_PLAN_CHANGED')
        elif reason is None:
            try:
                guard=approve_guard(db,group,source.manifest['gold_versions'],
                    price_history_mode=source.price_history_mode,as_of=now(),owner_inputs=inputs)
            except PipelineError as exc:
                if isinstance(exc,Conflict) or exc.code not in WAIT_REASONS:raise
                reason=exc.code
            if reason is None:
                plan=make_plan([group],source.manifest['gold_versions'],export_hash=source.manifest['content_hash'],
                    as_of=source.manifest['as_of'],guard_hashes={key:guard['guard_hash']},target=db.target,
                    price_history_mode=source.price_history_mode,price_max_age_seconds=source.price_max_age_seconds,
                    activate_pending=True)
                atomic_json(path,plan)
        if reason:
            record=dict(status='WAITING_REVIEW',product_key=key,reason=reason,export_hash=source.manifest['content_hash'])
            atomic_json(output/('waiting-'+identity+'.json'),record)
            counts['waiting']+=1
            waiting_keys.append(key)
        else:
            result=publish(plan,db)
            with db.read_snapshot():audit=audit_publication(plan,db)
            record=dict(status='ACTIVATION_CONFIRMED',product_key=key,plan_id=plan['plan_id'],published=result,audit=audit)
            atomic_json(output/('receipt-'+identity+'.json'),record)
            counts['activated']+=1
        if progress:progress(record)
    if len(seen)!=source.manifest['product_count']:raise Conflict('PUBLISH_EXPORT_COUNT_CHANGED')
    result=dict(status='COMPLETED',**counts,export_hash=source.manifest['content_hash'],waiting_product_keys=sorted(waiting_keys))
    atomic_json(output/'completed.json',result)
    return result
