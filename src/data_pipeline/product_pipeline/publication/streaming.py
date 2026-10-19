"""Publish confirmed bounded Gold groups without waiting to prepare every plan."""
from pathlib import Path
from ..contracts import canonical,digest
from ..errors import Conflict,PipelineError
from ..files import atomic_json
from ..files import parse
from .plan import make_plan,load_plan
from .runner import publish,audit_publication


def publish_pending(source,db,output,category_id,*,batch_size=100,progress=None):
    if not 1<=batch_size<=100:raise PipelineError('PUBLISH_BATCH_LIMIT')
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    pending=[];size=0;seen=set();counts=dict(plans=0,products=0,images=0,owner_managed=0)
    def owner_managed(group):
        catalog=group['tables']['product_catalog'][0]
        old=db.get('products',dict(product_key=catalog['product_key']),locking=False)
        if not old:return False
        if old['status']!='PENDING_REVIEW':return True
        for image in group['tables']['product_images']:
            link=db.get('product_images',dict(product_id=old['id'],source_code=catalog['source_code'],source_product_id=catalog['source_product_id'],
                image_variant=image['image_slot'],raw_document_id=image['raw_document_id'],source_version=image['source_version']),locking=False)
            if link and (link['active'] or parse(link['usage_review_json'])!={'decision':'NOT_REVIEWED'}):return True
        return False
    def commit(groups):
        plan=make_plan(groups,source.manifest['gold_versions'],export_hash=source.manifest['content_hash'],
            as_of=source.manifest['as_of'],guard_hashes={},target=db.target,price_history_mode=source.price_history_mode,
            price_max_age_seconds=source.price_max_age_seconds,pending_category_id=category_id,max_products=batch_size)
        path=output/(plan['plan_id']+'.json')
        if path.exists():
            if load_plan(path)!=plan:raise Conflict('PUBLICATION_STAGING_CHANGED')
        else:atomic_json(path,plan)
        try:
            with db.read_snapshot():managed=any(owner_managed(group) for group in groups)
            if managed:raise Conflict('PUBLICATION_PENDING_OWNER_STATE_CHANGED')
            result=publish(plan,db)
        except Conflict as exc:
            if exc.code not in {'PUBLICATION_PENDING_OWNER_STATE_CHANGED','PUBLICATION_PENDING_IMAGE_REVIEW_CHANGED'}:raise
            if len(groups)>1:
                for group in groups:commit([group])
                return
            key=groups[0]['receipt']['product_key']
            record=dict(status='SKIPPED_OWNER_MANAGED',product_key=key,plan_id=plan['plan_id'],reason=exc.code,export_hash=source.manifest['content_hash'])
            atomic_json(output/('owner-managed-'+digest(key)+'.json'),record)
            counts['owner_managed']+=1
            if progress:progress(dict(**counts,last_commit_status='SKIPPED_OWNER_MANAGED'))
            return
        with db.read_snapshot():audit=audit_publication(plan,db)
        counts['plans']+=1;counts['products']+=len(groups);counts['images']+=audit['images']
        if progress:progress(dict(**counts,last_plan_id=plan['plan_id'],last_commit_status=result['status']))
    def flush():
        if not pending:return
        commit(list(pending))
        pending.clear()
    groups=source.groups_batched(batch_size) if hasattr(source,'groups_batched') else source.groups()
    for group in groups:
        key=group['receipt']['product_key']
        if key in seen:raise Conflict('PUBLISH_EXPORT_DUPLICATE_PRODUCT')
        seen.add(key);amount=len(canonical(group).encode())
        if pending and (len(pending)>=batch_size or size+amount>16*1024*1024):flush();size=0
        pending.append(group);size+=amount
        if len(pending)>=batch_size:flush();size=0
    flush()
    if counts['products']+counts['owner_managed']!=source.manifest['product_count']:raise Conflict('PUBLISH_EXPORT_COUNT_CHANGED')
    result=dict(status='COMPLETED',**counts,export_hash=source.manifest['content_hash'])
    atomic_json(output/'completed.json',result);return result
