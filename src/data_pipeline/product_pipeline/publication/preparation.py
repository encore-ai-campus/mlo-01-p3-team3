"""Prepare immutable publication plans and publish ready metadata last."""
from pathlib import Path
import tempfile

from ..contracts import digest
from ..errors import PipelineError
from ..files import atomic_json
from .plan import make_plan


def prepare_from_source(source,guards,target,output,*,batch_size=100,pending_category_id=None):
    if type(batch_size) is not int or not 1<=batch_size<=1000:raise PipelineError('PUBLISH_BATCH_LIMIT')
    output=Path(output).resolve()
    if output.exists():raise PipelineError('PUBLISH_OUTPUT_EXISTS')
    output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.publication-',dir=output.parent) as temporary:
        build=Path(temporary)/'plans';build.mkdir();pending=[];blocked=[];proofs={}
        def flush():
            if not pending:return
            plan=make_plan(pending,source.manifest['gold_versions'],export_hash=source.manifest['content_hash'],
                as_of=source.manifest['as_of'],guard_hashes=guards,target=target,price_history_mode=source.price_history_mode,
                price_max_age_seconds=source.price_max_age_seconds,max_products=batch_size,pending_category_id=pending_category_id)
            name=plan['plan_id']+'.json';atomic_json(build/name,plan);proofs[name]=digest(plan);pending.clear()
        for group in source.groups():
            key=group['receipt']['product_key']
            try:
                make_plan([group],source.manifest['gold_versions'],export_hash=source.manifest['content_hash'],
                    as_of=source.manifest['as_of'],guard_hashes=guards,target=target,price_history_mode=source.price_history_mode,
                    price_max_age_seconds=source.price_max_age_seconds,pending_category_id=pending_category_id)
            except PipelineError as exc:
                if exc.code not in {'PUBLISH_CATALOG_QUALITY_WAIT','PUBLISH_OWNER_GUARD_REQUIRED','PUBLISH_HISTORY_POLICY_UNRESOLVED'}:raise
                blocked.append(dict(product_key=key,reason=exc.code));continue
            pending.append(group)
            if len(pending)==batch_size:flush()
        flush()
        manifest=dict(contract_version=1,stage='PUBLICATION_PLANS_READY',export_hash=source.manifest['content_hash'],
                      target=target,plans=proofs,blocked=blocked)
        manifest['content_hash']=digest(manifest);atomic_json(build/'ready.json',manifest)
        build.rename(output)
    return dict(status='PLANS_PREPARED',plans=len(proofs),blocked=len(blocked),target=target)
