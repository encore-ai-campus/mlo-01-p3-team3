"""Publication use case: durable staging, then one atomic service transaction.

Legacy helper exports remain import-compatible; implementations live in their
own policy, repository and audit modules.
"""
import time
from ..errors import Conflict
from ..ports import PublicationStore
from .plan import verify_plan
from .repository import PublicationRepository
from .policy import (uuid_hex, money, guarded_projection, snapshot_ref,
                     product_reference, attribute_projection, extension_values)
from .audit import audit_reference, audit_publication


def publish(plan,db: PublicationStore,*,hook=None):
    started=time.monotonic();verify_plan(plan)
    if plan['target']!=db.target:raise Conflict('PUBLICATION_TARGET_MISMATCH')
    db.preflight()
    repository = PublicationRepository(db)
    # Durable staging transaction is separate from the atomic service publication.
    with db.transaction():
        repository.stage(plan)
    if hook:hook('STAGED')
    with db.transaction():
        receipt=repository.receipt(plan)
        if receipt['status']=='COMPLETED':return dict(status='REUSED',publication_id=receipt['id'],plan_id=plan['plan_id'],duration_ms=round((time.monotonic()-started)*1000))
        at=repository.now()
        ids=repository.apply_products(plan,at,hook)
        repository.complete(plan,at)
        if hook:hook('BEFORE_COMMIT')
    return dict(status='COMPLETED',plan_id=plan['plan_id'],publication_id=receipt['id'],products=len(ids),
                product_ids=ids,duration_ms=round((time.monotonic()-started)*1000))
