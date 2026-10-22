"""Explicit, audited recovery. Expiry alone never authorizes a takeover."""
from contextlib import contextmanager
from datetime import datetime
from .contracts import digest,now,utc
from .errors import PipelineError,Conflict


def identity(lease):
    return dict(fingerprint=lease.settings.fingerprint,database=lease.settings.lease_database,
        collection=lease.settings.lease_collection,targets=sorted(lease._targets))


def claims(lease,*,session=None):
    targets={'bronze-path:'+digest(t):t for t in lease._targets}
    rows=list(lease.collection.find({'_id':{'$in':list(targets)}},session=session))
    result=[]
    for d in rows:
        if (d.get('target_uri')!=targets.get(d['_id']) or d.get('target_fingerprint')!=lease.settings.fingerprint
                or not isinstance(d.get('token'),str) or not d['token']):raise Conflict('LEASE_RECOVERY_TARGET_CHANGED')
        result.append(dict(_id=d['_id'],token=d['token'],target_uri=d['target_uri'],target_fingerprint=d['target_fingerprint'],
            heartbeat_at=utc(d['heartbeat_at'].isoformat()),expires_at=utc(d['expires_at'].isoformat())))
    return sorted(result,key=lambda d:d['_id'])


def assert_no_ttl(lease):
    if any('expireAfterSeconds' in index for index in lease.collection.list_indexes()):
        raise PipelineError('BRONZE_LEASE_TTL_FORBIDDEN')


def assert_quiet(evidence):
    if (not isinstance(evidence,dict) or evidence.get('status')!='QUIESCENT' or evidence.get('driver_pids')!=[]
            or evidence.get('active_applications')!=[] or not evidence.get('workers')
            or any(w.get('state')!='ALIVE' or w.get('coresused')!=0 or w.get('memoryused')!=0 for w in evidence['workers'])):
        raise PipelineError('LEASE_RECOVERY_CLUSTER_NOT_QUIESCENT')


def _expired(lease,rows,*,session=None):
    ids=[d['_id'] for d in rows]
    expired=list(lease.collection.find({'_id':{'$in':ids},'$expr':{'$lte':['$expires_at','$$NOW']}},session=session))
    if {r['_id'] for r in expired}!=set(ids):raise PipelineError('LEASE_RECOVERY_NOT_EXPIRED')


def inspect(lease,probe):
    assert_no_ttl(lease)
    evidence=probe();assert_quiet(evidence)
    rows=claims(lease);_expired(lease,rows)
    body=dict(contract_version=1,target=identity(lease),claims=rows,quiescence=evidence,created_at=now())
    return dict(body,content_hash=digest(body))


@contextmanager
def transaction_scope(collection):
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
    with collection.database.client.start_session() as session:
        with session.start_transaction(read_concern=ReadConcern('snapshot'),
                write_concern=WriteConcern(w='majority',j=True,wtimeout=5000)):
            yield session


def apply(lease,plan,probe,audit):
    """Recheck live evidence; atomically remove only the exact expired claims.

    audit must durably store the protected full plan before the transaction.
    No process launch, data modification or checkpoint deletion occurs here.
    """
    assert_no_ttl(lease)
    if (plan.get('contract_version')!=1 or plan.get('content_hash')!=digest({k:v for k,v in plan.items() if k!='content_hash'})
            or plan.get('target')!=identity(lease)):raise Conflict('LEASE_RECOVERY_PLAN_CHANGED')
    assert_quiet(plan.get('quiescence'));evidence=probe();assert_quiet(evidence)
    current=claims(lease)
    if not current:
        result=dict(status='ALREADY_CLEAR',plan_hash=plan['content_hash'],released=0,at=now());audit(result);return result
    if current!=plan['claims']:raise Conflict('LEASE_RECOVERY_OWNER_CHANGED')
    _expired(lease,current)
    audit(dict(status='PREPARED',plan=plan,live_quiescence=evidence,at=now()))
    try:
        with transaction_scope(lease.collection) as session:
            # A renewal or owner replacement after inspect aborts the whole
            # transaction. A new Driver cannot claim these IDs before commit.
            if claims(lease,session=session)!=current:raise Conflict('LEASE_RECOVERY_OWNER_CHANGED')
            _expired(lease,current,session=session)
            for d in current:
                query=dict(d)
                for key in ('heartbeat_at','expires_at'):query[key]=datetime.fromisoformat(d[key].replace('Z','+00:00'))
                query['$expr']={'$lte':['$expires_at','$$NOW']}
                result=lease.collection.delete_one(query,session=session)
                if result.deleted_count!=1:raise Conflict('LEASE_RECOVERY_OWNER_CHANGED')
    except Exception as exc:
        audit(dict(status='UNCONFIRMED',plan_hash=plan['content_hash'],exception_type=type(exc).__name__,at=now()))
        if isinstance(exc,PipelineError):raise
        raise PipelineError('LEASE_RECOVERY_COMMIT_UNCONFIRMED') from None
    try:
        remaining=claims(lease)
        if any(d['token'] in {r['token'] for r in current} for d in remaining):raise PipelineError('LEASE_RECOVERY_READBACK_UNCONFIRMED')
    except Exception:
        audit(dict(status='READBACK_UNCONFIRMED',plan_hash=plan['content_hash'],at=now()))
        raise PipelineError('LEASE_RECOVERY_READBACK_UNCONFIRMED') from None
    result=dict(status='RELEASED',plan_hash=plan['content_hash'],released=len(current),new_claims_present=bool(remaining),at=now())
    audit(result);return result
