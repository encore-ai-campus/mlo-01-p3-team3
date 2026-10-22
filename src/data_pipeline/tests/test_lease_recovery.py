import copy
import unittest
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import patch
from product_pipeline import lease_recovery as recovery
from product_pipeline.contracts import digest
from product_pipeline.errors import PipelineError,Conflict


def quiet():return dict(status='QUIESCENT',driver_pids=[],active_applications=[],
    workers=[dict(host='worker',state='ALIVE',coresused=0,memoryused=0)])


class Collection:
    def __init__(self):
        self.docs={};self.now=datetime.now(timezone.utc);self.fail_delete=False;self.unknown_commit=False;self.begin_hook=None
        self.now=self.now.replace(microsecond=self.now.microsecond//1000*1000)
        self.indexes=[{'key':{'_id':1}}]
    def list_indexes(self):return self.indexes
    def find(self,query,session=None):
        return [copy.deepcopy(d) for key,d in self.docs.items() if key in query['_id']['$in']
            and ('$expr' not in query or d['expires_at']<=self.now)]
    def delete_one(self,query,session=None):
        if self.fail_delete and len(self.docs)==1:raise OSError('ambiguous operation')
        row=self.docs.get(query['_id']);match=row and all(row.get(k)==v for k,v in query.items() if k!='$expr') and row['expires_at']<=self.now
        if match:del self.docs[query['_id']]
        return SimpleNamespace(deleted_count=int(bool(match)))
    @contextmanager
    def transaction(self):
        if self.begin_hook:self.begin_hook()
        before=copy.deepcopy(self.docs)
        try:yield None
        except BaseException:self.docs=before;raise
        if self.unknown_commit:raise OSError('reply lost after commit')


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.collection=Collection();self.targets=['s3a://lake/output','s3a://lake/checkpoint']
        self.lease=SimpleNamespace(_targets=self.targets,settings=SimpleNamespace(fingerprint='a'*64,lease_database='ops',lease_collection='leases'),collection=self.collection)
        for target in self.targets:
            key='bronze-path:'+digest(target)
            self.collection.docs[key]=dict(_id=key,token='old-token',target_uri=target,target_fingerprint='a'*64,
                heartbeat_at=self.collection.now-timedelta(minutes=4),expires_at=self.collection.now-timedelta(minutes=2))
        self.audits=[]
        self.patch=patch('product_pipeline.lease_recovery.transaction_scope',side_effect=lambda c:c.transaction());self.patch.start();self.addCleanup(self.patch.stop)
    def inspect(self):return recovery.inspect(self.lease,quiet)
    def apply(self,plan,probe=quiet):return recovery.apply(self.lease,plan,probe,self.audits.append)

    def test_expired_claims_release_atomically_and_repeated_apply_is_noop(self):
        plan=self.inspect();self.assertEqual(self.apply(plan)['released'],2)
        self.assertEqual(self.audits[0]['status'],'PREPARED');self.assertEqual(self.audits[0]['plan'],plan)
        self.assertEqual(self.apply(plan)['status'],'ALREADY_CLEAR');self.assertEqual(self.collection.docs,{})
    def test_running_driver_or_cluster_blocks_recovery(self):
        plan=self.inspect()
        for field,value in [('driver_pids',[123]),('active_applications',['app'])]:
            evidence=quiet();evidence[field]=value
            with self.assertRaises(PipelineError):self.apply(plan,lambda:evidence)
        self.assertEqual(len(self.collection.docs),2)
    def test_unexpired_claim_is_not_recoverable(self):
        next(iter(self.collection.docs.values()))['expires_at']=self.collection.now+timedelta(minutes=1)
        with self.assertRaises(PipelineError):self.inspect()
    def test_replaced_owner_and_renewed_heartbeat_preserve_all_claims(self):
        for field,value in [('token','new-owner'),('heartbeat_at',self.collection.now)]:
            plan=self.inspect();row=next(iter(self.collection.docs.values()));old=row[field];row[field]=value
            with self.assertRaises(Conflict):self.apply(plan)
            self.assertEqual(len(self.collection.docs),2);row[field]=old
    def test_plan_tampering_is_rejected(self):
        plan=self.inspect();plan['claims'][0]['token']='forged'
        with self.assertRaises(Conflict):self.apply(plan)
        self.assertFalse(self.audits)
    def test_race_after_live_check_aborts_transaction(self):
        plan=self.inspect();self.collection.begin_hook=lambda:next(iter(self.collection.docs.values())).update(token='new-owner')
        with self.assertRaises(Conflict):self.apply(plan)
        self.assertEqual(len(self.collection.docs),2)
    def test_partial_failure_rolls_back_and_records_unconfirmed(self):
        plan=self.inspect();self.collection.fail_delete=True
        with self.assertRaises(PipelineError):self.apply(plan)
        self.assertEqual(len(self.collection.docs),2);self.assertEqual(self.audits[-1]['status'],'UNCONFIRMED')
    def test_commit_response_loss_is_reconciled_without_deleting_new_owner(self):
        plan=self.inspect();self.collection.unknown_commit=True
        with self.assertRaises(PipelineError):self.apply(plan)
        self.assertEqual(self.collection.docs,{})
        self.assertEqual(self.apply(plan)['status'],'ALREADY_CLEAR')
    def test_missing_worker_proof_is_rejected(self):
        with self.assertRaises(PipelineError):recovery.inspect(self.lease,lambda:dict(quiet(),workers=[]))
    def test_ttl_cleanup_cannot_substitute_for_recovery(self):
        self.collection.indexes.append(dict(expireAfterSeconds=0))
        with self.assertRaises(PipelineError):self.inspect()
