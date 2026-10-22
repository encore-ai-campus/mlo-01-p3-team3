import tempfile,unittest,copy,os
from unittest.mock import patch
from types import SimpleNamespace
from pathlib import Path
from product_pipeline.operations.automatic_recovery import eligible,checked_owner,check_plan_owner,quota
from product_pipeline.errors import PipelineError,Conflict
from product_pipeline.files import atomic_json


def failure():
    return dict(status='RECOVERY_REQUIRED',error_code='BRONZE_WRITER_LEASE_LOST',last_failure=dict(
        lease_failure=dict(phase='renew',reason='DATABASE_OPERATION_FAILED',exception_type='_OperationCancelled')))


class AutomaticRecoveryTests(unittest.TestCase):
    def test_only_transient_renewal_failures_are_eligible(self):
        self.assertTrue(eligible(failure()))
        for key,value in [('phase','acquire'),('reason','OWNER_OR_EXPIRY_MISMATCH'),('exception_type','ValueError')]:
            f=failure();f['last_failure']['lease_failure'][key]=value;self.assertFalse(eligible(f))
        f=failure();f['error_code']='GOLD_INPUT_CHANGED';self.assertFalse(eligible(f))

    def test_foreign_or_partial_owners_are_never_deleted(self):
        owner=dict(token='owned',claimed=['one','two'],target=dict(fingerprint='f'))
        plan=dict(target=owner['target'],claims=[dict(_id=k,token='owned') for k in owner['claimed']])
        check_plan_owner(plan,owner)
        for altered in [dict(plan,target={}),dict(plan,claims=[dict(_id='one',token='other')]),dict(plan,claims=plan['claims'][:1])]:
            with self.assertRaises(Conflict):check_plan_owner(altered,owner)
        check_plan_owner(dict(plan,claims=[]),owner)

    def test_majority_timeout_requires_exact_server_code_and_renewal(self):
        f=failure();d=f['last_failure']['lease_failure']
        d.update(exception_type='WTimeoutError',server_code=64)
        self.assertTrue(eligible(f))
        d['server_code']=None;self.assertFalse(eligible(f))
        d.update(server_code=64,phase='acquire');self.assertFalse(eligible(f))

    def test_spark_error_cannot_hide_renewal_loss_or_trigger_plain_retry(self):
        from product_pipeline.windows import checked_stage
        for original,expected in [('Py4JJavaError','BRONZE_WRITER_LEASE_LOST'),
                                  ('GOLD_SHUTDOWN_UNCONFIRMED','GOLD_SHUTDOWN_UNCONFIRMED')]:
            with tempfile.TemporaryDirectory() as d:
                root=Path(d);(root/'runs').mkdir()
                def run(args):
                    atomic_json(root/'runs'/'failed.json',dict(error_code=original,
                        lease_failure=dict(phase='renew',exception_type='WTimeoutError',server_code=64),
                        lease_owner_ref='private-owner'))
                    return 2
                with self.assertRaises(PipelineError) as caught:checked_stage(run,[],root,'gold')
                self.assertEqual(caught.exception.code,expected)
                self.assertEqual(caught.exception.lease_owner_ref,'private-owner')

    def test_budget_does_not_reset_for_a_new_failure_in_same_unpublished_window(self):
        state=dict(anchor='commit-one',attempts=3)
        with self.assertRaises(PipelineError):quota(state,'commit-one',3,'2026-10-21T00:00:00.000Z')
        self.assertEqual(quota(state,'commit-two',3,'2026-10-21T00:00:00.000Z')['attempts'],0)

    def test_cooldown_blocks_immediate_retries(self):
        with self.assertRaises(PipelineError):quota(dict(anchor='a',attempts=1,next_allowed_at='2026-10-21T00:01:00.000Z'),'a',3,'2026-10-21T00:00:00.000Z')

    def test_private_owner_evidence_is_required_and_must_stay_inside_root(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p=root/'owner.json';atomic_json(p,dict(contract_version=1,token='t',claimed=['k']));p.chmod(0o600)
            self.assertEqual(checked_owner(root,str(p))['token'],'t')
            with self.assertRaises(PipelineError):checked_owner(root,None)
            with self.assertRaises(PipelineError):checked_owner(root,str(root.parent/'outside.json'))

    def test_lease_records_owner_separately_from_public_diagnostics(self):
        from test_bronze_lease import Collection,settings
        from product_pipeline.bronze.lease import MongoLease
        lease=MongoLease(settings(),'unused',collection=Collection());lease.acquire()
        with tempfile.TemporaryDirectory() as d:
            ref=lease.record_owner(Path(d)/'owners'/'execution.json','execution')
            private=checked_owner(Path(d),ref)
            self.assertEqual(private['token'],lease.token)
            self.assertEqual(private['claimed'],sorted(lease._claimed))
        lease.close(release=True)

    def scenario(self,root):
        from test_lease_recovery import RecoveryTests,quiet
        from product_pipeline.lease_recovery import identity
        fixture=RecoveryTests('test_expired_claims_release_atomically_and_repeated_apply_is_noop');fixture.setUp();self.addCleanup(fixture.doCleanups)
        lease=fixture.lease;lease.settings.lease_uri_env='OPS_URI';lease.close=lambda **kw:None
        cut=root/'epochs/000001/windows/000000';cut.mkdir(parents=True)
        ref=cut/'gold-state/lease-owners/execution.json';ref.parent.mkdir(parents=True)
        atomic_json(ref,dict(contract_version=1,pid=999999999,token='old-token',claimed=sorted(fixture.collection.docs),target=identity(lease)));ref.chmod(0o600)
        f=failure();f.update(epoch=1,recovery_stage='windows',last_mysql_commit_at='commit-one')
        f['last_failure'].update(stage='gold',execution_root=str(cut),lease_owner_ref=str(ref))
        atomic_json(root/'flow-progress.json',f)
        atomic_json(root/'auto-recovery-policy.json',dict(contract_version=1,enabled=True,max_attempts=3,cooldown_seconds=60))
        modules={'production_work':SimpleNamespace(setup=lambda:None),'recover_writer_lease':SimpleNamespace(settings_for=lambda *args:lease.settings)}
        self.enterContext(patch.dict('sys.modules',modules));self.enterContext(patch.dict(os.environ,{'OPS_URI':'unused'}))
        self.enterContext(patch('product_pipeline.bronze.lease.MongoLease',return_value=lease))
        return fixture,quiet

    def test_full_automatic_recovery_preserves_failed_cursor_and_cooldown(self):
        from product_pipeline.operations.automatic_recovery import recover
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);fixture,quiet=self.scenario(root);before=(root/'flow-progress.json').read_bytes()
            result=recover(root,quiet)
            self.assertEqual(result['status'],'RECOVERED');self.assertEqual(result['released'],2)
            self.assertEqual((root/'flow-progress.json').read_bytes(),before)
            with self.assertRaises(PipelineError):recover(root,quiet)

    def test_unknown_release_commit_can_reconcile_without_another_delete(self):
        from product_pipeline.operations.automatic_recovery import recover
        from product_pipeline.files import read_json
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);fixture,quiet=self.scenario(root);fixture.collection.unknown_commit=True
            with self.assertRaises(PipelineError):recover(root,quiet)
            self.assertEqual(fixture.collection.docs,{})
            p=root/'auto-recovery/state.json';state=read_json(p);state['next_allowed_at']='2000-01-01T00:00:00.000Z';atomic_json(p,state)
            result=recover(root,quiet)
            self.assertEqual(result['released'],0);self.assertEqual(result['attempt'],2)

    def test_busy_cluster_never_releases_claims_or_uses_retry_budget(self):
        from product_pipeline.operations.automatic_recovery import recover
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);fixture,quiet=self.scenario(root)
            with self.assertRaises(PipelineError):recover(root,lambda:{**quiet(),'driver_pids':[123]})
            self.assertEqual(len(fixture.collection.docs),2)
            self.assertFalse((root/'auto-recovery/state.json').exists())

    def test_live_original_pid_is_rejected_even_without_spark_jobs(self):
        from product_pipeline.operations.automatic_recovery import assert_owner_dead
        with tempfile.TemporaryDirectory() as d:
            proc=Path(d);(proc/'42').mkdir()
            with self.assertRaises(PipelineError):assert_owner_dead(dict(pid=42),proc)


if __name__=='__main__':unittest.main()
