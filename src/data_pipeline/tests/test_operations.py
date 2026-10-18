import copy
from datetime import datetime,timedelta,timezone
from pathlib import Path
import tempfile
import unittest

from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.files import atomic_json,read_json
from product_pipeline.operations.queue import Queue,RetryLater
from product_pipeline.operations.migration import pack,verify,restore
from product_pipeline.operations.nifi import Client,processor_plan,TYPE
from product_pipeline.operations.dispatch import resolve_path


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.time=datetime(2026,10,18,tzinfo=timezone.utc)
        self.q=Queue(self.root/'queue','deployment-v1',clock=lambda:self.time.isoformat())
    def tearDown(self):self.tmp.cleanup()
    def test_repeat_enqueue_and_success_reuses_fixed_identity(self):
        one=self.q.enqueue('INGEST',{'snapshot_id':'a'})
        self.assertEqual(one,self.q.enqueue('INGEST',{'snapshot_id':'a'}))
        report=self.q.tick({'INGEST':lambda _:dict(event_id='fixed-event')})
        self.assertEqual(report['succeeded'],1)
        self.assertEqual(self.q.tick({'INGEST':lambda _:self.fail('repeated')})['processed'],0)
    def test_retry_after_and_process_restart_preserve_due_time(self):
        row=self.q.enqueue('INGEST',{'snapshot_id':'a'})
        def fail(_):raise RetryLater(retry_after_seconds=120)
        self.assertEqual(self.q.tick({'INGEST':fail})['retry_wait'],1)
        restarted=Queue(self.root/'queue','deployment-v1',clock=lambda:self.time.isoformat())
        self.assertIsNone(restarted.claim())
        self.time+=timedelta(seconds=121)
        self.assertEqual(restarted.tick({'INGEST':lambda _:None})['succeeded'],1)
    def test_retry_budget_and_dead_letter(self):
        row=self.q.enqueue('INGEST',{},max_attempts=1)
        def fail(_):raise TimeoutError()
        self.assertEqual(self.q.tick({'INGEST':fail})['retry_exhausted'],1)
        self.assertEqual(self.q._read(row['task_id'])['status'],'RETRY_EXHAUSTED')
        self.q.retry_exhausted(row['task_id'],'operator://dependency-repaired',1)
        self.assertEqual(self.q.enqueue('INGEST',{},max_attempts=1)['task_id'],row['task_id'])
        self.assertEqual(self.q.tick({'INGEST':lambda _:None})['recovered_succeeded'],1)
    def test_conflict_never_retries_and_secret_error_text_not_logged(self):
        def fail(_):raise Conflict('INPUT_CONFLICT','password=must-not-appear')
        row=self.q.enqueue('INGEST',{})
        self.assertEqual(self.q.tick({'INGEST':fail})['quarantined'],1)
        content=''.join(p.read_text() for p in (self.root/'queue').rglob('*.json'))
        self.assertNotIn('must-not-appear',content)
    def test_crash_requires_explicit_fenced_recovery(self):
        row=self.q.enqueue('INGEST',{})
        def crash(_):raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):self.q.tick({'INGEST':crash})
        running=self.q._read(row['task_id']);self.time+=timedelta(days=1)
        self.assertIsNone(self.q.claim())
        with self.assertRaises(Conflict):self.q.recover(row['task_id'],'wrong','operator://stopped')
        self.q.recover(row['task_id'],running['attempt_token'],'operator://stopped')
        self.assertEqual(self.q.tick({'INGEST':lambda _:None})['succeeded'],1)
    def test_stale_attempt_cannot_ack_recovered_task(self):
        row=self.q.enqueue('INGEST',{});first=self.q.claim()
        self.q.recover(row['task_id'],first['attempt_token'],'operator://stopped');second=self.q.claim()
        with self.assertRaises(Conflict):self.q.finish(row['task_id'],first['attempt_token'])
        self.q.finish(row['task_id'],second['attempt_token'])
    def test_worker_lock_covers_handler(self):
        def handle(_):
            with self.assertRaises(PipelineError):self.q.tick({})
        self.q.enqueue('INGEST',{});self.assertEqual(self.q.tick({'INGEST':handle})['succeeded'],1)
    def test_input_mutation_and_target_change_rejected(self):
        row=self.q.enqueue('INGEST',{'snapshot_id':'a'});row['spec']['snapshot_id']='b'
        atomic_json(self.q.root/'tasks'/(row['task_id']+'.json'),row)
        with self.assertRaises(Conflict):self.q.claim()
        with self.assertRaises(Conflict):Queue(self.q.root,'different').enqueue('INGEST',{})
    def test_command_or_password_not_accepted(self):
        for spec in ({'password':'x'},{'shell':'x'}):
            with self.assertRaises(PipelineError):self.q.enqueue('INGEST',spec)
    def test_path_binding_changes_resolution_not_immutable_task(self):
        task={'snapshot':'C:/old/snapshot'};before=digest(task)
        self.assertEqual(resolve_path(task['snapshot'],{'C:/old':'/srv/new'}),Path('/srv/new/snapshot'))
        self.assertEqual(digest(task),before)

    def test_throttling_retry_after_is_preserved(self):
        class Throttled(Exception):
            response={'ResponseMetadata':{'HTTPStatusCode':429,'HTTPHeaders':{'retry-after':'300'}}}
        row=self.q.enqueue('RECEIVE',{})
        def fail(_):raise Throttled()
        self.assertEqual(self.q.tick({'RECEIVE':fail})['retry_wait'],1)
        self.time+=timedelta(seconds=299);self.assertIsNone(self.q.claim())
        self.time+=timedelta(seconds=2);self.assertIsNotNone(self.q.claim())

    def test_existing_capture_snapshot_can_be_scheduled_in_file_rehearsal(self):
        from helpers import fixture
        from product_pipeline.snapshot import prepare
        from product_pipeline.config import Settings
        from product_pipeline.operations.dispatch import handlers
        source,media=fixture(self.root)
        manifest=prepare(source,self.root/'snapshot',media)
        settings=Settings('rehearsal','rehearsal','rehearsal-images')
        self.q.enqueue('INGEST',dict(snapshot=str(self.root/'snapshot'),snapshot_id=manifest['snapshot_id'],
              state=str(self.root/'ingest'),target_fingerprint=settings.fingerprint))
        report=self.q.tick(handlers(dict(mode='rehearse'),self.q))
        self.assertEqual(report['succeeded'],1)
        self.assertEqual(len(list((self.root/'ingest/ledger/observations').glob('*.json'))),2)
        self.assertEqual(self.q.tick(handlers(dict(mode='rehearse'),self.q))['processed'],0)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.source=self.root/'source';self.source.mkdir()
        atomic_json(self.source/'registration.json',dict(raw_id='a'*24,event_id='fixed',service_id='existing-uuid'))
        self.proof=dict(status='QUIESCED_AND_BACKED_UP',logical_target='logical-v1',evidence_ref='operator://stopped',
                        services={'mongo':dict(backup_id='backup-v1',identity_digest='a'*64)})
    def tearDown(self):self.tmp.cleanup()
    def bundle(self):return pack({'ingest':self.source},self.root/'bundle',self.proof)
    def test_roundtrip_preserves_bytes_ids_and_is_idempotent(self):
        manifest=self.bundle();proof={**self.proof,'status':'RESTORED_AND_VERIFIED'}
        one=restore(self.root/'bundle',self.root/'restored',proof)
        self.assertEqual((self.source/'registration.json').read_bytes(),(self.root/'restored/ingest/registration.json').read_bytes())
        self.assertEqual(one,restore(self.root/'bundle',self.root/'restored',proof))
        self.assertFalse(one['automatic_start'])
    def test_active_owner_or_running_task_blocks_pack(self):
        atomic_json(self.source/'running.json',dict(status='RUNNING'))
        with self.assertRaises(PipelineError):self.bundle()
    def test_secrets_and_symlinks_block(self):
        (self.source/'cluster.key').write_text('secret')
        with self.assertRaises(PipelineError):self.bundle()
    def test_external_backup_or_target_mismatch_blocks_restore(self):
        self.bundle();wrong={**self.proof,'status':'RESTORED_AND_VERIFIED','logical_target':'other'}
        with self.assertRaises(Conflict):restore(self.root/'bundle',self.root/'restored',wrong)
    def test_checksum_failure_never_creates_ready_destination(self):
        self.bundle();(self.root/'bundle/ingest/registration.json').write_text('{}')
        with self.assertRaises(Conflict):restore(self.root/'bundle',self.root/'restored',{**self.proof,'status':'RESTORED_AND_VERIFIED'})
        self.assertFalse((self.root/'restored').exists())

    def test_held_pipeline_worker_lock_blocks_pack(self):
        from product_pipeline.files import lock
        q=Queue(self.source,'same');q.enqueue('INGEST',{})
        with lock(self.source/'worker.lock'):
            with self.assertRaises(PipelineError):self.bundle()

    def test_completed_ingestion_relocated_keeps_raw_event_and_blob_ids(self):
        from helpers import fixture
        from product_pipeline.snapshot import prepare
        from product_pipeline.config import Settings
        from product_pipeline.local import FileLedger,FileObjects,FilePublisher
        from product_pipeline.runner import ingest
        self.source.rename(self.root/'unused-source')
        export,media=fixture(self.root);snapshot=self.root/'snapshot';prepare(export,snapshot,media)
        settings=Settings('rehearsal','rehearsal','rehearsal-images');state=self.root/'ingest'
        def run(root):return ingest(snapshot,settings,FileLedger(root/'ledger',settings),FileObjects(root/'objects'),FilePublisher(root/'events'),root/'audit',sleep=lambda _:None)
        first=run(state);self.assertGreater(first['raw_created'],0)
        before={p.name:read_json(p)['document']['_id'] for p in (state/'ledger/observations').glob('*.json')}
        pack({'ingest':state},self.root/'bundle',self.proof)
        restore(self.root/'bundle',self.root/'relocated',{**self.proof,'status':'RESTORED_AND_VERIFIED'})
        again=run(self.root/'relocated/ingest')
        self.assertEqual(again['raw_created'],0);self.assertEqual(again['events_sent'],0);self.assertEqual(again['objects_written'],0)
        after={p.name:read_json(p)['document']['_id'] for p in (self.root/'relocated/ingest/ledger/observations').glob('*.json')}
        self.assertEqual(before,after)


class NiFiTests(unittest.TestCase):
    def setUp(self):
        self.spec=processor_plan('/opt/venv/bin/python','/etc/pipeline/ops.json','/opt/project')
        self.calls=[];self.existing=[]
        def request(method,path,body=None):
            self.calls.append((method,path,body))
            if path=='/controller/about':return {'about':{'version':'2.fixture'}}
            if path.startswith('/flow/processor-types'):return {'processorTypes':[dict(type=TYPE,bundle=dict(group='org.apache.nifi',artifact='nifi-standard-nar',version='2.fixture'))]}
            if method=='GET':return {'processors':self.existing}
            return {'id':'created-id'}
        self.client=Client('https://nifi.example/nifi-api',request=request)
        self.group='00000000-0000-4000-8000-000000000001'
    def test_plan_has_no_writes_and_apply_creates_stopped_only(self):
        report=self.client.prepare(self.group,self.spec,expected_version='2.fixture')
        self.assertEqual(report['status'],'PLAN_ONLY');self.assertFalse(any(c[0]=='POST' for c in self.calls))
        out=self.client.prepare(self.group,self.spec,expected_version='2.fixture',apply=True)
        self.assertEqual(out['status'],'CREATED_STOPPED');self.assertFalse(out['automatic_start'])
    def test_version_or_existing_configuration_conflict_blocks(self):
        with self.assertRaises(Conflict):self.client.prepare(self.group,self.spec,expected_version='wrong')
        component=copy.deepcopy(self.spec);component.update(id='id',bundle=dict(group='org.apache.nifi',artifact='nifi-standard-nar',version='2.fixture'))
        self.existing=[dict(component=component)]
        self.assertEqual(self.client.prepare(self.group,self.spec,expected_version='2.fixture')['status'],'EXISTING_MATCH')
        component['config']['properties']['Command']='/bin/sh'
        with self.assertRaises(Conflict):self.client.prepare(self.group,self.spec,expected_version='2.fixture',apply=True)
    def test_no_shell_el_or_multiple_concurrent_workers(self):
        for bad in ('/path;x','/path/${secret}','relative'):
            with self.assertRaises(PipelineError):processor_plan(bad,'/etc/c','/opt/p')
        self.assertEqual(self.spec['config']['concurrentlySchedulableTaskCount'],1)
        self.assertEqual(self.spec['config']['executionNode'],'PRIMARY')

    def test_activation_requires_exact_config_validation_and_revision(self):
        identity='00000000-0000-4000-8000-000000000002';calls=[]
        component={**copy.deepcopy(self.spec),'validationStatus':'VALID'}
        def request(method,path,body=None):
            calls.append((method,path,body))
            if path=='/controller/about':return {'about':{'version':'2.fixture'}}
            if method=='GET':return dict(revision={'version':7},component=component)
            return {}
        client=Client('https://nifi.example/nifi-api',request=request)
        result=client.set_running(identity,self.spec,expected_version='2.fixture',running=True)
        self.assertEqual(result['status'],'RUNNING_REQUESTED')
        self.assertEqual(calls[-1][2],dict(revision={'version':7},state='RUNNING',disconnectedNodeAcknowledged=False))
        component['validationStatus']='INVALID'
        with self.assertRaises(PipelineError):client.set_running(identity,self.spec,expected_version='2.fixture',running=True)


class ReleaseTests(unittest.TestCase):
    def test_release_excludes_data_venv_and_local_secrets_and_detects_tamper(self):
        from product_pipeline.operations.release import build,verify
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'source';source.mkdir();(source/'product_pipeline').mkdir()
            (source/'pyproject.toml').write_text('[project]\nname="fixture"\n')
            (source/'product_pipeline/__init__.py').write_text('')
            (source/'config').mkdir();(source/'config/private.local.json').write_text('{"password":"secret"}')
            (source/'config/production.json').write_text('{"password":"secret"}')
            (source/'products.json').write_text('{"data":"not-source"}')
            (source/'config/ops.example.json').write_text('{}')
            (source/'docs').mkdir();(source/'docs/verify.scala').write_text('// public verification source\n')
            report=build(source,root/'release');self.assertEqual(verify(root/'release')['release_id'],report['release_id'])
            self.assertFalse((root/'release/config/private.local.json').exists())
            self.assertEqual((root/'release/docs/verify.scala').read_text(),'// public verification source\n')
            self.assertFalse((root/'release/config/production.json').exists());self.assertFalse((root/'release/products.json').exists())
            (root/'release/product_pipeline/__init__.py').write_text('changed')
            with self.assertRaises(Conflict):verify(root/'release')


class OperationsCliTests(unittest.TestCase):
    def test_pre_task_failure_is_persisted_without_secret_exception_text(self):
        from unittest.mock import patch
        from product_pipeline.operations.cli import main
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);config=root/'config.json'
            atomic_json(config,dict(contract_version=1,logical_target='test-v1',mode='rehearse',
                                    queue_root=str(root/'queue'),audit_root=str(root/'audit')))
            with patch('product_pipeline.operations.cli.tick',side_effect=RuntimeError('secret-password')):
                with patch('product_pipeline.operations.cli.emit') as emit:
                    self.assertEqual(main(['tick','--config',str(config)]),2)
                    self.assertEqual(emit.call_args.args[0]['error_code'],'RuntimeError')
            logs=list((root/'audit/command_failures').glob('*.json'));self.assertEqual(len(logs),1)
            self.assertNotIn('secret-password',logs[0].read_text())
