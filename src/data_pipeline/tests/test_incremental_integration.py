"""Real adapter boundaries with in-memory transports; no external DB writes."""
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import copy,tempfile,unittest
from unittest.mock import patch
from product_pipeline.contracts import canonical,digest
from product_pipeline.files import atomic_json,read_json
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.incremental.production import ProductionChanges
from product_pipeline.incremental.runtime import boundary
from product_pipeline.review_repository import read_set,verify_read_set,invalidate,_write,expire_permissions
from product_pipeline.execution import StageResult
from product_pipeline.operations.server_flow import decision

AT='2026-10-22T00:00:00.000Z'


class ReviewScopeTests(unittest.TestCase):
    def observation(self):return dict(source_code='danawa',source_category_path_json=canonical([dict(codes=['1','2'])]))

    def test_absent_mapping_is_a_dependency_and_unrelated_product_is_excluded(self):
        body=dict(item=dict(payload=dict(product_key='danawa:other')))
        events={('PRODUCT','danawa:other'):dict(body_json=canonical(body),review_hash='other')}
        reads=read_set(SimpleNamespace(),'danawa:1',[self.observation()],events)
        subjects={(r['kind'],r['subject_key']) for r in reads}
        self.assertIn(('MAPPING','danawa:'+digest(['1','2'])),subjects)
        self.assertNotIn(('PRODUCT','danawa:other'),subjects)
        self.assertTrue(all(r['review_hash'] is None for r in reads))

    def test_late_mapping_insertion_invalidates_a_prepared_absent_read(self):
        db=SimpleNamespace(one=lambda *args:dict(review_hash='new'))
        with self.assertRaises(Conflict):verify_read_set(db,[dict(kind='MAPPING',subject_key='path',review_hash=None)])

    def test_new_guards_are_invalidated_by_dependency_not_unconditional_update(self):
        statements=[];db=SimpleNamespace(execute=lambda sql,args:statements.append((sql,args)))
        item=dict(kind='PRODUCT',subject_key='danawa:1')
        invalidate(db,[(item,None,None,None)],'bundle')
        sql,args=statements[0]
        self.assertIn(' WHERE ',sql);self.assertIn('JSON_CONTAINS_PATH',sql)
        self.assertEqual(len(args),2)

    def test_ordinary_product_approval_retains_existing_service_state(self):
        statements=[];db=SimpleNamespace(get=lambda *a,**k:dict(active=True),execute=lambda *a:statements.append(a))
        item=dict(kind='PRODUCT',decision='APPROVED',payload=dict(raw_document_ids=['a'*24],service_category_id='b'*32,active=True,supported=True))
        state=dict(source_refs_json=[dict(raw_document_id='a'*24)],id='id')
        _write(db,item,dict(reviewed_at=AT),state)
        self.assertEqual(statements,[])
        item['decision']='REVOKED';_write(db,item,dict(reviewed_at=AT),state)
        self.assertIn("status='INACTIVE'",statements[0][0])
        self.assertNotIn('DELETE',statements[0][0])

    def test_expiry_removes_access_without_creating_approval_events(self):
        statements=[]
        event=dict(kind='PRODUCT',subject_key='danawa:1',body_json=canonical(dict(expires_at='2026-10-21T00:00:00Z')))
        db=SimpleNamespace(all=lambda sql,args:[] if sql.startswith('SELECT i.id') else [event],
            get=lambda *a:dict(status='ACTIVE',id='id'),execute=lambda sql,args:statements.append(sql))
        self.assertEqual(expire_permissions(db,['danawa:1'],AT)['expired'],1)
        self.assertTrue(any("status='INACTIVE'" in sql for sql in statements))
        self.assertFalse(any('INSERT' in sql for sql in statements))

    def test_lease_diagnostics_survive_structured_stage_errors(self):
        diagnostic=dict(phase='renew',error_code='NETWORK')
        with self.assertRaises(PipelineError) as raised:
            StageResult(2,dict(error_code='BRONZE_WRITER_LEASE_LOST',lease_failure=diagnostic,lease_owner_ref='owner.json')).verified()
        self.assertEqual(raised.exception.lease_failure,diagnostic)
        self.assertEqual(raised.exception.lease_owner_ref,'owner.json')


class ProductionChangeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def row(self,key='1',revision=1):
        return dict(observation_key=digest(key),image_state_revision=revision,image_state_fingerprint=digest(revision),incremental_change=dict(reason='IMAGE_STATE'),
            document=dict(_id=digest(key)[:24],source_code='danawa',source_product_id=key,document_hash=digest(key),
                payload=dict(listings=[dict(api_product=dict(category=dict(codes=['1','2'])))])))

    def source(self,rows,events):
        collection=SimpleNamespace(find=lambda *a:copy.deepcopy(rows))
        db=SimpleNamespace(read_snapshot=nullcontext,all=lambda sql,args=():copy.deepcopy(events) if 'pipeline_review_events' in sql else [])
        source=ProductionChanges(collection,db,self.root);source._reviews();return source

    def test_projection_never_requests_raw_html_or_image_bytes(self):
        from product_pipeline.incremental.production import PROJECTION
        self.assertNotIn('document.payload',PROJECTION)
        self.assertNotIn('document.image_refs',PROJECTION)
        source=self.source([self.row()],[])
        snapshot=next(source.read_products())
        self.assertIn('MAPPING:danawa:'+digest(['1','2']),snapshot['dependencies'])

    def test_late_review_commit_is_discovered_without_uuid_or_timestamp_cursor(self):
        event=dict(kind='POLICY',subject_key='publication',review_hash=digest(1),
            body_json=canonical(dict(item=dict(payload={}),reviewed_at='2026-10-01T00:00:00Z',expires_at='2027-01-01T00:00:00Z')))
        source=self.source([self.row()],[event]);source.discovery_completed()
        self.assertEqual(source._reviews(),set())
        event['review_hash']=digest(2)
        source.db.all=lambda sql,args=():[event] if 'pipeline_review_events' in sql else []
        self.assertEqual(source._reviews(),{'POLICY:publication'})

    def test_mongo_ack_is_conditional_on_exact_captured_revision(self):
        calls=[];source=self.source([self.row()],[])
        source.collection.update_one=lambda where,update:calls.append((where,update))
        value=next(source.read_products());source.acknowledge([dict(snapshot=value)])
        self.assertEqual(calls[0][0]['image_state_revision'],1)
        self.assertEqual(calls[0][0]['image_state_fingerprint'],digest(1))
        self.assertEqual(calls[0][1],{'$unset':{'incremental_change':''}})

    def test_master_pause_prevents_maintenance_even_after_full_ingest_completed(self):
        atomic_json(self.root/'incremental-operations.json',{})
        atomic_json(self.root/'flow-progress.json',dict(status='COMPLETED'))
        self.assertEqual(decision(self.root),'MAINTENANCE_READY')
        atomic_json(self.root/'pause-after-window.json',{})
        self.assertEqual(decision(self.root),'PAUSED')
        with patch('product_pipeline.operations.service.tick') as tick:
            self.assertEqual(boundary(self.root,at=AT)['status'],'PAUSED');tick.assert_not_called()

    def test_maintenance_failure_needs_explicit_recovery(self):
        atomic_json(self.root/'flow-progress.json',dict(status='COMPLETED'))
        atomic_json(self.root/'incremental-last-tick.json',dict(status='FAILED'))
        self.assertEqual(decision(self.root),'INCREMENTAL_RECOVERY_REQUIRED')

    def test_existing_nifi_boundary_uses_shared_application_tick(self):
        atomic_json(self.root/'incremental-operations.json',{})
        with patch('product_pipeline.operations.service.tick',return_value=dict(status='TICK_COMPLETED')) as tick:
            self.assertEqual(boundary(self.root,at=AT)['status'],'TICK_COMPLETED')
            self.assertEqual(boundary(self.root,at=AT)['status'],'NOT_DUE');self.assertEqual(tick.call_count,1)

    def test_unconfirmed_or_quarantined_work_stops_the_boundary(self):
        atomic_json(self.root/'incremental-operations.json',{})
        with patch('product_pipeline.operations.service.tick',return_value=dict(status='ATTENTION_REQUIRED',queue_counts={'RUNNING':1})):
            with self.assertRaises(PipelineError) as raised:boundary(self.root,at=AT)
        self.assertEqual(raised.exception.code,'INCREMENTAL_ATTENTION_REQUIRED')
        self.assertEqual(read_json(self.root/'incremental-last-tick.json')['queue_counts'],{'RUNNING':1})


class PublicationReuseTests(unittest.TestCase):
    def test_identical_gold_and_verified_service_projection_need_no_republication(self):
        from test_publication import MemoryDB
        from publication_fixture import fixture
        from product_pipeline.gold.identity import MODE
        from product_pipeline.publication.runner import publish
        from product_pipeline.publication.reuse import current_publication
        with tempfile.TemporaryDirectory() as root:
            plan,guard=fixture(root,'a'*64,evaluation_identity=MODE)
            db=MemoryDB();db.prepare(plan,guard);db.review_lock=nullcontext
            publish(plan,db);before=copy.deepcopy(db.rows)
            with patch('product_pipeline.publication.reuse.verify_read_set',return_value={}):
                result=current_publication(db,plan['products'][0]['group'],{},price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400*3)
                self.assertEqual(result,dict(status='REUSED',plan_id=plan['plan_id']))
                self.assertEqual(before,db.rows)
                db.rows['products'][0]['manufacturer']='tampered'
                with self.assertRaises(Conflict):current_publication(db,plan['products'][0]['group'],{},price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400*3)


if __name__=='__main__':unittest.main()
