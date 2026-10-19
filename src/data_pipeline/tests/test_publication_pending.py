import copy
from contextlib import contextmanager
from datetime import datetime
import tempfile
import unittest
from uuid import uuid4

from publication_fixture import fixture
from product_pipeline.contracts import canonical
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.publication.plan import make_plan,verify_plan
from product_pipeline.publication.runner import publish,audit_publication
from product_pipeline.publication.schema import TABLES

CATEGORY='25701b75-22f9-5fea-9148-d7a4c006a440'


class Database:
    target='a'*64
    def __init__(self):
        self.rows={t:[] for t in TABLES}
        self.rows['categories']=[dict(id=CATEGORY.replace('-',''),code='PIPELINE_UNMAPPED',active=False,recognition_enabled=False)]
    def preflight(self):return {}
    @contextmanager
    def transaction(self):
        previous=copy.deepcopy(self.rows)
        try:yield
        except BaseException:self.rows=previous;raise
    def one(self,sql):return dict(stamp=datetime(2026,10,19))
    def get(self,table,key,**kwargs):return next((r for r in self.rows[table] if all(r.get(k)==v for k,v in key.items())),None)
    def identity(self,table,key,values):
        old=self.get(table,key)
        if old:return old,False
        row=dict(id=uuid4().hex,**values,**key);self.rows[table].append(row);return row,True
    def update(self,table,key,values):self.get(table,key).update(values)
    def read_snapshot(self):return self.transaction()


def pending(root):
    original,_=fixture(root,'a'*64)
    return make_plan([p['group'] for p in original['products']],original['gold_versions'],
        export_hash=original['export_hash'],as_of=original['as_of'],target=original['target'],guard_hashes={},
        price_history_mode='UNRESOLVED',price_max_age_seconds=None,pending_category_id=CATEGORY)


class PendingTests(unittest.TestCase):
    def test_background_intake_preserves_reviewed_images_and_records_owner_managed_skip(self):
        from product_pipeline.publication.streaming import publish_pending
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database()
            class Source:
                manifest=dict(gold_versions=plan['gold_versions'],content_hash=plan['export_hash'],as_of=plan['as_of'],product_count=1)
                price_history_mode='UNRESOLVED';price_max_age_seconds=None
                def groups(self):yield plan['products'][0]['group']
            output=Path(root)/'stream'
            publish_pending(Source(),db,output,CATEGORY)
            db.rows['product_images'][0]['usage_review_json']=canonical(dict(decision='APPROVED',evidence_ref='fixture://owner'))
            expected=copy.deepcopy(db.rows)
            result=publish_pending(Source(),db,output,CATEGORY)
            self.assertEqual(result['owner_managed'],1)
            self.assertEqual(db.rows,expected)
            self.assertEqual(len(list(output.glob('owner-managed-*.json'))),1)
    def test_streaming_commits_before_next_group_and_reuses_the_same_plan(self):
        from product_pipeline.publication.streaming import publish_pending
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database();seen=[]
            class Source:
                manifest=dict(gold_versions=plan['gold_versions'],content_hash=plan['export_hash'],as_of=plan['as_of'],product_count=1)
                price_history_mode='UNRESOLVED';price_max_age_seconds=None
                def groups(self):yield plan['products'][0]['group']
            output=Path(root)/'stream'
            result=publish_pending(Source(),db,output,CATEGORY,batch_size=1,progress=seen.append)
            self.assertEqual(result['products'],1);self.assertEqual(seen[0]['last_commit_status'],'COMPLETED')
            publish_pending(Source(),db,output,CATEGORY,batch_size=1,progress=seen.append)
            self.assertEqual(seen[-1]['last_commit_status'],'REUSED');self.assertEqual(len(db.rows['products']),1)
    def test_missing_owner_inputs_and_unmapped_gold_can_be_preserved_without_approval(self):
        from test_gold import group,side,VERSIONS,MemoryGoldSink
        from test_silver import RUN,AS_OF
        from product_pipeline.gold.runner import process
        with tempfile.TemporaryDirectory() as root:
            sink=MemoryGoldSink();process([group(root)],side(),VERSIONS,sink,processing_run_id=RUN,as_of=AS_OF)
            gold=dict(receipt=next(iter(sink.data['pipeline_evaluations'].values())),
                tables={t:list(rows.values()) for t,rows in sink.data.items() if t!='pipeline_evaluations'})
            catalog=gold['tables']['product_catalog'][0]
            self.assertFalse(catalog['catalog_ready']);self.assertIsNone(catalog['service_category_id'])
            plan=make_plan([gold],{t:sink.version(t) for t in sink.data},export_hash='c'*64,as_of=AS_OF,
                target='a'*64,guard_hashes={},price_history_mode='UNRESOLVED',price_max_age_seconds=86400,pending_category_id=CATEGORY)
            db=Database();publish(plan,db)
            self.assertEqual(db.rows['products'][0]['category_id'],CATEGORY.replace('-',''))
            self.assertFalse(db.rows['products'][0]['recommendation_ready'])

    def test_explicit_mode_binds_inactive_policy_and_reuses_ids_and_images(self):
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database();verify_plan(plan)
            result=publish(plan,db);again=publish(plan,db)
            self.assertEqual(again['status'],'REUSED')
            self.assertEqual(result['publication_id'],again['publication_id'])
            self.assertEqual(len(db.rows['products']),1)
            product=db.rows['products'][0]
            self.assertEqual(product['status'],'PENDING_REVIEW')
            for k in ('catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready'):
                self.assertFalse(product[k])
            self.assertIsNone(product['price_krw']);self.assertIsNone(product['comparison_url'])
            self.assertFalse(db.rows['offers']);self.assertFalse(db.rows['pipeline_publication_guards'])
            self.assertEqual(len(db.rows['product_images']),2)
            self.assertTrue(all(not r['active'] and not r['gold_recommendation_approved'] for r in db.rows['product_images']))
            self.assertEqual(audit_publication(plan,db)['images'],2)
            db.rows['product_images'][0]['active']=True
            with self.assertRaises(Conflict):audit_publication(plan,db)

    def test_active_category_or_owner_product_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database();db.rows['categories'][0]['active']=True
            with self.assertRaises(PipelineError):publish(plan,db)
            self.assertFalse(db.rows['products'])
            db.rows['categories'][0]['active']=False;publish(plan,db)
            original_id=db.rows['products'][0]['id'];db.rows['products'][0]['status']='ACTIVE'
            changed=copy.deepcopy(plan);changed['export_hash']='b'*64
            from product_pipeline.contracts import digest
            changed['plan_id']=digest({k:v for k,v in changed.items() if k!='plan_id'})
            with self.assertRaises(Conflict):publish(changed,db)
            self.assertEqual(db.rows['products'][0]['id'],original_id)
            self.assertEqual(db.rows['products'][0]['status'],'ACTIVE')

    def test_partial_failure_rolls_back_products_but_preserves_staging(self):
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database()
            def fail(stage):
                if stage=='PRODUCT_WRITTEN':raise RuntimeError('injected')
            with self.assertRaises(RuntimeError):publish(plan,db,hook=fail)
            self.assertFalse(db.rows['products']);self.assertFalse(db.rows['product_images'])
            self.assertEqual(db.rows['pipeline_publications'][0]['status'],'STAGED')
            self.assertEqual(publish(plan,db)['status'],'COMPLETED')

    def test_default_approved_path_and_plan_tamper_remain_blocked(self):
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root)
            changed=copy.deepcopy(plan);changed.pop('pending_category_id')
            with self.assertRaises(Conflict):verify_plan(changed)
            with self.assertRaises(PipelineError):make_plan([plan['products'][0]['group']],plan['gold_versions'],
                export_hash=plan['export_hash'],as_of=plan['as_of'],target=plan['target'],guard_hashes={},
                price_history_mode='UNRESOLVED',price_max_age_seconds=None)
