import copy
from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4
from unittest.mock import patch

from product_pipeline import review
from product_pipeline.contracts import canonical,digest
from product_pipeline.errors import PipelineError,Conflict
from product_pipeline.review_cli import main

AT='2026-10-19T00:00:00.000Z'


class Ledger:
    target='a'*64
    settings={'database':'isolated'}
    def __init__(self):self.events=[];self.invalidated=0
    def preflight(self):return {}
    def all(self,sql,args=()):
        if 'information_schema.COLUMNS' in sql:
            return [dict(name=k) for k in ('id','kind','subject_key','revision','review_hash','body_json','actor_ref','actor_db_user','recorded_at')]
        return copy.deepcopy(self.events)
    def one(self,sql,args=()):
        if 'CURRENT_USER' in sql:return dict(actor='review_owner@localhost')
        if 'WHERE id=' in sql:return next((e for e in self.events if e['id']==args[0]),None)
        if 'subject_key=' in sql:
            values=[e for e in self.events if e['kind']==args[0] and e['subject_key']==args[1]]
            return max(values,key=lambda e:e['revision']) if values else None
        raise AssertionError(sql)
    def execute(self,sql,args=()):
        if sql.startswith('UPDATE pipeline_publication_guards'):self.invalidated+=1;return
        if sql.startswith('INSERT INTO pipeline_review_events'):
            self.events.append(dict(zip(('id','kind','subject_key','revision','review_hash','body_json','actor_ref','actor_db_user'),args)));return
        raise AssertionError(sql)
    @contextmanager
    def transaction(self):
        before=copy.deepcopy(self.events);n=self.invalidated
        try:yield
        except BaseException:self.events=before;self.invalidated=n;raise
    def read_snapshot(self):return self.transaction()
    @contextmanager
    def review_lock(self):yield self


def approved(db):
    value=review.draft(db,[dict(kind='POLICY',payload=dict(price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400,representative_price_kinds=[]))])
    value.update(status='REVIEWED',actor_ref='fixture://reviewer',criteria_ref='fixture://criteria',evidence_ref='fixture://decision',
        reviewed_at='2026-10-18T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z')
    value['items'][0]['decision']='APPROVED'
    return review.seal(value)


class ReviewTests(unittest.TestCase):
    def test_gold_only_policy_changes_do_not_invalidate_silver_mapping_inputs(self):
        db=Ledger();category=str(uuid4())
        db.get=lambda *a,**k:dict(code='CHAIR',active=True)
        item=dict(id=str(uuid4()),kind='MAPPING',decision='APPROVED',payload=dict(source_code='danawa',
            source_category_codes=['15','3647'],service_category_id=category,active=True,excluded=False))
        body=dict(item=item,reviewed_at='2026-10-18T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z')
        db.events.append(dict(kind='MAPPING',review_hash=digest(body),body_json=canonical(body)))
        before=review.export_inputs(db,[],as_of=AT)
        policy=approved(Ledger())
        # export_inputs reads sealed event bodies; create a separate valid
        # POLICY decision without editing the mapping event.
        body2=dict(item=policy['items'][0],reviewed_at=policy['reviewed_at'],expires_at=policy['expires_at'])
        db.events.append(dict(kind='POLICY',review_hash=digest(body2),body_json=canonical(body2)))
        after=review.export_inputs(db,[],as_of=AT)
        self.assertEqual(before['silver'],after['silver'])
        self.assertNotEqual(before['gold']['content_hash'],after['gold']['content_hash'])
        body['item']['payload']['active']=False
        db.events[0].update(body_json=canonical(body),review_hash=digest(body))
        self.assertNotEqual(after['silver'],review.export_inputs(db,[],as_of=AT)['silver'])

    def test_gold_uses_explicit_successful_silver_evaluation_not_processing_time(self):
        from test_silver import sample,RUN,AS_OF
        from test_silver_runtime import Sink,States
        from product_pipeline.silver.runner import process
        with tempfile.TemporaryDirectory() as directory:
            raw,state=sample(directory);db=Ledger();side=review.export_inputs(db,[],as_of=AT)['silver'];sink=Sink()
            keys=[]
            for revision in [1,2]:
                current=dict(state,image_state_revision=revision)
                out=process(raw,States(current),sink,side,processing_run_id=RUN,as_of=AS_OF,bronze_version=1,bronze_path='file:///bronze')
                keys.append(out['evaluation_key'])
            groups=[dict(receipt=r,tables={t:[v for v in rows.values() if v['evaluation_key']==r['evaluation_key']]
                    for t,rows in sink.data.items() if t!='pipeline_evaluations'}) for r in sink.data['pipeline_evaluations'].values()]
            before=review.export_inputs(db,groups,as_of=AT)
            self.assertEqual(before['gold']['evaluation_selection']['status'],'NOT_PROVIDED')
            chosen=dict(product_key='danawa:11',raw_document_id=raw['_id'],evaluation_key=keys[1],evidence_ref='silver-completion://fixed-window')
            after=review.export_inputs(db,groups,as_of=AT,evaluation_selection=[chosen])
            self.assertEqual(after['gold']['evaluation_selection']['rows'][0]['evaluation_key'],keys[1])
            with self.assertRaises(Conflict):review.export_inputs(db,groups,as_of=AT,evaluation_selection=[dict(chosen,evaluation_key='missing')])

    def test_draft_cannot_apply_and_expired_or_tampered_decision_is_rejected(self):
        db=Ledger();bundle=approved(db)
        draft=copy.deepcopy(bundle);draft['status']='DRAFT';draft=review.seal(draft)
        with self.assertRaises(PipelineError):review.apply(db,draft,at=AT)
        with self.assertRaises(PipelineError):review.apply(db,bundle,at='2027-01-01T00:00:00Z')
        broken=copy.deepcopy(bundle);broken['items'][0]['payload']['price_max_age_seconds']=10
        with self.assertRaises(PipelineError):review.apply(db,broken,at=AT)
        self.assertFalse(db.events)

    def test_same_review_reuses_receipt_and_changed_revision_conflicts(self):
        db=Ledger();bundle=approved(db)
        self.assertEqual(review.apply(db,bundle,at=AT)['created'],1)
        self.assertEqual(review.apply(db,bundle,at=AT)['reused'],1)
        self.assertEqual(db.invalidated,1)
        changed=copy.deepcopy(bundle);changed['items'][0]['id']=str(uuid4());changed['bundle_id']=str(uuid4());changed=review.seal(changed)
        with self.assertRaises(Conflict):review.apply(db,changed,at=AT)
        self.assertEqual(len(db.events),1)

    def test_guard_audit_does_not_break_export_or_change_owner_input_hash(self):
        db=Ledger();review.apply(db,approved(db),at=AT)
        initial=review.export_inputs(db,[],as_of=AT)
        db.events.append(dict(kind='GUARD',subject_key='danawa:1',revision=1,review_hash='b'*64,body_json=canonical(dict(guard_hash='c'*64))))
        result=review.export_inputs(db,[],as_of=AT)
        self.assertEqual(initial,result)
        self.assertEqual(result['gold']['embeddings']['status'],'NOT_PROVIDED')
        self.assertFalse(result['silver']['reviews']['rows'])

    def test_image_approval_requires_explicit_shared_file_scopes(self):
        db=Ledger();bundle=approved(db);item=bundle['items'][0]
        item.update(kind='IMAGE',payload=dict(image_id=str(uuid4()),source_version='v1',checksum='d'*64,scopes=['DISPLAY'],permission_evidence_ref='fixture://permission',asset_policy={}))
        item['subject_key']=review.subject('IMAGE',item['payload']);bundle=review.seal(bundle)
        with self.assertRaises(PipelineError) as raised:review.validate(bundle,at=AT)
        self.assertEqual(raised.exception.code,'REVIEW_ASSET_PERMISSION_SCOPE_REQUIRED')

    def test_cli_requires_apply_before_opening_any_connection(self):
        with patch('product_pipeline.review_cli.MySQL') as connect:
            self.assertEqual(main(['apply','--config','absent','--bundle','absent']),2)
            connect.assert_not_called()

    def test_explicit_activation_preserves_product_and_image_ids_and_is_idempotent(self):
        from test_publication_pending import Database,CATEGORY
        from publication_fixture import fixture
        from product_pipeline.publication.runner import publish
        from product_pipeline.publication.plan import make_plan
        from product_pipeline.files import parse
        with tempfile.TemporaryDirectory() as root:
            db=Database();base,guard=fixture(root,db.target)
            initial=make_plan([p['group'] for p in base['products']],base['gold_versions'],export_hash=base['export_hash'],as_of=base['as_of'],
                guard_hashes={},target=db.target,price_history_mode='UNRESOLVED',price_max_age_seconds=None,pending_category_id=CATEGORY)
            publish(initial,db)
            original=db.rows['products'][0]['id'];image_ids={i['id'] for i in db.rows['product_images']}
            catalog=base['products'][0]['group']['tables']['product_catalog'][0]
            for asset in db.rows['assets']:asset['status']='AVAILABLE'
            db.rows['categories'].append(dict(id=catalog['service_category_id'].replace('-',''),code=catalog['service_category_code'],active=True,recognition_enabled=True))
            for image in db.rows['product_images']:
                usage=dict(decision='APPROVED',scopes=['DISPLAY','RECOMMENDATION'],evidence_ref='fixture://owner')
                image['usage_review_json']=canonical(usage)
                entry=guard['image_reviews'][image['image_relation_key']]
                entry.update(usage_review=usage,current_review_hash=digest(usage),asset_policy_hash=digest(parse(db.rows['assets'][0]['usage_policy_json'])))
            guard['activation']=dict(from_status='PENDING_REVIEW',product_id=original,
                current_snapshot_hash=digest(parse(db.rows['products'][0]['gold_snapshot_ref_json'])),evidence_ref='fixture://activation',image_relations=list(guard['image_reviews']))
            db.rows['pipeline_publication_guards'].append(dict(product_key=catalog['product_key'],guard_json=canonical(guard)))
            plan=make_plan([p['group'] for p in base['products']],base['gold_versions'],export_hash=base['export_hash'],
                as_of=base['as_of'],guard_hashes={catalog['product_key']:digest(guard)},target=db.target,
                price_history_mode=base['price_history_mode'],price_max_age_seconds=base['price_max_age_seconds'],activate_pending=True)
            publish(plan,db);self.assertEqual(publish(plan,db)['status'],'REUSED')
            self.assertEqual(db.rows['products'][0]['id'],original)
            self.assertEqual(db.rows['products'][0]['status'],'ACTIVE')
            self.assertEqual({i['id'] for i in db.rows['product_images']},image_ids)
            self.assertTrue(all(i['active'] for i in db.rows['product_images']))


if __name__=='__main__':unittest.main()
