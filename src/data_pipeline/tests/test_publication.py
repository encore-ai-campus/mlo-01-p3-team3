import copy
from contextlib import contextmanager
from datetime import datetime
from uuid import uuid4
from pathlib import Path
import tempfile
import unittest

from publication_fixture import fixture
from product_pipeline.contracts import canonical,digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.files import parse
from product_pipeline.publication.mysql import schema_difference
from product_pipeline.publication.plan import verify_plan,make_plan
from product_pipeline.publication.runner import guarded_projection,money,publish,audit_publication,attribute_projection
from product_pipeline.publication.schema import TABLES,PRIMARY,UNIQUE,FKS,validate_schema,CONTRACT_HASH,reference_ddl,dispositions


class PublicationTests(unittest.TestCase):
    def test_existing_gold_fixture_builds_canonical_pinned_plan(self):
        with tempfile.TemporaryDirectory() as root:
            plan,guard=fixture(root,'a'*64)
            self.assertEqual(verify_plan(plan),plan)
            changed=copy.deepcopy(plan);changed['products'][0]['guard_hash']='b'*64
            with self.assertRaises(Conflict):verify_plan(changed)
    def test_changed_gold_content_missing_guard_and_unresolved_policy_block(self):
        with tempfile.TemporaryDirectory() as root:
            plan,guard=fixture(root,'a'*64);group=plan['products'][0]['group']
            args=dict(export_hash=plan['export_hash'],as_of=plan['as_of'],target='a'*64,price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400*3)
            with self.assertRaises(PipelineError):make_plan([group],plan['gold_versions'],guard_hashes={},**args)
            args['price_history_mode']='UNRESOLVED'
            with self.assertRaises(PipelineError):make_plan([group],plan['gold_versions'],guard_hashes={},**args)
            group['tables']['product_catalog'][0]['display_name']='tampered'
            args['price_history_mode']='SNAPSHOT_V1'
            with self.assertRaises(Conflict):make_plan([group],plan['gold_versions'],guard_hashes={},**args)
    def test_per_field_old_time_cannot_replace_newer_or_null_clear(self):
        versions={'name':dict(observed_at='2026-10-17T00:00:00.000Z',value_hash=digest('new'))}
        value,proof=guarded_projection({'name':'new'},{'name':'old','brand':None},
             {'name':dict(observed_at='2026-10-16T00:00:00Z')},versions)
        self.assertEqual(value,{});self.assertEqual(proof,versions)
        with self.assertRaises(Conflict):guarded_projection({'name':'new'},{'name':'different'},
             {'name':dict(observed_at='2026-10-17T00:00:00Z')},versions)
        with self.assertRaises(PipelineError):guarded_projection({'brand':'old'},{'brand':'new'},{},{})
    def test_unknown_price_is_not_zero_and_fractional_krw_not_published(self):
        for bad in ('0','-1','0.5','NaN','Infinity'):
            with self.assertRaises(PipelineError):money(bad)
        self.assertEqual(money('12500.0000'),12500)
    def test_schema_requires_uuid_natural_keys_fks_engine_and_no_unmapped_required(self):
        actual={t:dict(engine='InnoDB',columns=dict(cols),primary=PRIMARY[t],unique=UNIQUE.get(t,[]),
                       fks=FKS.get(t,[]),defaulted=[]) for t,cols in TABLES.items()}
        self.assertEqual(validate_schema(actual),CONTRACT_HASH)
        bad=copy.deepcopy(actual);bad['products']['columns']['id']=('bigint',False)
        with self.assertRaises(PipelineError):validate_schema(bad)
        for field in ('unique','fks'):
            bad=copy.deepcopy(actual);bad['products'][field]=[]
            with self.assertRaises(PipelineError):validate_schema(bad)
        bad=copy.deepcopy(actual);bad['products']['columns']['unmapped']=('int',False)
        with self.assertRaises(PipelineError):validate_schema(bad)


class MemoryDB:
    target='a'*64
    def __init__(self):self.rows={t:[] for t in TABLES}
    def preflight(self):return {'status':'fixture'}
    @contextmanager
    def transaction(self):
        before=copy.deepcopy(self.rows)
        try:yield self
        except BaseException:self.rows=before;raise
    def one(self,sql,args=()):
        if sql.startswith('SELECT UTC_TIMESTAMP'):return {'stamp':datetime(2026,10,18)}
        if sql.startswith('SELECT COUNT'):return {'n':self.null_count}
        raise AssertionError(sql)
    def get(self,table,key,**kwargs):
        return next((copy.deepcopy(r) for r in self.rows[table] if all(r[k]==v for k,v in key.items())),None)
    def identity(self,table,key,values):
        old=self.get(table,key)
        if old:return old,False
        row={**values,**key,'id':uuid4().hex}
        for field,(_,nullable) in TABLES[table].items():
            if nullable:row.setdefault(field,None)
            elif field not in row:raise AssertionError('missing NOT NULL '+table+'.'+field)
        self.rows[table].append(copy.deepcopy(row));return row,True
    def update(self,table,key,values):
        for row in self.rows[table]:
            if all(row[k]==v for k,v in key.items()):row.update(copy.deepcopy(values));return
        raise AssertionError('missing row')
    def prepare(self,plan,guard):
        c=plan['products'][0]['group']['tables']['product_catalog'][0]
        from uuid import UUID
        self.rows['categories']=[dict(id=UUID(c['service_category_id']).hex,code=c['service_category_code'],active=True)]
        self.rows['pipeline_publication_guards']=[dict(product_key=c['product_key'],guard_json=canonical(guard))]


class PublicationV2Tests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def published(self,**kwargs):
        plan,guard=fixture(self.root,'a'*64,**kwargs);db=MemoryDB();db.prepare(plan,guard)
        result=publish(plan,db);return plan,db,result
    def test_all_gold_columns_classified_and_unknown_column_blocks(self):
        from unittest.mock import patch
        from product_pipeline.gold.storage import EXTRA
        contract=dispositions();self.assertEqual(len(contract),8)
        with patch.dict(EXTRA,product_catalog=EXTRA['product_catalog']+' unexpected_column'):
            with self.assertRaises(PipelineError):dispositions()
    def test_reference_sql_matches_schema_generator(self):
        sql=Path(__file__).resolve().parents[1]/'sql/publication_reference_v2.sql'
        self.assertEqual(sql.read_text(encoding='utf-8-sig'),reference_ddl())
    def test_extended_fields_links_and_representative_image_are_written(self):
        plan,db,result=self.published(price_amount='1000')
        product=db.rows['products'][0];catalog=plan['products'][0]['group']['tables']['product_catalog'][0]
        self.assertEqual(parse(product['attributes_json']),parse(catalog['attributes_json']))
        self.assertEqual(parse(product['field_status_json']),parse(catalog['field_status_json']))
        self.assertEqual(product['price_krw'],1000)
        self.assertEqual(len(db.rows['product_link_observations']),len(plan['products'][0]['group']['tables']['product_links']))
        image=db.get('product_images',dict(id=product['representative_image_id']))
        self.assertEqual(image['product_id'],product['id']);self.assertFalse(image['active'])
        self.assertTrue(audit_publication(plan,db)['current_products'])
        self.assertEqual(publish(plan,db)['status'],'REUSED')
    def test_service_tamper_is_detected(self):
        plan,db,_=self.published()
        db.rows['products'][0]['manufacturer']='invented'
        with self.assertRaises(Conflict):audit_publication(plan,db)
    def test_later_missing_price_retains_original_snapshot_and_uuid(self):
        first,db,_=self.published(price_amount='1000')
        original=db.rows['products'][0]['id']
        later,guard=fixture(self.root/'later','a'*64,as_of='2026-10-18T00:00:00Z')
        group=later['products'][0]['group']
        catalog=group['tables']['product_catalog'][0]
        catalog['price_krw']=None
        group['tables']['current_offers']=[];group['tables']['offer_history']=[]
        from product_pipeline.gold.contracts import semantic_hash
        for table,rows in group['tables'].items():
            for row in rows:row['record_hash']=semantic_hash(row)
        manifest=parse(group['receipt']['output_manifest_json'])
        for table,rows in group['tables'].items():manifest[table]['records']={r['row_key']:r['record_hash'] for r in rows}
        group['receipt']['output_manifest_json']=canonical(manifest)
        group['receipt']['record_hash']=semantic_hash(group['receipt'])
        later=make_plan([group],later['gold_versions'],export_hash=later['export_hash'],as_of=later['as_of'],
            guard_hashes={catalog['product_key']:digest(guard)},target=db.target,price_history_mode='SNAPSHOT_V1',price_max_age_seconds=259200)
        db.prepare(later,guard);publish(later,db)
        self.assertEqual(db.rows['products'][0]['price_krw'],1000)
        self.assertEqual(db.rows['products'][0]['id'],original)
        self.assertTrue(parse(db.rows['products'][0]['gold_snapshot_ref_json'])['retained_snapshot_refs'])
        self.assertEqual(audit_publication(later,db)['current_products'],1)
    def test_partial_failure_rolls_back_service_rows_not_staging(self):
        plan,guard=fixture(self.root,'a'*64);db=MemoryDB();db.prepare(plan,guard)
        def fail(stage):
            if stage=='PRODUCT_WRITTEN':raise RuntimeError('injected')
        with self.assertRaises(RuntimeError):publish(plan,db,hook=fail)
        self.assertFalse(db.rows['products']);self.assertTrue(db.rows['pipeline_publication_staging'])
        self.assertEqual(publish(plan,db)['status'],'COMPLETED')
    def test_confirmed_attributes_survive_failed_or_older_observations(self):
        old=dict(attribute_name='COLOR',option_key='same',attribute_status='CONFIRMED',value_text='red',observed_at='2026-10-17T00:00:00Z')
        failed={**old,'attribute_status':'FAILED','value_text':None,'observed_at':'2026-10-18T00:00:00Z'}
        self.assertIn(old,attribute_projection([old],[failed]))
        older={**old,'value_text':'blue','observed_at':'2026-10-16T00:00:00Z'}
        self.assertIn(old,attribute_projection([old],[older]))
        with self.assertRaises(Conflict):attribute_projection([old],[{**old,'value_text':'blue'}])
    def test_schema_difference_reports_only_and_never_changes_tables(self):
        class DB:
            target='a'*64
            connection=type('Connection',(),{'rollback':lambda self:None})()
            null_count=1
            one=MemoryDB.one
            def inspect_schema(self):
                actual={t:dict(engine='InnoDB',columns=dict(cols),primary=PRIMARY[t],unique=UNIQUE.get(t,[]),fks=FKS.get(t,[])) for t,cols in TABLES.items()}
                actual['products']['columns']['attributes_json']=('json',True)
                return {'version':'8.4.11'},actual
        result=schema_difference(DB())
        self.assertEqual(result['column_differences'],[dict(table='products',column='attributes_json',expected=('json',False),actual=('json',True))])
        self.assertFalse(result['ddl_applied']);self.assertFalse(result['service_rows_changed'])
    def test_schema_rejects_cascade_and_nullable_required_fields(self):
        actual={t:dict(engine='InnoDB',columns=dict(cols),primary=PRIMARY[t],unique=UNIQUE.get(t,[]),fks=FKS.get(t,[])) for t,cols in TABLES.items()}
        actual['products']['columns']['attributes_json']=('json',True)
        with self.assertRaises(PipelineError):validate_schema(actual)
        actual['products']['columns']['attributes_json']=('json',False)
        actual['products']['fk_delete_rules']=['CASCADE']
        with self.assertRaises(PipelineError):validate_schema(actual)
        ddl=reference_ddl();self.assertGreater(ddl.index('ALTER TABLE `products`'),ddl.index('CREATE TABLE `product_images`'))
