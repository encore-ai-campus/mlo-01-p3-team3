import copy
import unittest
from contextlib import contextmanager
from uuid import uuid4
from product_pipeline.registration import validate_request, register, export_row, mapping_snapshot, COLUMNS
from product_pipeline.silver.inputs import registration as silver_registration
from product_pipeline.gold.contracts import registration as gold_registration, INPUT_TABLES
from product_pipeline.errors import PipelineError, Conflict
from product_pipeline.contracts import canonical

def request(stage='NORMALIZATION'):
    scope = dict(bronze_path='file:///isolated/bronze',bronze_version=0,inputs_hash='a'*64)
    if stage=='GOLD_BUILD':
        scope=dict(silver_root='file:///isolated/silver',silver_versions={k:0 for k in INPUT_TABLES},inputs_hash='a'*64)
    return dict(processing_run_id=str(uuid4()),stage=stage,input_range=scope,code_version='b'*64)

class DB:
    settings={'database':'isolated_test'}
    target='test_target'
    def __init__(self): self.rows={};self.tables=['categories'];self.mapping=[];self.calls=[]
    def one(self,sql,args=()):
        if 'TABLES' in sql:return {'engine':'InnoDB'}
        if '@@global' in sql:return {'ro':0,'sro':0}
        return self.rows.get(args[0])
    def all(self,sql,args=()):
        if 'KEY_COLUMN_USAGE' in sql:return [{'name':'id'}]
        if 'COLUMNS' in sql:
            if args[-1]=='category_mappings':
                return [{'name':n} for n in ['source_code','source_category','category_id','mapping_version','active']]
            return [{'name':n,'kind':'char(32)' if n=='id' else 'json' if n=='input_range_json' else 'datetime(3)' if n.endswith('_at') else 'varchar(191)',
                     'nullable':'YES' if n in ['started_at','finished_at'] else 'NO'} for n in COLUMNS]
        if 'TABLES' in sql:return [{'name':n} for n in self.tables]
        return self.mapping
    def execute(self,sql,args):
        self.calls.append(sql)
        key,stage,scope,version,status=args
        self.rows[key]=dict(id=key,type=stage,input_range_json=scope,code_version=version,status=status)
    @contextmanager
    def transaction(self):
        before=copy.deepcopy(self.rows)
        try:yield self
        except BaseException:self.rows=before;raise

class RegistrationTests(unittest.TestCase):
    def test_observation_reuse_scope_is_bound_and_requires_valid_window(self):
        value=request();value['input_range'].update(window_hash='c'*64,reuse_selection_hash='d'*64)
        db=DB();first=register(db,value);self.assertEqual(register(db,value),first)
        changed=copy.deepcopy(value);changed['input_range']['reuse_selection_hash']='e'*64
        with self.assertRaises(Conflict):register(db,changed)
        del changed['input_range']['window_hash']
        with self.assertRaises(PipelineError):validate_request(changed)
        changed=copy.deepcopy(value);changed['input_range']['reuse_selection_hash']='bad'
        with self.assertRaises(PipelineError):validate_request(changed)
    def test_window_hash_is_bound_to_both_stage_registrations(self):
        for stage in ['NORMALIZATION','GOLD_BUILD']:
            r=request(stage);r['input_range']['window_hash']='c'*64
            db=DB();first=register(db,r);self.assertEqual(register(db,r),first)
            bad=copy.deepcopy(r);bad['input_range']['window_hash']='bad'
            with self.assertRaises(PipelineError):validate_request(bad)

    def test_collector_normalized_binding_is_registered_and_unknown_contract_rejected(self):
        db=DB();r=request();r['input_range']['normalized_input']='danawa_products/observation-binding-v1'
        first=register(db,r);self.assertEqual(register(db,r),first)
        self.assertEqual(db.rows[validate_request(r)]['input_range_json'],canonical(r['input_range']))
        bad=copy.deepcopy(r);bad['input_range']['normalized_input']='unverified-other-input'
        with self.assertRaises(PipelineError):validate_request(bad)

    def test_both_stages_export_only_real_registered_rows(self):
        for stage,validator in [('NORMALIZATION',silver_registration),('GOLD_BUILD',gold_registration)]:
            db=DB();r=request(stage)
            out=register(db,r)
            self.assertEqual(validator(out),r['processing_run_id'])
            self.assertEqual(len(db.rows),1)
            self.assertEqual(register(db,r),out)
            self.assertEqual(len(db.calls),1)
    def test_absent_registration_does_not_become_registered(self):
        with self.assertRaisesRegex(PipelineError,'NOT_FOUND'):export_row(DB(),request())
    def test_uuid_reuse_for_other_range_is_conflict(self):
        db=DB();r=request();register(db,r);changed=copy.deepcopy(r)
        changed['input_range']['bronze_version']=1
        with self.assertRaises(Conflict):register(db,changed)
        self.assertEqual(len(db.rows),1)
        self.assertIn('"bronze_version":0',next(iter(db.rows.values()))['input_range_json'])
    def test_failed_or_completed_row_is_not_new_registration(self):
        db=DB();r=request();register(db,r)
        for status in ['FAILED','SUCCEEDED','RUNNING']:
            db.rows[validate_request(r)]['status']=status
            with self.assertRaisesRegex(PipelineError,'NOT_READY'):export_row(db,r)
    def test_missing_approval_is_not_a_registration_dependency(self):
        db=DB();register(db,request())
        self.assertFalse(any('guard' in sql or 'categories' in sql for sql in db.calls))
    def test_unknown_stage_bad_hash_or_negative_version_rejected(self):
        for change in [{'stage':'PUBLISH'},{'code_version':'latest'},{'processing_run_id':'not-uuid'}]:
            r=request();r.update(change)
            with self.assertRaises(PipelineError):validate_request(r)
        r=request();r['input_range']['bronze_version']=-1
        with self.assertRaises(PipelineError):validate_request(r)
    def test_gold_requires_all_fixed_versions(self):
        r=request('GOLD_BUILD');r['input_range']['silver_versions'].pop(INPUT_TABLES[0])
        with self.assertRaises(PipelineError):validate_request(r)
    def test_request_does_not_accept_credentials(self):
        r=request();r['input_range']['bronze_path']='s3a://key:secret@bucket/bronze'
        with self.assertRaises(PipelineError):validate_request(r)
    def test_mapping_table_missing_remains_not_provided(self):
        out=mapping_snapshot(DB(),version='v1',evidence_ref='owner://export')
        self.assertEqual(out['status'],'NOT_PROVIDED')
        self.assertEqual(out['snapshot']['rows'],[])
        self.assertEqual(out['missing_tables'],['category_mappings'])
    def mapping_db(self):
        db=DB();db.tables+=['category_mappings']
        db.mapping=[dict(source_code='danawa',source_category='["known","path"]',category_id=uuid4().hex,
                         mapping_version='v1',active=1,category_code='OWNER_SUPPLIED',category_active=1)]
        return db
    def test_real_mapping_export_keeps_owner_path_and_uuid(self):
        db=self.mapping_db();out=mapping_snapshot(db,version='v1',evidence_ref='owner://export')
        self.assertEqual(out['snapshot']['rows'][0]['source_category_codes'],['known','path'])
        self.assertEqual(out['snapshot']['rows'][0]['service_category_id'].replace('-',''),db.mapping[0]['category_id'])
    def test_ambiguous_scalar_mapping_is_not_guessed(self):
        db=self.mapping_db();db.mapping[0]['source_category']='known/path'
        with self.assertRaisesRegex(PipelineError,'ADAPTER_REQUIRED'):mapping_snapshot(db,version='v1',evidence_ref='owner://export')
    def test_orphan_and_duplicate_mapping_rejected(self):
        db=self.mapping_db();db.mapping[0]['category_code']=None
        with self.assertRaisesRegex(PipelineError,'FK_INVALID'):mapping_snapshot(db,version='v1',evidence_ref='owner://export')
        db=self.mapping_db();db.mapping*=2
        with self.assertRaises(Conflict):mapping_snapshot(db,version='v1',evidence_ref='owner://export')
    def test_empty_real_mapping_is_not_approved_data(self):
        db=self.mapping_db();db.mapping=[]
        self.assertEqual(mapping_snapshot(db,version='v1',evidence_ref='owner://export')['status'],'NOT_PROVIDED')

if __name__=='__main__':unittest.main()
