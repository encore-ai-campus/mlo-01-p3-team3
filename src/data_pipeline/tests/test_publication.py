import copy
from pathlib import Path
import tempfile
import unittest

from publication_fixture import fixture
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.publication.plan import verify_plan,make_plan
from product_pipeline.publication.runner import guarded_projection,money
from product_pipeline.publication.schema import TABLES,PRIMARY,UNIQUE,FKS,validate_schema,CONTRACT_HASH


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
