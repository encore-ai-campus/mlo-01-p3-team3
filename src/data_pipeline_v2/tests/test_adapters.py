import copy
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

import test_engine as fixtures
from pipeline_v2.adapters.delta import DeltaLake
from pipeline_v2.adapters.files import confined
from pipeline_v2.adapters.mysql import DirectMySQL, USED, TABLES, PRIMARY, UNIQUE, FKS
from pipeline_v2.adapters.sources import MongoSource
from pipeline_v2.bootstrap import configuration
from pipeline_v2.domain.contracts import Invalid


class AdapterTests(unittest.TestCase):
    def test_mongo_product_allowlist_scopes_discovery_and_rejects_missing_product(self):
        class Cursor:
            def __init__(self, values):self.values=values
            def sort(self,*args):return self
            def batch_size(self,*args):return self
            def close(self):pass
            def __iter__(self):return iter(self.values)
        class Raw:
            def __init__(self):self.distinct_query=None;self.find_query=None;self.available=['p1','p2']
            def distinct(self,field,query):
                self.distinct_query=(field,query)
                allowed=query.get('source_product_id',{}).get('$in',self.available)
                return [p for p in self.available if p in allowed]
            def find(self,query,projection):
                self.find_query=query
                return Cursor([dict(_id='a'*24,source_code='danawa',source_product_id=p,observation_key='obs-'+p,
                    document_hash='hash-'+p,payload={'listings':[],'detail':{'specifications':[]}})
                    for p in self.available if p in query['source_product_id']['$in']])
        class States:
            def find(self,query,projection):
                return [dict(observation_key='obs-p2',image_state_revision=1,image_state_fingerprint='fingerprint')]
        source=object.__new__(MongoSource);source.raw=Raw();source.states=States();source.run='frozen-run'
        source.source_product_ids=['p2']
        refs=list(source.references())
        self.assertEqual([r['product_key'] for r in refs],['danawa:p2'])
        self.assertEqual(source.raw.distinct_query,('source_product_id',{
            'source_code':'danawa','collection_run_id':'frozen-run','source_product_id':{'$in':['p2']}}))
        self.assertEqual(source.raw.find_query,{'source_code':'danawa','source_product_id':{'$in':['p2']}})
        source.raw.available=['p1']
        with self.assertRaisesRegex(Invalid,'SOURCE_PRODUCT_SCOPE_MISMATCH'):
            list(source.references())

    def test_source_release_is_allowlisted_and_detects_tamper(self):
        spec=importlib.util.spec_from_file_location('release_tool',fixtures.HERE/'tools/release.py')
        release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'release.zip'
            result=release.build(path)
            self.assertEqual(result['status'],'SOURCE_RELEASE_VERIFIED')
            with zipfile.ZipFile(path) as archive:
                names=archive.namelist()
                self.assertIn('v2/pipeline_v2/application/coordinator.py',names)
                self.assertIn('v2/pipeline_v2/legacy/silver/normalize.py',names)
                self.assertFalse(any(n.startswith('legacy/product_pipeline/') for n in names))
                self.assertIn('legacy/requirements-bronze.lock.txt',names)
                self.assertFalse(any('/data/' in n or '.local.json' in n or '.env' in n or '__pycache__' in n for n in names))
            with zipfile.ZipFile(path,'a') as archive:archive.writestr('../outside','bad')
            with self.assertRaisesRegex(ValueError,'UNSAFE_PATH'):release.verify(path)

    def test_server_example_refuses_unfilled_placeholders(self):
        with self.assertRaisesRegex(Invalid,'PLACEHOLDERS'):configuration(fixtures.HERE/'config/server.example.json')

    def test_readonly_delta_refuses_writes_without_starting_spark(self):
        lake=DeltaLake(None,'s3a://bucket/direct-v2',SimpleNamespace(check=lambda:None),read_only=True)
        with self.assertRaisesRegex(Invalid,'READ_ONLY'):lake.commit('gold_catalog',[])

    def test_state_path_cannot_escape_root(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(Invalid,'PATH_OUTSIDE'):confined(Path(root),'../outside')

    def test_schema_discovery_is_six_queries_without_review_dependency(self):
        database=object.__new__(DirectMySQL);database.settings={'database':'test'}
        database.connection=SimpleNamespace(rollback=lambda:None)
        columns=[dict(tab=t,name=k,kind=kind,nullable='YES' if nullable else 'NO',**{'def':None},extra='')
            for t in USED for k,(kind,nullable) in TABLES[t].items()]
        indexes=[]
        for table in USED:
            for name,cols in [('PRIMARY',PRIMARY[table]),*((str(i),v) for i,v in enumerate(UNIQUE.get(table,[])))]:
                indexes.extend(dict(tab=table,idx=name,col=col,seq=i+1,non_unique=0,prefix=None) for i,col in enumerate(cols))
        foreign=[dict(tab=t,col=col,parent=parent,pk=pk,parent_schema='test') for t in USED for col,parent,pk in FKS.get(t,[])]
        calls=[]
        def all_rows(sql,args):
            calls.append((sql,args))
            self.assertNotIn('pipeline_publication_guards',args)
            self.assertNotIn('search_index_versions',args)
            if 'information_schema.TABLES' in sql:return [dict(tab=t,engine='InnoDB') for t in USED]
            if 'information_schema.COLUMNS' in sql:return copy.deepcopy(columns)
            if 'information_schema.STATISTICS' in sql:return copy.deepcopy(indexes)
            if 'information_schema.KEY_COLUMN_USAGE' in sql:return copy.deepcopy(foreign)
            if 'information_schema.REFERENTIAL_CONSTRAINTS' in sql:return [dict(tab=t,rule='RESTRICT') for t in USED]
            self.fail(sql)
        database.all=all_rows
        with patch.object(database,'one',return_value=dict(version='8.4.11',ro=0,sro=0)) as server:
            self.assertEqual(database.preflight()['queries'],6)
            self.assertEqual(len(calls)+server.call_count,6)
            columns[0]['kind']='bigint'
            with self.assertRaisesRegex(Invalid,'COLUMN_MISMATCH'):database.preflight()


if __name__=='__main__':unittest.main()
