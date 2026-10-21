import importlib.util,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.files import atomic_json,read_json


def module():
    path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_gold_inputs.py'
    spec=importlib.util.spec_from_file_location('gold_preparation_fixture',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


class GoldPreparationTests(unittest.TestCase):
    def test_history_supplement_is_split_registered_separately_and_reuses_success(self):
        from product_pipeline.windows import seal_window
        from product_pipeline.silver.selection import seal
        helper=module()
        class Row(dict):
            def asDict(self):return dict(self)
        rows=[Row(raw_document_id='%024x'%i,observation_key='%064x'%i,source_code='danawa',source_product_id='11') for i in range(1,102)]
        class Frame:
            def filter(self,ids):self.ids=ids;return self
            def select(self,*a):return self
            def limit(self,n):return self
            def collect(self):return [r for r in rows if r['raw_document_id'] in self.ids]
        functions=MagicMock();functions.col.return_value.isin.side_effect=lambda ids:ids
        missing=[dict(product_key='danawa:11',raw_document_id=r['raw_document_id']) for r in rows]
        selection=seal_window(42,None,[rows[0]])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);atomic_json(root/'silver-inputs.json',{'frozen':'owner-inputs'})
            calls=[]
            def run(stage,target):
                calls.append((stage,target));scope=read_json(target/'selection.json')
                choices=[dict(product_key='danawa:11',raw_document_id=r['raw_document_id'],observation_key=r['observation_key'],
                    evaluation_key='silver-eval-v1:'+r['observation_key'],evidence_ref='silver-receipt://silver-eval-v1:'+r['observation_key'])
                    for r in rows if r['observation_key'] in scope['observation_keys']]
                atomic_json(target/'silver-state/evaluation-selection.json',seal(scope,choices))
                atomic_json(target/'silver-result.json',dict(status='COMPLETED'))
            work=SimpleNamespace(ROOT=root,LAKE='s3a://bucket/lake',session=lambda o:object(),release_spark=lambda s:None,run_at=run)
            with patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=functions)}),patch('product_pipeline.silver.source.BronzeSource',return_value=SimpleNamespace(frame=Frame())):
                result=helper._supplement(work,{},selection,missing)
                again=helper._supplement(work,{},selection,missing)
            self.assertEqual(len(result),101);self.assertEqual(again,result)
            self.assertEqual(len(calls),2)
            self.assertEqual(sorted(len(read_json(p/'selection.json')['observation_keys']) for _,p in calls),[1,100])
            self.assertEqual(read_json(root/'silver-inputs.json'),{'frozen':'owner-inputs'})
            self.assertEqual({r['raw_document_id'] for r in result},{r['raw_document_id'] for r in rows})

    def test_registered_legacy_contract_stays_unchanged_and_result_points_at_new_attempt(self):
        helper=module()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name in ('selection.json','silver-inputs.json','silver-state/evaluation-selection.json','gold-inputs.json'):
                atomic_json(root/name,dict(fixture=name))
            before=(root/'gold-inputs.json').read_bytes();calls=[]
            def run(stage,target):
                calls.append((stage,target));return dict(status='COMPLETED')
            work=SimpleNamespace(ROOT=root,run_at=run)
            result=helper.legacy_attempt(work)
            self.assertEqual((root/'gold-inputs.json').read_bytes(),before)
            self.assertEqual(result['gold_state_dir'],str(root/'gold-recovery-v2'))
            self.assertEqual(calls,[('gold',root/'gold-recovery-v2')])
            self.assertEqual(read_json(root/'gold-recovery-v2/selection.json'),dict(fixture='selection.json'))
            atomic_json(root/'gold-recovery-v2/selection.json',{'tampered':True})
            with self.assertRaises(RuntimeError):helper.legacy_attempt(work)

    def test_new_contract_or_prepared_attempt_does_not_nest_recovery_directories(self):
        helper=module()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);work=SimpleNamespace(ROOT=root)
            self.assertIsNone(helper.legacy_attempt(work))
            for name in ('selection.json','gold-inputs.json','gold-preparation.json'):atomic_json(root/name,{})
            self.assertIsNone(helper.legacy_attempt(work))


if __name__=='__main__':unittest.main()
