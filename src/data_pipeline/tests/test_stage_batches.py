import copy
import tempfile
import unittest

from test_silver import sample,reseal,inputs,RUN,AS_OF
from test_silver_runtime import Sink
from test_gold import MemoryGoldSink,side,VERSIONS
from product_pipeline.silver.batch import process_many as silver_batch
from product_pipeline.gold.batch import process_many as gold_batch
from product_pipeline.gold.runner import process as gold_single
from product_pipeline.errors import Conflict,PipelineError


class States:
    def __init__(self,values):self.values=values;self.changed=False
    def get(self,raw_id,key):
        value=copy.deepcopy(self.values[raw_id])
        if self.changed:value['image_state_revision']+=1
        return value


def source(root):
    a,s=sample(root);b=copy.deepcopy(a);t=copy.deepcopy(s)
    b.update(_id='0123456789abcdef01234568',source_product_id='12',source_record_key='danawa:PRODUCT:12',source_url='https://prod.danawa.com/info/?pcode=12')
    b['collector_metadata']['product_id']='12';t['raw_document_id']=b['_id']
    b['payload']['listings'][0]['api_product']['productCode']=12
    b['payload']['listings'][0]['product_url']='https://prod.danawa.com/info/?pcode=12'
    b['payload']['detail']['selected_product']['sku']='12'
    b['payload']['detail']['url']='https://prod.danawa.com/info/?pcode=12'
    reseal(b,t)
    return [a,b],States({a['_id']:s,b['_id']:t})


class StageBatchTests(unittest.TestCase):
    def silver(self,docs,states,sink,**kw):
        return silver_batch(docs,states,sink,inputs(),processing_run_id=RUN,as_of=AS_OF,
            bronze_version=1,bronze_path='file:///bronze',**kw)
    def test_batch_commits_outputs_before_receipts_and_reuses_unchanged_ids(self):
        with tempfile.TemporaryDirectory() as root:
            docs,states=source(root);sink=Sink();out=self.silver(docs,states,sink)
            self.assertEqual(len(out),2);self.assertTrue(all(not x['reused'] for x in out))
            self.assertEqual(len(sink.data['pipeline_evaluations']),2)
            self.assertEqual(sink.version('product_observations'),1)
            self.assertTrue(all(x['reused'] for x in self.silver(docs,states,sink)))
            sink.data['product_images'].pop(next(iter(sink.data['product_images'])))
            with self.assertRaises(Conflict):self.silver(docs,states,sink)
    def test_partial_outputs_or_changed_state_never_authorize_success(self):
        for mode in ('crash','changed'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as root:
                docs,states=source(root);sink=Sink()
                def hook(stage):
                    if stage=='product_images':
                        if mode=='crash':raise RuntimeError('injected')
                        states.changed=True
                with self.assertRaises((RuntimeError,PipelineError)):self.silver(docs,states,sink,hook=hook)
                self.assertFalse(sink.data['pipeline_evaluations'])
                states.changed=False;self.assertEqual(len(self.silver(docs,states,sink)),2)
    def test_gold_batch_matches_single_engine_and_recovers_partial_commits(self):
        with tempfile.TemporaryDirectory() as root:
            docs,states=source(root);silver=Sink();self.silver(docs,states,silver)
            groups=[]
            for receipt in silver.data['pipeline_evaluations'].values():
                groups.append([dict(receipt=receipt,tables={t:[r for r in rows.values() if r['evaluation_key']==receipt['evaluation_key']]
                    for t,rows in silver.data.items() if t!='pipeline_evaluations'})])
            batch=MemoryGoldSink();single=MemoryGoldSink();params=dict(processing_run_id=RUN,as_of=AS_OF)
            for g in groups:gold_single(g,side(),VERSIONS,single,**params)
            def fail(stage):
                if stage=='BEFORE_RECEIPT':raise RuntimeError('injected')
            with self.assertRaises(RuntimeError):gold_batch(groups,side(),VERSIONS,batch,hook=fail,**params)
            self.assertFalse(batch.data['pipeline_evaluations'])
            gold_batch(groups,side(),VERSIONS,batch,**params)
            self.assertTrue(all(x['reused'] for x in gold_batch(groups,side(),VERSIONS,batch,**params)))
            for table in batch.data:
                if table=='pipeline_evaluations':
                    from product_pipeline.files import parse
                    for key,row in batch.data[table].items():
                        expected=single.data[table][key]
                        self.assertEqual(row['input_fingerprint'],expected['input_fingerprint'])
                        self.assertEqual({t:p['records'] for t,p in parse(row['output_manifest_json']).items()},
                            {t:p['records'] for t,p in parse(expected['output_manifest_json']).items()})
                    continue
                self.assertEqual({k:v['record_hash'] for k,v in batch.data[table].items()},
                    {k:v['record_hash'] for k,v in single.data[table].items()})
