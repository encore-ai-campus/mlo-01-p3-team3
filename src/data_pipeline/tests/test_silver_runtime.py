import copy
import tempfile
import unittest
from types import SimpleNamespace

from test_silver import sample,inputs,RUN,AS_OF
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.silver.normalize import KEYS,semantic_hash
from product_pipeline.silver.schemas import EXTRA
from product_pipeline.silver.runner import process
from product_pipeline.silver.checkpoint import Checkpoint
from product_pipeline.files import parse
from product_pipeline.contracts import canonical


class States:
    def __init__(self,value):self.value=value;self.reads=0
    def get(self,*args):self.reads+=1;return copy.deepcopy(self.value)


class Sink:
    def __init__(self):self.data={t:{} for t in EXTRA};self.versions={t:[{}] for t in EXTRA}
    def existing(self,table,keys):return {k:v for k,v in self.data[table].items() if k in keys}
    def version(self,table):return len(self.versions[table])-1
    def insert(self,table,rows):
        for row in rows:
            key=row[KEYS[table]]
            if key in self.data[table] and self.data[table][key]['record_hash']!=row['record_hash']:raise Conflict('CONTENT')
            self.data[table].setdefault(key,copy.deepcopy(row))
        self.versions[table].append(copy.deepcopy(self.data[table]))
    def verify(self,table,expected,version):
        for values in (self.data[table],self.versions[table][version]):
            for key,checksum in expected.items():
                if key not in values or values[key]['record_hash']!=checksum or semantic_hash(values[key])!=checksum:
                    raise Conflict('OUTPUT_MISSING_OR_CHANGED')
    def assert_revision(self,raw_id,revision,fingerprint):
        for row in self.data['product_images'].values():
            if row['raw_document_id']==raw_id and row['image_state_revision']==revision and row['image_state_fingerprint']!=fingerprint:
                raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')


class SilverRuntimeTests(unittest.TestCase):
    def execute(self,doc,state,sink,**kw):
        return process(doc,state,sink,inputs(),processing_run_id=kw.get('run_id',RUN),as_of=AS_OF,
                       bronze_version=3,bronze_path='file:///bronze',hook=kw.get('hook'))
    def test_receipt_last_crash_recovery_and_same_evaluation_reuses_first_run(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d);states=States(state);sink=Sink()
            def crash(stage):
                if stage=='BEFORE_RECEIPT':raise RuntimeError('crash')
            with self.assertRaises(RuntimeError):self.execute(doc,states,sink,hook=crash)
            self.assertFalse(sink.data['pipeline_evaluations'])
            original=copy.deepcopy(sink.data['product_images'])
            result=self.execute(doc,states,sink,run_id='0f7c4e4a-e0c1-41cd-a8a8-9c5cdcecd765')
            self.assertFalse(result['reused'])
            self.assertEqual(sink.data['product_images'],original)
            self.assertTrue(self.execute(doc,states,sink)['reused'])
    def test_missing_output_and_forged_hash_are_not_successfully_skipped(self):
        for tamper in ('remove','change'):
            with self.subTest(tamper=tamper),tempfile.TemporaryDirectory() as d:
                doc,state=sample(d);sink=Sink();states=States(state)
                self.execute(doc,states,sink)
                row=next(iter(sink.data['product_images'].values()))
                if tamper=='remove':sink.data['product_images'].clear()
                else:row['source_url']='https://wrong.example/a.png'
                with self.assertRaises(Conflict):self.execute(doc,states,sink)
    def test_input_change_or_database_failure_never_creates_receipt(self):
        for failure in ('change','db'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as d:
                doc,state=sample(d);sink=Sink();states=States(state)
                original=states.get
                def changed(*args):
                    value=original(*args)
                    if states.reads>=2:
                        if failure=='db':raise OSError('database unavailable')
                        value['image_state_revision']+=1
                    return value
                states.get=changed
                with self.assertRaises((PipelineError,OSError)):self.execute(doc,states,sink)
                self.assertFalse(sink.data['pipeline_evaluations'])
    def test_same_revision_different_vector_is_conflict_new_revision_is_new_evaluation(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d);sink=Sink();states=States(state)
            first=self.execute(doc,states,sink)
            states.value['image_state_vector'][0]['availability']='MISSING'
            states.value['image_state_fingerprint']=digest(states.value['image_state_vector'])
            with self.assertRaises(Conflict):self.execute(doc,states,sink)
            states.value['image_state_revision']+=1
            second=self.execute(doc,states,sink)
            self.assertNotEqual(first['evaluation_key'],second['evaluation_key'])
            self.assertEqual(len(sink.data['pipeline_evaluations']),2)


class SilverCheckpointTests(unittest.TestCase):
    def checkpoint(self):
        cp=object.__new__(Checkpoint);cp.settings=SimpleNamespace(fingerprint='target')
        data={}
        class Name(str):
            def getParent(self):return 'inputs'
            def toString(self):return str(self)
        cp._path=Name
        cp.fs=SimpleNamespace(exists=lambda p:p in data,mkdirs=lambda p:None)
        cp._read=lambda p:parse(data[p])
        cp._write=lambda p,v,overwrite:data.__setitem__(p,canonical(v))
        return cp,data

    def test_torn_discovery_restarts_instead_of_skipping_and_complete_starts_new_epoch(self):
        cp,data=self.checkpoint()
        cursor=dict(bronze_version=1,after='a'*64,epoch=0,completed=False)
        cp.save(cursor)
        self.assertEqual(cp.cursor(1)['after'],'a'*64)
        data['discovery.json']=data['discovery.json'].replace('a'*64,'b'*64)
        self.assertIsNone(cp.cursor(1)['after'])
        cursor['completed']=True;cp.save(cursor)
        self.assertEqual(cp.cursor(1)['epoch'],1)

    def test_partial_input_archive_is_repaired_but_valid_conflicting_body_is_preserved(self):
        cp,data=self.checkpoint();value=dict(kind='rules',rows=[])
        key='inputs/'+digest(value)+'.json'
        data[key]='{"kind":'
        self.assertEqual(cp.archive(value),key)
        self.assertEqual(parse(data[key]),value)
        data[key]=canonical(dict(kind='wrong'))
        with self.assertRaises(Conflict):cp.archive(value)
        self.assertEqual(parse(data[key]),dict(kind='wrong'))


if __name__=='__main__':unittest.main()
