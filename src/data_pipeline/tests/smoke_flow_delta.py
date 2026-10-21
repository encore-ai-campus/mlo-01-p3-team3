"""Actual local file ingest→Bronze/Silver/Gold Delta→quality-blocked publication.

Fixture data, local lease double, no Kafka/Mongo/S3/MySQL server connections.
Uses the durable flow engine with explicit test adapters, not production CLI fallback.
"""
import argparse
from pathlib import Path
from unittest.mock import patch

from helpers import fixture
from product_pipeline.config import Settings
from product_pipeline.contracts import canonical,digest,now
from product_pipeline.files import atomic_json,read_json,rows,sha_file
from product_pipeline.snapshot import prepare as snapshot
from product_pipeline.local import FileLedger,FileObjects,FilePublisher
from product_pipeline.runner import ingest
from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.bronze.delta import DeltaSink as BronzeSink
from product_pipeline.bronze.batch import process_batch
from product_pipeline.bronze.contracts import Message
from product_pipeline.silver.config import SilverSettings
from product_pipeline.silver.delta import DeltaSink as SilverSink
from product_pipeline.silver.source import BronzeSource,FileStates
from product_pipeline.silver.inputs import input_snapshot
from product_pipeline.silver.runner import process as silver_process
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.source import SilverSource
from product_pipeline.gold.storage import DeltaSink as GoldSink,Checkpoint
from product_pipeline.gold.runner import process as gold_process
from product_pipeline.gold.export import finalize
from product_pipeline.gold.contracts import INPUT_TABLES
from product_pipeline.publication.source import GoldSource
from product_pipeline.publication.cli import prepare_from_source
from product_pipeline.operations.flow import prepare,run,REQUIRED
from product_pipeline.operations.flow_runtime import spark_session,versions
from product_pipeline.operations.flow_audit import frame_audit,prepared
from smoke_gold_delta import empty_inputs


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=Path);p.add_argument('--spark-options',required=True,type=Path)
    a=p.parse_args();root=a.output.resolve();root.mkdir(parents=True,exist_ok=False)
    source,media=fixture(root,codes=('11',));snapshot(source,root/'snapshot',media)
    files={}
    for name in REQUIRED:
        path=root/(name+'.json');atomic_json(path,{'explicit_fixture':True});files[name]=str(path)
    request=root/'request.json';atomic_json(request,dict(contract_version=1,mode='live',target='flow-local-fixture',
        as_of=now(),snapshot=str(root/'snapshot'),files=files))
    state=root/'flow'
    with patch('product_pipeline.operations.flow_runtime.settings'):prepare(request,state)
    import os
    os.environ['GOLD_SPARK_OPTIONS']=a.spark_options.read_text(encoding='utf-8')
    spark=spark_session();spark.sparkContext.setLogLevel('ERROR')
    class LocalLease:
        def check(self,*args):pass
    lease=LocalLease();settings=Settings('rehearsal','rehearsal','rehearsal-images')
    ledger=FileLedger(root/'ledger',settings);objects=FileObjects(root/'objects');publisher=FilePublisher(root/'events')
    bronze=BronzeSettings('flow-local','rehearsal',*( (root/k).as_uri() for k in ('bronze','events-delta','quarantine','bronze-checkpoint')),
        'fixture-kafka',single_writer_confirmed=True)
    silver=SilverSettings('flow-local','rehearsal',bronze.bronze_path,(root/'silver').as_uri(),(root/'silver-checkpoint').as_uri())
    gold=GoldSettings('flow-local',silver.silver_root,(root/'gold').as_uri(),(root/'gold-checkpoint').as_uri())
    saved=[];calls=[];as_of=read_json(request)['as_of'];registration='5dd50e5a-18b9-4c80-ad7e-05145774a650'
    def execute(context):
        stage=context['stage'];out=context['outputs'];calls.append(stage)
        if stage=='INGEST':
            result=ingest(root/'snapshot',settings,ledger,objects,publisher,root/'ingest-audit')
            assert not result['failed']
            saved.extend(ledger.get(r['observation_key']) for r in rows(root/'snapshot/observations.jsonl'))
            return dict(status='VERIFIED',observations=len(saved))
        if stage=='BRONZE':
            sink=BronzeSink(spark,bronze,lease=lease)
            class Source:
                def get(self,key):return next(r['document'] for r in saved if r['document']['_id']==key)
            messages=[Message(settings.kafka_topic,0,i,r['document']['source_record_key'].encode(),canonical(r['event']).encode()) for i,r in enumerate(saved)]
            result=process_batch(messages,Source(),sink,0);assert result['accepted']==len(saved)
            return dict(status='VERIFIED',versions=versions(spark,sink.paths))
        if stage=='SILVER':
            sink=SilverSink(spark,silver,lease=lease);side={k:input_snapshot(k) for k in ('mappings','reviews','links','rules')}
            states=FileStates({r['document']['_id']:dict(raw_document_id=r['document']['_id'],**{k:r[k] for k in
                ('observation_key','image_state_vector','image_state_revision','image_state_fingerprint','image_state_checked_at')}) for r in saved})
            for doc in BronzeSource(spark,silver,out['BRONZE']['versions']['raw']).page(None):
                silver_process(doc,states,sink,side,processing_run_id=registration,as_of=as_of,
                    bronze_version=out['BRONZE']['versions']['raw'],bronze_path=bronze.bronze_path)
            return dict(status='VERIFIED',versions={t:sink.version(t) for t in INPUT_TABLES})
        if stage=='GOLD':
            sink=GoldSink(spark,gold,lease=lease);cp=Checkpoint(spark,gold,lease);side=empty_inputs()
            reader=SilverSource(spark,gold,out['SILVER']['versions']);selections=[]
            refs=dict(inputs=cp.archive(side),versions=cp.archive(out['SILVER']['versions']),silver_root=gold.silver_root,as_of=as_of)
            for key in reader.products():
                result=gold_process(reader.groups(key),side,out['SILVER']['versions'],sink,
                    processing_run_id=registration,as_of=as_of,input_refs=refs)
                selections.append(dict(product_key=key,evaluation_key=result['evaluation_key']))
            result=finalize(spark,sink,cp,[cp.archive(dict(selections=selections))],as_of=as_of,source_drained=True,input_refs=refs)
            return dict(status='VERIFIED',**result)
        if stage=='PREPARE':
            reader=GoldSource(spark,out['GOLD']['export_manifest_ref'],gold.gold_root,gold.checkpoint_path)
            result=prepare_from_source(reader,{},'a'*64,root/'plans');assert result['blocked']==1 and result['plans']==0
            return dict(status='VERIFIED',directory=str(root/'plans'),**{k:result[k] for k in ('blocked','plans')})
        if stage=='PUBLISH':
            # Correct outcome: no fabricated approvals or service write.
            ready=read_json(root/'plans/ready.json');assert not ready['plans']
            return dict(status='VERIFIED',products=0)
        expected=[dict(raw_document_id=r['document']['_id'],observation_key=r['observation_key'],
            document_hash=r['document']['document_hash'],payload_hash=r['document']['payload_hash'],
            event_id=r['event']['event_id'],event_hash=digest(r['event'])) for r in saved]
        from pyspark.sql import functions as F
        frame=spark.range(1).select(F.explode(F.from_json(F.lit(canonical(expected)),
            'array<struct<raw_document_id:string,observation_key:string,document_hash:string,payload_hash:string,event_id:string,event_hash:string>>')).alias('r')).select('r.*')
        raw=spark.read.format('delta').option('versionAsOf',out['BRONZE']['versions']['raw']).load(bronze.bronze_path)
        events=spark.read.format('delta').option('versionAsOf',out['BRONZE']['versions']['events']).load(bronze.events_path)
        assert frame_audit(frame,raw,'raw_document_id',['observation_key','document_hash','payload_hash'])==1
        assert frame_audit(frame,events,'event_id',['raw_document_id','document_hash','event_hash'])==1
        from product_pipeline.errors import Conflict
        try:frame_audit(frame,raw.withColumn('payload_hash',F.lit('wrong')),'raw_document_id',['payload_hash']);raise AssertionError('bad hash accepted')
        except Conflict:pass
        reader=GoldSource(spark,out['GOLD']['export_manifest_ref'],gold.gold_root,gold.checkpoint_path)
        groups=list(reader.groups());assert len(groups)==1
        refs=__import__('json').loads(groups[0]['tables']['product_catalog'][0]['source_refs_json'])
        assert refs[0]['raw_document_id']==saved[0]['document']['_id']
        ready=read_json(root/'plans/ready.json');assert prepared(root/'plans',reader.manifest['content_hash'],ready['target'])['blocked']==1
        return dict(status='VERIFIED',observations=1,gold_products=1,quality_wait=1,sql_products=0)
    try:
        def fail_once(stage,output):
            if stage=='GOLD':raise RuntimeError('injected after Gold commit, before flow receipt')
        try:run(state,execute,hook=fail_once);raise AssertionError('injection missing')
        except RuntimeError:pass
        calls.clear();result=run(state,execute)
        assert calls==['GOLD','PREPARE','PUBLISH','RECONCILE'] and result['reused_stages']==3
        calls.clear();again=run(state,execute);assert again['reused_stages']==7 and not calls
        report=dict(passed=True,actual_delta=True,spark_version=spark.version,source='synthetic-fixture',
            observations=1,image_relations=2,quality_wait_products=1,mysql_writes=0,
            resumed_after_gold_commit=True,repeated_flow_reused_stages=7,wrong_hash_rejected=True,
            external_service_connections=0,lease_backend='local-test-double',production_worker_cli_tested=False)
        atomic_json(root/'report.json',report);print(canonical(report));return 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
