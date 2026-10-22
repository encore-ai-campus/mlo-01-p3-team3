"""Isolated real Mongo/Delta trial; never advance production Kafka/checkpoints."""
import argparse,importlib.util,json,os,sys,time
from pathlib import Path
from dataclasses import replace
from uuid import uuid4


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deployment-root',type=Path,required=True)
    parser.add_argument('--legacy-delta',type=Path,required=True)
    parser.add_argument('--count',type=int,default=12)
    args=parser.parse_args()
    if not 1<=args.count<=20:parser.error('isolated trial count must be 1..20')
    root=args.deployment_root.resolve();sys.path[:0]=[str(root),str(root/'source')]
    from product_pipeline.files import read_json,atomic_json,parse,sha_file
    from product_pipeline.contracts import canonical,now
    from product_pipeline.bronze.config import BronzeSettings
    from product_pipeline.bronze.lease import MongoLease
    from product_pipeline.bronze.mongo import MongoSource
    from product_pipeline.bronze.delta import DeltaSink,same_content
    from product_pipeline.bronze.batch import process_batch
    from product_pipeline.bronze.contracts import Message
    from product_pipeline.bronze.metrics import Profile,timed
    from product_pipeline.spark_session import SessionScope,release_spark
    from delta.tables import DeltaTable
    from production_work import setup,session
    _,options=setup()
    original=BronzeSettings.load(root/'epochs/000006/bronze.json')
    name=uuid4().hex
    prefix=original.bronze_path.rsplit('/',1)[0]+'/verification/bronze-optimization-'+name
    def settings(label):
        return replace(original,environment_id='isolated-optimization',checkpoint_recovery=None,
            bronze_path=prefix+'/'+label+'/products',events_path=prefix+'/'+label+'/events',
            quarantine_path=prefix+'/'+label+'/quarantine',checkpoint_path=prefix+'/'+label+'/checkpoint')
    rows=[]
    with (root/'kafka-recovery-20261021/events.jsonl').open() as stream:
        for i,line in enumerate(stream):
            if i>=args.count:break
            saved=parse(line);rows.append(Message(original.kafka_topic,0,i,saved['key'].encode(),canonical(saved['event']).encode()))
    assert len(rows)==args.count
    source=MongoSource(original,os.environ[original.mongo_uri_env])
    spec=importlib.util.spec_from_file_location('product_pipeline.bronze.isolated_legacy_delta',args.legacy_delta)
    legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy)
    report=dict(status='FAILED',at=now(),trial_prefix=prefix,count=args.count,
                production_checkpoint_written=False,production_raw_written=False,
                kafka_published=False,legacy_sha256=sha_file(args.legacy_delta),runs=[])
    output=root/'perf-20261020/bronze-optimization-trial.json'
    proofs={}
    try:
        with SessionScope() as runtime:
            for label,sink_class in [('merge_baseline',legacy.DeltaSink),('append_optimized',DeltaSink)]:
                s=settings(label);lease=MongoLease(s,os.environ[s.lease_uri_env]);spark=None;quiet=False
                lease.acquire()
                try:
                    # A real latched writer selects the S3 log store, with
                    # unchanged Driver/Executor identity across both trials.
                    from product_pipeline.spark_storage import storage_options,bind_spark_credentials
                    from product_pipeline.spark_session import get_or_create
                    from pyspark.sql import SparkSession
                    opts=storage_options(options,writer=True,lease=lease)
                    builder=SparkSession.builder.appName('bronze-optimization-isolated')
                    for k,v in opts.items():builder=builder.config(k,v)
                    builder=builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                    spark=get_or_create(builder,opts);bind_spark_credentials(spark,writer=True,lease=lease)
                    lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs())
                    with Profile() as profile:
                        begin=time.monotonic()
                        with timed('target_open'):sink=sink_class(spark,s,lease=lease)
                        result=process_batch(rows,source,sink,0,max_records=args.count)
                        first_ms=round((time.monotonic()-begin)*1000)
                        before={t:int(DeltaTable.forPath(spark,p).history(1).select('version').first()[0]) for t,p in sink.paths.items()}
                        begin=time.monotonic();again=process_batch(rows,source,sink,1,max_records=args.count)
                        repeat_ms=round((time.monotonic()-begin)*1000)
                        after={t:int(DeltaTable.forPath(spark,p).history(1).select('version').first()[0]) for t,p in sink.paths.items()}
                        assert before==after,'replay changed Delta version'
                        keys={r['event_id'] for r in [parse(m.value) for m in rows]}
                        events=sink.existing('events',keys)
                        raw=sink.existing('raw',{e['raw_document_id'] for e in events.values()})
                        proofs[label]=(raw,events)
                        if label=='append_optimized':
                            # Simulate an acknowledged durable append whose
                            # reply is lost: retry must discover the real row.
                            from product_pipeline.contracts import event_for
                            from product_pipeline.bronze.contracts import event_row,event_from
                            doc=parse(next(iter(raw.values()))['raw_envelope_json'])
                            event=event_for(doc,str(uuid4()),now())
                            message=Message(original.kafka_topic,0,0,event['payload']['source_record_key'].encode(),canonical(event).encode())
                            value=event_row(event_from(message),message)
                            append=sink._append_chunk
                            def lost(table,values):append(table,values);raise OSError('injected response loss')
                            sink._append_chunk=lost
                            try:sink.insert('events',[value])
                            except OSError:pass
                            else:raise AssertionError('fault not injected')
                            sink._append_chunk=append
                            version=int(DeltaTable.forPath(spark,sink.paths['events']).history(1).select('version').first()[0])
                            sink.insert('events',[value])
                            assert version==int(DeltaTable.forPath(spark,sink.paths['events']).history(1).select('version').first()[0])
                            assert sink.existing('events',{value['event_id']})[value['event_id']]['event_json']==value['event_json']
                            report['uncertain_append_reused_without_duplicate']=True
                    report['runs'].append(dict(mode=label,result=result,repeated=again,first_ms=first_ms,
                        repeat_ms=repeat_ms,repeated_versions_unchanged=True,application_id=spark.sparkContext.applicationId,
                        runtime_metrics=profile.snapshot()))
                    release_spark(spark);quiet=True
                finally:lease.close(release=quiet)
            a,b=proofs['merge_baseline'],proofs['append_optimized']
            for table,left,right in [('raw',a[0],b[0]),('events',a[1],b[1])]:
                assert set(left)==set(right)
                assert all(same_content(table,left[k],right[k]) for k in left)
            report.update(status='VERIFIED',same_source_and_event_contents=True,session_metrics=dict(runtime.metrics))
        report['session_metrics']=dict(runtime.metrics)
    finally:
        source.close();report['finished_at']=now();atomic_json(output,report)
    print(json.dumps(report))


if __name__=='__main__':main()
