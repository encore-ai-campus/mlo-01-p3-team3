"""Explicit paused-window maintenance. Retain every prior Delta data file/version.

Run on Spark Master as pj3-spark, with the ordinary production Spark/S3
environment. Never start this while another Driver is running. No VACUUM.
"""
import argparse,os,sys,time,json
from pathlib import Path
from uuid import uuid4


def fingerprint(spark,path,version):
    from pyspark.sql import functions as F
    frame=spark.read.format('delta').option('versionAsOf',version).load(path)
    hashed=frame.select(F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(frame.columns)]),
        {'ignoreNullFields':'false'}),256).alias('h'))
    stats=[F.count('*').alias('rows')]
    stats += [F.sum(F.conv(F.substring('h',i*16+1,16),16,10).cast('decimal(38,0)')).alias('s'+str(i)) for i in range(4)]
    value=hashed.agg(*stats).first().asDict()
    return {k:str(v or 0) for k,v in value.items()}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--deployment-root',type=Path,required=True)
    args=parser.parse_args();root=args.deployment_root.resolve();sys.path[:0]=[str(root),str(root/'source')]
    from product_pipeline.files import read_json,atomic_json,lock
    from product_pipeline.contracts import now
    from product_pipeline.recovery_probe import probe
    from product_pipeline.lease_recovery import assert_quiet
    from product_pipeline.operations.server_flow import worker
    from product_pipeline.spark_session import SessionScope,get_or_create,release_spark
    from product_pipeline.spark_storage import storage_options,bind_spark_credentials
    from product_pipeline.bronze.lease import MongoLease
    from product_pipeline.bronze.metadata import read_metadata
    from product_pipeline.delta_key_index import seed
    from recover_writer_lease import settings_for
    import production_work
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    os.environ['PIPELINE_DELTA_KEY_INDEX_DIR']=str(root/'key-index-cache')
    output=root/'perf-20261020/range-compaction-20261022.json'
    report=dict(status='RUNNING',started_at=now(),tables=[],vacuum=False,old_versions_deleted=False)
    with lock(root/'controller.lock'):
        assert (root/'pause-after-window.json').exists() and worker(root) is None,'PAUSED_WORKER_REQUIRED'
        assert_quiet(probe(master_url='http://192.168.0.121:8080/json/',ca='/etc/pj3-secrets/spark/ca.pem',
            client_cert='/etc/pj3-secrets/spark/cert.pem',client_key='/etc/pj3-secrets/spark/key.pem',
            worker_hosts=['192.168.0.122','192.168.0.123','192.168.0.124']))
        _,options=production_work.setup()
        try:
            with SessionScope():
                for stage in ('bronze','silver','gold'):
                    epoch=read_json(root/'flow-progress.json')['epoch']
                    config=root/'epochs'/('%06d'%epoch)/(stage+'.json') if stage=='bronze' else root/(stage+'.json')
                    settings=settings_for(stage,config);lease=MongoLease(settings,os.environ[settings.lease_uri_env])
                    spark=None;quiet=False;lease.acquire()
                    lease.record_owner(root/'maintenance-owners'/(uuid4().hex+'.json'),'compact-'+stage)
                    try:
                        opts=storage_options(options,writer=True,lease=lease);builder=SparkSession.builder.appName('pipeline-compaction')
                        for key,value in opts.items():builder=builder.config(key,value)
                        builder=builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                        spark=get_or_create(builder,opts);bind_spark_credentials(spark,writer=True,lease=lease)
                        spark.conf.set('spark.sql.session.timeZone','UTC')
                        spark.conf.set('spark.databricks.delta.optimize.maxFileSize',str(32*1024*1024))
                        spark.conf.set('spark.databricks.delta.optimize.minFileSize',str(8*1024*1024))
                        spark.sparkContext._jsc.hadoopConfiguration().setLong('parquet.block.size',16*1024*1024)
                        lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs())
                        if stage=='bronze':
                            paths={'products':settings.bronze_path,'events':settings.events_path,'quarantine':settings.quarantine_path}
                        else:
                            if stage=='silver':
                                from product_pipeline.silver.delta import DeltaSink
                                target=settings.silver_root
                            else:
                                from product_pipeline.gold.storage import DeltaSink
                                target=settings.gold_root
                            paths={table:target.rstrip('/')+'/'+table for table in DeltaSink.schema_tables}
                        for table,path in paths.items():
                            started=time.monotonic();lease.check()
                            before=read_metadata(spark,path)
                            assert (before and before['properties'].get('delta.appendOnly')=='true'
                                    and before['properties'].get('pipeline.targetFingerprint')==settings.fingerprint),'FOREIGN_TABLE_TARGET'
                            prior=fingerprint(spark,path,before['version'])
                            metrics=DeltaTable.forPath(spark,path).optimize().executeCompaction().collect()
                            lease.check();after=read_metadata(spark,path)
                            assert before['table_id']==after['table_id'] and before['schema']==after['schema']
                            assert before['properties']==after['properties']
                            current=fingerprint(spark,path,after['version']);assert prior==current,'COMPACTION_ROW_FINGERPRINT_CHANGED'
                            row=dict(stage=stage,table=table,before_version=before['version'],after_version=after['version'],
                                fingerprint=prior,verified_unchanged=True,duration_ms=round((time.monotonic()-started)*1000),
                                metrics=json.loads(json.dumps([x.asDict(recursive=True) for x in metrics],default=str)))
                            report['tables'].append(row);atomic_json(output,report);print(json.dumps(row),flush=True)
                        if stage=='bronze':
                            # Read-only equivalence check against the just-completed
                            # immutable window. No Kafka/SQL job is executed.
                            from dataclasses import replace
                            from types import SimpleNamespace
                            from product_pipeline.silver.config import SilverSettings
                            from product_pipeline.silver.source import BronzeSource
                            from product_pipeline.windows import seal_window
                            from product_pipeline.bronze.delta import DeltaSink as BronzeSink,same_content
                            from product_pipeline.bronze.contracts import raw_row
                            window=read_json(root/'epochs'/('%06d'%epoch)/'windows/000000/selection.json')
                            silver=replace(SilverSettings.load(root/'silver.json'),max_records=100)
                            source=BronzeSource(spark,silver,window['bronze_version'])
                            source.exclude_version(window['base_bronze_version']);docs=source.page(window['after'])
                            assert seal_window(window['bronze_version'],window['after'],docs,base_version=window['base_bronze_version'])==window,'RANGE_SELECTION_CHANGED'
                            sink=BronzeSink(spark,settings,lease=lease)
                            probe_sink=SimpleNamespace(spark=spark,settings=settings,paths=sink.paths,schemas=sink.schemas,
                                target_metadata={'raw':dict(sink.target_metadata['raw'],version=window['base_bronze_version'])})
                            stored=BronzeSink._newly_stored(probe_sink,'raw',len(docs))
                            assert stored is not None and all(d['_id'] in stored and same_content('raw',stored[d['_id']],raw_row(d)) for d in docs),'RANGE_STORAGE_PROOF_CHANGED'
                            report['range_verification']=dict(status='PASSED',window_hash=window['content_hash'],observations=len(docs),raw_bodies_verified=True)
                            atomic_json(output,report)
                        if stage!='bronze':
                            sink=DeltaSink(spark,settings,lease=lease)
                            seed(sink,{table:meta['version'] for table,meta in sink.target_metadata.items()})
                        release_spark(spark);quiet=True
                    finally:lease.close(release=quiet)
            report.update(status='COMPLETED',finished_at=now());atomic_json(output,report)
        except BaseException as exc:
            report.update(status='FAILED',error_type=type(exc).__name__,finished_at=now());atomic_json(output,report);raise


if __name__=='__main__':main()
