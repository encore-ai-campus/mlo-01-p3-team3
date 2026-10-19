"""Separate Spark job entrypoint; source ingestion dependencies stay lightweight."""
import argparse
import os
import signal
import threading
import time
from uuid import uuid4

from ..cli import emit
from ..errors import PipelineError
from ..files import parse
from .config import BronzeSettings


def await_window(query,lease,max_batches=0):
    """Stop only after Spark reports checkpointed progress, never callback output."""
    if not max_batches:
        query.awaitTermination();lease.check()
        return dict(source_drained=True)
    committed=set()
    while query.isActive:
        lease.check();query.awaitTermination(1)
        progress=query.lastProgress
        if progress and progress.get('numInputRows',0)>0:committed.add(int(progress['batchId']))
        if len(committed)>=max_batches:
            query.stop();query.awaitTermination();lease.check()
            return dict(source_drained=False,committed_batches=len(committed),last_committed_batch_id=max(committed))
    lease.check()
    return dict(source_drained=True,committed_batches=len(committed),last_committed_batch_id=max(committed) if committed else None)


def env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError('REQUIRED_ENV_MISSING')
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description='Kafka/MongoDB -> product Bronze Delta')
    parser.add_argument('--config', required=True)
    parser.add_argument('--available-now', action='store_true', help='Drain current Kafka backlog then exit')
    parser.add_argument('--max-batches',type=int,default=0,help='0=전체 적체, 양수=확정 배치 단위로 후속 단계 인계')
    parser.add_argument('--result-file',help='정상 종료 확인 후 단계 실행 결과 기록')
    args = parser.parse_args(argv)
    spark = source = query = lease = None
    previous_sigterm = None
    result_path=None
    result, code = {'result': 'failed'}, 2
    execution, started = str(uuid4()), time.monotonic()
    try:
        if args.max_batches<0 or (args.max_batches and not args.available_now):raise PipelineError('BRONZE_INVALID_WINDOW')
        if args.result_file:
            from pathlib import Path
            if Path(args.result_file).resolve()==Path(args.config).resolve():raise PipelineError('BRONZE_RESULT_OVERLAPS_CONFIG')
            result_path=args.result_file
        if threading.current_thread() is threading.main_thread():
            def terminate(signum, frame):
                raise KeyboardInterrupt
            previous_sigterm = signal.signal(signal.SIGTERM, terminate)
        settings = BronzeSettings.load(args.config)
        if any(getattr(settings, k).startswith('replace-with-') for k in ('environment_id', 'mongo_database', 'kafka_cluster_id')):
            raise PipelineError('EXAMPLE_CONFIG_NOT_DEPLOYABLE')
        uri, kafka = env(settings.mongo_uri_env), parse(env(settings.kafka_options_env))
        spark_options = parse(env(settings.spark_options_env))
        from .stream import kafka_options, start
        kafka_options(kafka, settings)
        if (not isinstance(spark_options, dict) or any(not isinstance(k, str) or not k.startswith('spark.')
                or not isinstance(v, str) for k, v in spark_options.items())):
            raise PipelineError('BRONZE_INVALID_SPARK_OPTIONS')
        from .lease import MongoLease
        lease = MongoLease(settings, env(settings.lease_uri_env))
        lease.acquire()  # No Spark job, checkpoint or Delta mutation before ownership.
        from ..spark_storage import storage_options, bind_spark_credentials
        spark_options = storage_options(spark_options, writer=True, lease=lease)
        from pyspark.sql import SparkSession
        builder = SparkSession.builder.appName('product-bronze-' + settings.environment_id)
        for key, value in spark_options.items():
            builder = builder.config(key, value)
        spark = (builder.config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .config('spark.redaction.regex', '(?i)secret|password|token|access[.]?key|connection|sasl[.]jaas')
                 .getOrCreate())
        bind_spark_credentials(spark, writer=True, lease=lease)
        def stop_on_lease_loss():
            # Called on the heartbeat thread, including while the Driver is idle.
            # Failure to stop leaves the lease latched and each callback fenced.
            try:
                spark.sparkContext.cancelAllJobs()
            finally:
                if query is not None:
                    query.stop()
        lease.start_heartbeat(stop_on_lease_loss)
        from .mongo import MongoSource
        source = MongoSource(settings, uri)
        query = start(spark, settings, source, kafka, available_now=args.available_now,
                      progress=lambda value: emit({'stage': 'bronze_batch', **value}), lease=lease)
        lease.check()
        emit({'result': 'started', 'execution_id': execution, 'query_id': str(query.id),
              'available_now': args.available_now})
        window=await_window(query,lease,args.max_batches)
        lease.check()
        result, code = {'result': 'completed',**window}, 0
    except KeyboardInterrupt:
        result, code = {'result': 'interrupted'}, 130
    except Exception as exc:
        result = {'result': 'failed', 'error_code': exc.code if isinstance(exc, PipelineError) else type(exc).__name__}
    finally:
        stopped = True
        for resource, cleanup in ((query, lambda: query.stop() if query.isActive else None),
                                  (source, lambda: source.close()), (spark, lambda: spark.stop())):
            if resource is not None:
                try:
                    cleanup()
                except Exception:
                    stopped = False
        if lease is not None:
            try:
                lease.close(release=stopped)
            except Exception:
                stopped = False
        if not stopped:
            result, code = {'result': 'failed', 'error_code': 'BRONZE_SHUTDOWN_UNCONFIRMED'}, 2
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)
    result={**result,'execution_id':execution,'duration_ms':round((time.monotonic()-started)*1000)}
    if result_path:
        from ..files import atomic_json
        atomic_json(result_path,result)
    emit(result)
    return code
