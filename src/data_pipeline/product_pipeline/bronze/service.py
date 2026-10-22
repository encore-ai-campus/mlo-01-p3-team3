"""Bronze execution and resource lifetime, independent of command arguments."""
from dataclasses import dataclass
import os
import signal
import threading
import time
from uuid import uuid4

from ..errors import PipelineError
from ..files import parse
from ..execution import StageResult, ignore_progress
from .config import BronzeSettings


@dataclass(frozen=True)
class Request:
    config: str
    available_now: bool = False
    max_batches: int = 0
    result_file: str | None = None


def await_window(query,lease,max_batches=0,window_control=None):
    """Stop only after Spark reports checkpointed progress, never callback output."""
    if not max_batches:
        query.awaitTermination();lease.check()
        return dict(source_drained=True)
    committed=set()
    def progress_commits():
        progress=query.lastProgress
        if progress and progress.get('numInputRows',0)>0:committed.add(int(progress['batchId']))
    def stopped_boundary():
        progress_commits()
        boundary=(window_control or {}).get('boundary_batch_id')
        return (type(boundary) is int and len(committed)>=max_batches and boundary==max(committed)+1
                and not query.isActive)
    while query.isActive:
        lease.check()
        try:query.awaitTermination(1)
        except Exception:
            if not stopped_boundary():raise
        progress_commits()
        if len(committed)>=max_batches:
            try:query.stop();query.awaitTermination()
            except Exception:
                if not stopped_boundary():raise
            lease.check()
            return dict(source_drained=False,committed_batches=len(committed),last_committed_batch_id=max(committed))
    try:query.awaitTermination()
    except Exception:
        if not stopped_boundary():raise
    progress_commits();lease.check()
    return dict(source_drained=not stopped_boundary(),committed_batches=len(committed),last_committed_batch_id=max(committed) if committed else None)



def env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError('REQUIRED_ENV_MISSING')
    return value



def run(args: Request, *, progress=ignore_progress):
    from .metrics import Profile
    with Profile() as profile:
        return _run(args, profile, progress)


def _run(args, profile, emit):
    from .metrics import timed
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
        from .recovery import validate_replay_plan
        validate_replay_plan(settings)
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
        with timed('lease_acquire'):lease.acquire()  # No Spark/checkpoint/Delta mutation before ownership.
        if result_path:
            from pathlib import Path
            result['lease_owner_ref']=lease.record_owner(Path(result_path).parent/'lease-owners'/(execution+'.json'),execution)
        from ..spark_storage import storage_options, bind_spark_credentials
        spark_options = storage_options(spark_options, writer=True, lease=lease)
        from pyspark.sql import SparkSession
        builder = SparkSession.builder.appName('product-bronze-' + settings.environment_id)
        for key, value in spark_options.items():
            builder = builder.config(key, value)
        builder = (builder.config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .config('spark.redaction.regex', '(?i)secret|password|token|access[.]?key|connection|sasl[.]jaas'))
        from ..spark_session import get_or_create,has_session
        profile.add('spark_session_reused',int(has_session()))
        with timed('spark_connect'):spark=get_or_create(builder,spark_options)
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
        with timed('mongo_connect'):source = MongoSource(settings, uri)
        window_control=dict(limit=args.max_batches,calls=0,boundary_batch_id=None)
        with timed('query_start'):
            query = start(spark, settings, source, kafka, available_now=args.available_now,
                          progress=lambda value: emit({'stage': 'bronze_batch', **value}), lease=lease,window_control=window_control)
        lease.check()
        emit({'result': 'started', 'execution_id': execution, 'query_id': str(query.id),
              'available_now': args.available_now})
        with timed('query_run'):window=await_window(query,lease,args.max_batches,window_control)
        lease.check()
        result, code = {'result': 'completed',**window}, 0
    except KeyboardInterrupt:
        result, code = {'result': 'interrupted'}, 130
    except Exception as exc:
        gap='OffsetOutOfRangeException' in str(exc) or 'KAFKA_DATA_LOSS' in str(exc)
        result = {'result': 'failed', 'error_code': 'BRONZE_KAFKA_OFFSETS_OUT_OF_RANGE' if gap else exc.code if isinstance(exc, PipelineError) else type(exc).__name__}
    finally:
        stopped = True
        from ..spark_session import release_spark
        for name,resource, cleanup in (('query_stop',query, lambda: query.stop() if query.isActive else None),
                                  ('mongo_close',source, lambda: source.close()), ('spark_release',spark, lambda: release_spark(spark))):
            if resource is not None:
                try:
                    with timed(name):cleanup()
                except Exception:
                    stopped = False
        if lease is not None:
            try:
                with timed('lease_close'):lease.close(release=stopped)
            except Exception:
                stopped = False
        if not stopped:
            result, code = {'result': 'failed', 'error_code': 'BRONZE_SHUTDOWN_UNCONFIRMED'}, 2
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)
    if lease is not None and getattr(lease,'_lost',False) is True:
        from .lease import RECOVERY_ERRORS
        if result.get('error_code') not in RECOVERY_ERRORS:
            result.update(result='failed',trigger_error_code=result.get('error_code'),error_code='BRONZE_WRITER_LEASE_LOST');code=2
    if lease is not None and getattr(lease,'loss_diagnostic',None):result['lease_failure']=lease.loss_diagnostic
    if lease is not None and getattr(lease,'owner_ref',None):result['lease_owner_ref']=lease.owner_ref
    from ..spark_session import stamp_driver_exit
    if stamp_driver_exit(result):code=2
    result={**result,'execution_id':execution,'duration_ms':round((time.monotonic()-started)*1000),
            'runtime_metrics':profile.snapshot()}
    if result_path:
        from ..files import atomic_json
        atomic_json(result_path,result)
    return StageResult(code, result)
