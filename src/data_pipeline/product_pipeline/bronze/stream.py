"""Spark Kafka source -> foreachBatch -> Delta. Spark alone owns offsets."""
import time
from uuid import uuid4
from ..errors import Conflict, PipelineError
from .batch import process_batch
from .contracts import Message
from .delta import DeltaSink


def kafka_options(options, settings):
    if (not isinstance(options, dict) or not options.get('kafka.bootstrap.servers')
            or any(not isinstance(k, str) or not k.startswith('kafka.') for k in options)
            or any(k in options for k in ('kafka.group.id', 'kafka.enable.auto.commit', 'kafka.auto.offset.reset'))
            or any(not isinstance(v, str) for v in options.values())):
        raise PipelineError('BRONZE_INVALID_KAFKA_OPTIONS')
    from .recovery import recovery
    from ..contracts import canonical
    value=recovery(settings)
    return {**options, 'subscribe': settings.kafka_topic, 'startingOffsets': canonical(value['starting_offsets']) if value else 'earliest',
            'failOnDataLoss': 'true', 'maxOffsetsPerTrigger': str(settings.max_records)}


def bind_checkpoint(spark, settings, *, lease=None):
    if lease is None:
        raise PipelineError('BRONZE_WRITER_LEASE_REQUIRED')
    lease.check(settings)
    from .recovery import checkpoint_path,recovery
    path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path(settings))
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    marker = spark._jvm.org.apache.hadoop.fs.Path(path, '_pipeline_target')
    if fs.exists(marker):
        stream = fs.open(marker)
        try:
            if stream.readUTF() != settings.fingerprint:
                raise Conflict('BRONZE_CHECKPOINT_TARGET_CONFLICT')
        finally:
            stream.close()
    else:
        if fs.exists(path) and len(fs.listStatus(path)):
            raise Conflict('BRONZE_CHECKPOINT_BINDING_MISSING')
        lease.check(settings)
        fs.mkdirs(path)
        lease.check(settings)
        stream = fs.create(marker, False)
        try:
            lease.check(settings)
            stream.writeUTF(settings.fingerprint)
            stream.hflush()
        finally:
            stream.close()
        lease.check(settings)
    value=recovery(settings)
    if value:
        proof=spark._jvm.org.apache.hadoop.fs.Path(path,'_pipeline_recovery')
        if fs.exists(proof):
            stream=fs.open(proof)
            try:
                if stream.readUTF()!=value['content_hash']:raise Conflict('BRONZE_RECOVERY_CHECKPOINT_CHANGED')
            finally:stream.close()
        else:
            if any(p.getPath().getName()!='_pipeline_target' for p in fs.listStatus(path)):
                raise Conflict('BRONZE_RECOVERY_CHECKPOINT_UNBOUND')
            lease.check(settings);stream=fs.create(proof,False)
            try:stream.writeUTF(value['content_hash']);stream.hflush()
            finally:stream.close()
            lease.check(settings)


def check_kafka_cluster(spark, options, settings):
    admin = None
    try:
        properties = spark._jvm.java.util.Properties()
        for key, value in options.items():
            if key.startswith('kafka.'):
                properties.setProperty(key[6:], value)
        properties.setProperty('default.api.timeout.ms', '10000')
        properties.setProperty('request.timeout.ms', '5000')
        admin = spark._jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
        actual = admin.describeCluster().clusterId().get(10000, spark._jvm.java.util.concurrent.TimeUnit.MILLISECONDS)
        if actual != settings.kafka_cluster_id:
            raise Conflict('BRONZE_KAFKA_CLUSTER_CONFLICT')
        from .recovery import check_frontiers
        check_frontiers(spark,admin,settings)
    except PipelineError:
        raise
    except Exception:
        raise PipelineError('BRONZE_KAFKA_CLUSTER_UNCONFIRMED') from None
    finally:
        if admin is not None:
            admin.close(spark._jvm.java.time.Duration.ofSeconds(5))


def start(spark, settings, source, options, available_now=False, progress=None, *, lease=None,window_control=None):
    if lease is None:
        raise PipelineError('BRONZE_WRITER_LEASE_REQUIRED')
    lease.check(settings)
    options = kafka_options(options, settings)
    from .metrics import current,bind,timed
    with timed('kafka_connect_and_frontiers'):check_kafka_cluster(spark, options, settings)
    lease.check(settings)
    with timed('checkpoint_binding'):bind_checkpoint(spark, settings, lease=lease)
    profile=current()
    with timed('delta_target_open'):sink = DeltaSink(spark, settings, lease=lease)
    frame = spark.readStream.format('kafka').options(**options).load()

    def callback(data, batch_id):
        # Fail before any I/O beyond a bounded window. Returning normally here
        # would incorrectly acknowledge offsets for data we have not stored.
        if window_control is not None and window_control['limit'] and window_control['calls']>=window_control['limit']:
            window_control['boundary_batch_id']=int(batch_id)
            if progress:progress(dict(batch_id=batch_id,status='WINDOW_BOUNDARY'))
            raise PipelineError('BRONZE_WINDOW_BOUNDARY')
        started, execution_id = time.monotonic(), str(uuid4())
        # Executed on Driver: credentials never serialized into Executor closures.
        messages = (Message(row.topic, int(row.partition), int(row.offset),
                            None if row.key is None else bytes(row.key),
                            None if row.value is None else bytes(row.value))
                    for row in data.select('topic', 'partition', 'offset', 'key', 'value').toLocalIterator())
        try:
            lease.check(settings)
            result = process_batch(messages, source, sink, batch_id,
                max_records=settings.max_records, max_raw_bytes=settings.max_raw_bytes,
                max_input_bytes=settings.max_input_bytes)
            lease.check(settings)  # A lost lease must not return success to foreachBatch.
        except Exception as exc:
            # Spark prints Python callback exceptions; never expose raw SDK credentials there.
            if progress:
                # Class/condition identifiers only; SDK messages may contain
                # source bodies or authentication details.
                diagnostic={}
                java=getattr(exc,'java_exception',None)
                causes=[]
                if java is not None:
                    for _ in range(8):
                        try:
                            name=str(java.getClass().getName())
                            condition=None
                            try:condition=java.getErrorClass()
                            except Exception:pass
                            import re
                            causes.append(dict(type=name,condition=condition if isinstance(condition,str) and re.fullmatch(r'[A-Z0-9_.]+',condition) else None))
                            java=java.getCause()
                            if java is None:break
                        except Exception:break
                    diagnostic['java_causes']=causes
                progress({'batch_id': batch_id, 'status': 'FAILED', 'execution_id': execution_id,
                          'error_code': exc.code if isinstance(exc, PipelineError) else type(exc).__name__,
                          **diagnostic,
                          'duration_ms': round((time.monotonic() - started) * 1000)})
            raise PipelineError(exc.code if isinstance(exc, PipelineError) else type(exc).__name__) from None
        if window_control is not None and result.get('received',0)>0:window_control['calls']+=1
        if progress:
            progress({**result, 'status': 'COMMITTED', 'execution_id': execution_id,
                      'duration_ms': round((time.monotonic() - started) * 1000)})

    def batch(data,batch_id):
        # Py4J invokes foreachBatch on another Python thread.
        with bind(profile),timed('foreach_batch'):
            return callback(data,batch_id)

    from .recovery import checkpoint_path
    writer = (frame.writeStream.foreachBatch(batch).option('checkpointLocation', checkpoint_path(settings))
              .queryName('product-bronze-' + settings.environment_id))
    lease.check(settings)
    return writer.trigger(availableNow=True).start() if available_now else writer.trigger(
        processingTime=f'{settings.trigger_seconds} seconds').start()
