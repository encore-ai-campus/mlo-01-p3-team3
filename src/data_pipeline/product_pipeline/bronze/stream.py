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
    return {**options, 'subscribe': settings.kafka_topic, 'startingOffsets': 'earliest',
            'failOnDataLoss': 'true', 'maxOffsetsPerTrigger': str(settings.max_records)}


def bind_checkpoint(spark, settings):
    path = spark._jvm.org.apache.hadoop.fs.Path(settings.checkpoint_path)
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
        fs.mkdirs(path)
        stream = fs.create(marker, False)
        try:
            stream.writeUTF(settings.fingerprint)
            stream.hflush()
        finally:
            stream.close()


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
    except PipelineError:
        raise
    except Exception:
        raise PipelineError('BRONZE_KAFKA_CLUSTER_UNCONFIRMED') from None
    finally:
        if admin is not None:
            admin.close(spark._jvm.java.time.Duration.ofSeconds(5))


def start(spark, settings, source, options, available_now=False, progress=None):
    options = kafka_options(options, settings)
    check_kafka_cluster(spark, options, settings)
    bind_checkpoint(spark, settings)
    sink = DeltaSink(spark, settings)
    frame = spark.readStream.format('kafka').options(**options).load()

    def batch(data, batch_id):
        started, execution_id = time.monotonic(), str(uuid4())
        # Executed on Driver: credentials never serialized into Executor closures.
        messages = (Message(row.topic, int(row.partition), int(row.offset),
                            None if row.key is None else bytes(row.key),
                            None if row.value is None else bytes(row.value))
                    for row in data.select('topic', 'partition', 'offset', 'key', 'value').toLocalIterator())
        try:
            result = process_batch(messages, source, sink, batch_id,
                max_records=settings.max_records, max_raw_bytes=settings.max_raw_bytes,
                max_input_bytes=settings.max_input_bytes)
        except Exception as exc:
            # Spark prints Python callback exceptions; never expose raw SDK credentials there.
            if progress:
                progress({'batch_id': batch_id, 'status': 'FAILED', 'execution_id': execution_id,
                          'error_code': exc.code if isinstance(exc, PipelineError) else type(exc).__name__,
                          'duration_ms': round((time.monotonic() - started) * 1000)})
            raise PipelineError(exc.code if isinstance(exc, PipelineError) else type(exc).__name__) from None
        if progress:
            progress({**result, 'status': 'COMMITTED', 'execution_id': execution_id,
                      'duration_ms': round((time.monotonic() - started) * 1000)})

    writer = (frame.writeStream.foreachBatch(batch).option('checkpointLocation', settings.checkpoint_path)
              .queryName('product-bronze-' + settings.environment_id))
    return writer.trigger(availableNow=True).start() if available_now else writer.trigger(
        processingTime=f'{settings.trigger_seconds} seconds').start()
