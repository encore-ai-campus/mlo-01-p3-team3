"""Separate Spark job entrypoint; source ingestion dependencies stay lightweight."""
import argparse
import os
import time
from uuid import uuid4

from ..cli import emit
from ..errors import PipelineError
from ..files import parse
from .config import BronzeSettings


def env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError('REQUIRED_ENV_MISSING')
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description='Kafka/MongoDB -> product Bronze Delta')
    parser.add_argument('--config', required=True)
    parser.add_argument('--available-now', action='store_true', help='Drain current Kafka backlog then exit')
    args = parser.parse_args(argv)
    spark = source = query = None
    execution, started = str(uuid4()), time.monotonic()
    try:
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
        from pyspark.sql import SparkSession
        builder = SparkSession.builder.appName('product-bronze-' + settings.environment_id)
        for key, value in spark_options.items():
            builder = builder.config(key, value)
        spark = (builder.config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .config('spark.redaction.regex', '(?i)secret|password|token|access[.]?key|connection|sasl[.]jaas')
                 .getOrCreate())
        from .mongo import MongoSource
        source = MongoSource(settings, uri)
        query = start(spark, settings, source, kafka, available_now=args.available_now,
                      progress=lambda value: emit({'stage': 'bronze_batch', **value}))
        emit({'result': 'started', 'execution_id': execution, 'query_id': str(query.id),
              'available_now': args.available_now})
        query.awaitTermination()
        emit({'result': 'completed', 'execution_id': execution,
              'duration_ms': round((time.monotonic() - started) * 1000)})
        return 0
    except KeyboardInterrupt:
        emit({'result': 'interrupted', 'execution_id': execution,
              'duration_ms': round((time.monotonic() - started) * 1000)})
        return 130
    except Exception as exc:
        emit({'result': 'failed', 'error_code': exc.code if isinstance(exc, PipelineError) else type(exc).__name__,
              'execution_id': execution, 'duration_ms': round((time.monotonic() - started) * 1000)})
        return 2
    finally:
        if query is not None and query.isActive:
            query.stop()
        if source is not None:
            source.close()
        if spark is not None:
            spark.stop()
