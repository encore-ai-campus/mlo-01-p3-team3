"""Opt-in real Mongo/S3/Kafka -> Spark/Delta acceptance. Never deletes data.

Exit 0 = PASS, 2 = FAIL, 3 = NOT_RUN (missing opt-in/configuration).
No fake backend, package installation, Docker, or automatic service startup.
"""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import time

from product_pipeline.config import Settings
from product_pipeline.contracts import canonical
from product_pipeline.errors import PipelineError
from product_pipeline.files import atomic_json, parse, rows
from product_pipeline.snapshot import verify
from product_pipeline.bronze.config import BronzeSettings


def require(condition, code):
    if not condition:
        raise PipelineError(code)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--bronze-config', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--allow-live-writes', action='store_true')
    parser.add_argument('--query-timeout', type=int, default=300)
    parser.add_argument('--resume-registered', action='store_true',
                        help='Resume only matching registrations with no raw/event/image completion')
    args = parser.parse_args(argv)
    started = time.monotonic()
    report = {'status': 'NOT_RUN', 'stages': [], 'scope': 'real Mongo+S3+Kafka+Spark+Delta'}
    output = None

    def stage(name, operation):
        tick = time.monotonic()
        item = {'stage': name, 'status': 'FAIL'}
        report['stages'].append(item)
        try:
            result = operation()
            item['status'] = 'PASS'
            return result
        except Exception as exc:
            item['error_code'] = exc.code if isinstance(exc, PipelineError) else type(exc).__name__
            raise
        finally:
            item['duration_ms'] = round((time.monotonic() - tick) * 1000)

    try:
        missing = [name for name in ('snapshot', 'config', 'bronze_config', 'output') if getattr(args, name) is None]
        if not args.allow_live_writes:
            missing.append('allow_live_writes')
        if missing:
            report['missing'] = missing
            return 3
        settings, bronze = Settings.load(args.config), BronzeSettings.load(args.bronze_config)
        env_names = {settings.mongo_uri_env, settings.s3_endpoint_env, settings.kafka_config_env,
                     bronze.mongo_uri_env, bronze.kafka_options_env, bronze.spark_options_env, bronze.lease_uri_env}
        missing = sorted(name for name in env_names if not os.environ.get(name))
        if missing:
            report['missing_env'] = missing
            return 3
        # All endpoint/options validation precedes constructing a network client.
        require(1 <= args.query_timeout <= 3600, 'ACCEPTANCE_INVALID_TIMEOUT')
        require(all(getattr(settings, k) == getattr(bronze, k) for k in
                    ('environment_id', 'mongo_database', 'mongo_replica_set', 'mongo_uri_env', 'kafka_topic')),
                'ACCEPTANCE_TARGET_MISMATCH')
        require(not any(getattr(settings, k).startswith('replace-with-') for k in
                        ('environment_id', 'mongo_database', 'image_bucket'))
                and not bronze.kafka_cluster_id.startswith('replace-with-'), 'EXAMPLE_CONFIG_NOT_DEPLOYABLE')
        endpoint = os.environ[settings.s3_endpoint_env]
        require(endpoint == 'aws' or endpoint.startswith('https://'), 'S3_TLS_ENDPOINT_REQUIRED')
        producer_options = parse(os.environ[settings.kafka_config_env])
        kafka = parse(os.environ[bronze.kafka_options_env])
        spark_options = parse(os.environ[bronze.spark_options_env])
        from product_pipeline.bronze.stream import kafka_options, start
        kafka_options(kafka, bronze)
        require(isinstance(producer_options, dict) and producer_options.get('bootstrap.servers') ==
                kafka.get('kafka.bootstrap.servers'), 'ACCEPTANCE_KAFKA_TARGET_MISMATCH')
        require(isinstance(spark_options, dict) and all(isinstance(k, str) and k.startswith('spark.')
                and isinstance(v, str) for k, v in spark_options.items()), 'BRONZE_INVALID_SPARK_OPTIONS')
        require(not any(k in spark_options for k in ('spark.jars.packages', 'spark.jars.repositories')),
                'ACCEPTANCE_PREINSTALLED_JARS_REQUIRED')
        manifest = verify(args.snapshot)
        require(1 <= manifest['product_count'] <= 10 and manifest['verified_image_relations'] > 0,
                'ACCEPTANCE_SMALL_IMAGE_SNAPSHOT_REQUIRED')
        inputs = list(rows(args.snapshot / 'observations.jsonl'))
        candidate = args.output.resolve()
        snapshot = args.snapshot.resolve()
        require(not candidate.exists() and not candidate.is_relative_to(snapshot) and not snapshot.is_relative_to(candidate),
                'ACCEPTANCE_NEW_SEPARATE_OUTPUT_REQUIRED')
        candidate.mkdir(parents=True)
        output = candidate
        report.update(status='FAIL', snapshot_id=manifest['snapshot_id'], product_count=len(inputs),
                      target_fingerprint=settings.fingerprint)
        with ExitStack() as stack:
            # Actual adapters only: SDK failures are acceptance failures, never skips.
            from product_pipeline.mongo import MongoLedger
            from product_pipeline.objects import S3Objects
            from product_pipeline.publisher import KafkaPublisher
            from product_pipeline.bronze.mongo import MongoSource
            from product_pipeline.bronze.lease import MongoLease
            from product_pipeline.runner import ingest
            ledger = stage('mongo_connect', lambda: MongoLedger(settings, os.environ[settings.mongo_uri_env]))
            stack.callback(ledger.close)
            objects = S3Objects.from_settings(settings, endpoint)
            publisher = KafkaPublisher(settings.kafka_topic, producer_options, max_attempts=settings.max_attempts)
            stack.callback(publisher.close)
            stage('s3_preflight', objects.preflight)
            stage('kafka_durability_preflight', publisher.preflight)
            keys = [row['observation_key'] for row in inputs]
            require(ledger.raw.count_documents({'observation_key': {'$in': keys}}) == 0,
                    'ACCEPTANCE_FRESH_RAW_REQUIRED')
            registered_ids = set()
            if args.resume_registered:
                from product_pipeline.runner import check_registration
                for row in inputs:
                    saved = ledger.get(row['observation_key'])
                    check_registration(row, saved, settings)
                    require(saved.get('stage') == 'OBSERVATION_REGISTERED'
                            and not saved.get('raw_confirmed') and not saved.get('event_confirmed')
                            and not saved.get('image_state_vector') and not saved.get('lease_owner'),
                            'ACCEPTANCE_REGISTRATION_NOT_PRISTINE')
                    require(saved['document']['input_snapshot_id'] == manifest['snapshot_id'],
                            'ACCEPTANCE_REGISTRATION_SNAPSHOT_MISMATCH')
                    registered_ids.add(saved['document']['_id'])
                report['resumed_registration_ids'] = sorted(registered_ids)
            else:
                require(ledger.observations.count_documents({'observation_key': {'$in': keys}}) == 0,
                        'ACCEPTANCE_FRESH_OBSERVATIONS_REQUIRED')
            shutdown = {'spark_stopped': True}
            lease = MongoLease(bronze, os.environ[bronze.lease_uri_env])
            stack.callback(lambda: lease.close(release=shutdown['spark_stopped']))
            stage('bronze_writer_lease', lease.acquire)
            from product_pipeline.spark_storage import storage_options, bind_spark_credentials
            spark_options = storage_options(spark_options, writer=True, lease=lease)
            from pyspark.sql import SparkSession
            builder = SparkSession.builder.appName('product-live-acceptance')
            for key, value in spark_options.items():
                builder = builder.config(key, value)
            spark = stage('spark_connect', lambda: builder
                .config('spark.sql.session.timeZone', 'UTC')
                .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                .config('spark.redaction.regex', '(?i)secret|password|token|access[.]?key|connection|sasl[.]jaas')
                .getOrCreate())
            shutdown['spark_stopped'] = False
            def stop_spark():
                spark.stop()
                shutdown['spark_stopped'] = True
            stack.callback(stop_spark)
            bind_spark_credentials(spark, writer=True, lease=lease)
            active = {'query': None}
            def stop_on_lease_loss():
                try:
                    spark.sparkContext.cancelAllJobs()
                finally:
                    if active['query'] is not None:
                        active['query'].stop()
            lease.start_heartbeat(stop_on_lease_loss)
            # Refuse existing Delta/checkpoint locations; never erase shared server data.
            for uri in (bronze.bronze_path, bronze.events_path, bronze.quarantine_path, bronze.checkpoint_path):
                path = spark._jvm.org.apache.hadoop.fs.Path(uri)
                require(not path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path),
                        'ACCEPTANCE_NEW_DELTA_PATHS_REQUIRED')
            source = stage('mongo_reader_connect', lambda: MongoSource(bronze, os.environ[bronze.mongo_uri_env]))
            stack.callback(source.close)
            first = stage('ingest_first', lambda: ingest(snapshot, settings, ledger, objects, publisher, output / 'audit'))
            require(first['result'] == 'completed' and first['raw_created'] == first['events_sent'] == len(inputs),
                    'ACCEPTANCE_FIRST_INGEST_FAILED')
            registrations = [ledger.get(key) for key in keys]
            ids = {r['document']['_id'] for r in registrations}
            require(not registered_ids or ids == registered_ids, 'ACCEPTANCE_RESUMED_IDS_CHANGED')
            events = {r['event']['event_id'] for r in registrations}
            require(len(ids) == len(events) == len(inputs), 'ACCEPTANCE_ID_COLLISION')

            def verify_sources():
                for saved in registrations:
                    require(source.get(saved['document']['_id']) == saved['document'], 'ACCEPTANCE_MONGO_CONTENT_MISMATCH')
                for row in inputs:
                    for image in row['images']:
                        if image.get('object_key'):
                            require(objects._exists(image['object_key'], image['sha256'], image['bytes']),
                                    'ACCEPTANCE_IMAGE_UNCONFIRMED')
            stage('mongo_and_s3_reread', verify_sources)
            second = stage('ingest_repeat', lambda: ingest(snapshot, settings, ledger, objects, publisher, output / 'audit'))
            require(second['result'] == 'completed' and all(second[k] == 0 for k in
                    ('raw_created', 'objects_written', 'events_sent')), 'ACCEPTANCE_REPEAT_WRITES')

            def drain():
                query = start(spark, bronze, source, kafka, available_now=True,
                    progress=lambda event: atomic_json(output / 'batches' / (event['execution_id'] + '.json'), event),
                    lease=lease)
                active['query'] = query
                try:
                    require(query.awaitTermination(args.query_timeout), 'ACCEPTANCE_STREAM_TIMEOUT')
                    lease.check()
                finally:
                    if query.isActive:
                        query.stop()
                    active['query'] = None

            from product_pipeline.bronze.delta import DeltaSink
            def reread():
                sink = DeltaSink(spark, bronze, lease=lease)
                counts = {key: spark.read.format('delta').load(uri).count() for key, uri in sink.paths.items()}
                require(counts == {'raw': len(inputs), 'events': len(inputs), 'quarantine': 0},
                        'ACCEPTANCE_DELTA_COUNTS_MISMATCH')
                stored, delivered = sink.existing('raw', ids), sink.existing('events', events)
                require(set(stored) == ids and set(delivered) == events, 'ACCEPTANCE_DELTA_IDS_MISMATCH')
                for saved in registrations:
                    require(stored[saved['document']['_id']]['raw_envelope_json'] == canonical(saved['document'])
                            and delivered[saved['event']['event_id']]['event_json'] == canonical(saved['event']),
                            'ACCEPTANCE_DELTA_CONTENT_MISMATCH')
                return counts

            stage('kafka_to_bronze_available_now', drain)
            report['first_delta_counts'] = stage('delta_reread', reread)
            replay = stage('ingest_event_replay', lambda: ingest(snapshot, settings, ledger, objects, publisher,
                          output / 'audit', replay_events=True))
            require(replay['result'] == 'completed' and replay['raw_created'] == replay['objects_written'] == 0
                    and replay['events_sent'] == len(inputs), 'ACCEPTANCE_REPLAY_FAILED')
            stage('kafka_replay_to_bronze_available_now', drain)
            report['repeated_delta_counts'] = stage('delta_reread_after_replay', reread)
            report['raw_document_ids'], report['event_ids'] = sorted(ids), sorted(events)
            report['status'] = 'PASS'
        return 0
    except KeyboardInterrupt:
        report.update(status='FAIL', error_code='INTERRUPTED')
        return 130
    except Exception as exc:
        report.update(status='FAIL', error_code=exc.code if isinstance(exc, PipelineError) else type(exc).__name__)
        return 2
    finally:
        report['duration_ms'] = round((time.monotonic() - started) * 1000)
        if output is not None and output.is_dir():
            atomic_json(output / 'report.json', report)
        print(canonical(report), flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
