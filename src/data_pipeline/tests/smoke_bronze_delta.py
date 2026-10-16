"""Real local Spark/Delta test; no Kafka/Mongo/S3 connections.

python -m tests.smoke_bronze_delta --source-rehearsal .runtime/actual-smoke-resumed-v1 --output .runtime/bronze-delta-new
"""
import argparse
import copy
from pathlib import Path

from product_pipeline.bronze.batch import process_batch
from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.bronze.contracts import Message
from product_pipeline.bronze.delta import DeltaSink
from product_pipeline.bronze.stream import bind_checkpoint
from product_pipeline.contracts import canonical
from product_pipeline.errors import PipelineError
from product_pipeline.files import atomic_json, read_json


class Source:
    def __init__(self, documents):
        self.documents = documents
    def get(self, key):
        if key not in self.documents:
            raise PipelineError('BRONZE_RAW_UNCONFIRMED')
        return copy.deepcopy(self.documents[key])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-rehearsal', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    records = [read_json(p) for p in sorted((args.source_rehearsal / 'ledger/observations').glob('*.json'))]
    if not records:
        raise ValueError('No rehearsal observations')
    settings = BronzeSettings('local-delta-smoke', 'rehearsal', (output/'raw').as_uri(),
        (output/'events').as_uri(), (output/'quarantine').as_uri(), (output/'checkpoint').as_uri(),
        single_writer_confirmed=True, kafka_cluster_id='local-no-kafka')
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master('local[2]').appName('product-bronze-local-smoke')
        .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2')
        .config('spark.driver.memory', '1g')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        bind_checkpoint(spark, settings)
        bind_checkpoint(spark, settings)
        sink = DeltaSink(spark, settings)
        source = Source({r['document']['_id']: r['document'] for r in records})
        messages = [Message('product.raw.collected', 0, index,
            r['document']['source_record_key'].encode(), canonical(r['event']).encode()) for index, r in enumerate(records)]
        first = process_batch(messages, source, sink, 0)
        repeated = process_batch(messages, source, sink, 1)
        bad = Message('product.raw.collected', 0, len(messages), b'bad', b'{broken')
        process_batch([bad], source, sink, 2)
        process_batch([bad], source, sink, 3)
        changed = copy.deepcopy(records[0]['event'])
        changed['occurred_at'] = '2026-10-17T00:00:00.000Z'
        conflict = Message('product.raw.collected', 0, len(messages) + 1, messages[0].key, canonical(changed).encode())
        process_batch([conflict], source, sink, 4)
        # Same bytes observed through a fresh sink instance simulate a restarted Driver.
        restored = DeltaSink(spark, settings)
        process_batch(messages, source, restored, 5)
        counts = {k: spark.read.format('delta').load(p).count() for k, p in sink.paths.items()}
        stored = restored.existing('raw', set(source.documents))
        same = all(stored[k]['raw_envelope_json'] == canonical(v)
                   and stored[k]['collected_at'] == v['collected_at'] for k, v in source.documents.items())
        passed = counts == {'raw': len(records), 'events': len(records), 'quarantine': 2} and same
        summary = {'passed': passed, 'spark_version': spark.version, 'products': len(records),
                   'counts': counts, 'payload_envelope_timestamp_match': same, 'first': first,
                   'repeated': repeated, 'external_service_connections': 0}
        atomic_json(output / 'report.json', summary)
        print(canonical(summary))
        return 0 if passed else 2
    finally:
        spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())
