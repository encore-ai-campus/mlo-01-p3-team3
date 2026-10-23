"""Separate Delta write experiment on a scratch namespace; never part of a measured batch.

The batch measurement records each Delta step's wall time and the Spark job time inside it, but a
step also includes upstream computation. This probe writes rows that are already computed and
cached, so a write's time is only the write: file output plus whatever the Driver does around the
jobs. It separates
  - a new table's first write from appends to an existing table,
  - one output file from about 100 files (the batch tables' 128-bucket layout),
  - whether each write produced a commit (the table version advanced) and whether a checkpoint
    exists afterwards.
Driver time outside Spark jobs is reported as is; this probe does not attribute it to log commit,
listing or checkpointing. The scratch namespace is deleted at the end.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE)]

# Exact form only: no traversal or other segments can normalize to a real namespace.
PROBE_ROOT = re.compile(r's3a://pj3-data/direct-v2/delta-probe-[0-9]{14}')


def payload(size, index):
    """Deterministic high-entropy text: a SHA-256 hex chain, compressible only to about half.

    Real batch JSON compresses better than this, so file bytes here are an upper bound; compare the
    reported output_bytes with the batch snapshots' output_bytes before reading byte-driven costs.
    """
    parts, block = [], hashlib.sha256(b'%d' % index).digest()
    while sum(map(len, parts)) < size:
        block = hashlib.sha256(block).digest()
        parts.append(block.hex())
    return ''.join(parts)[:size]


def main(argv=None):
    parser = argparse.ArgumentParser(description='Delta 쓰기 분리 실험 (임시 네임스페이스)')
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--root', required=True, help="s3a://.../delta-probe-<stamp>, deleted at the end")
    parser.add_argument('--rows', type=int, default=100)
    parser.add_argument('--row-bytes', type=int, default=170000)
    parser.add_argument('--appends', type=int, default=3)
    args = parser.parse_args(argv)
    if not PROBE_ROOT.fullmatch(args.root):
        raise SystemExit('PROBE_ROOT_REQUIRED')
    from pipeline_v2.adapters.spark_runtime import ServerLake
    from pipeline_v2.adapters.timing import Timing
    from pipeline_v2.domain.contracts import digest
    config = json.loads(args.config.read_text(encoding='utf-8'))
    config = dict(config, lake_root=args.root, target=config['target'] + ':delta-probe')
    timing = Timing(config['state_root'])
    scope = 'delta-probe:' + args.root.rsplit('/', 1)[-1]
    lake = ServerLake(config, limits=type('L', (), {'history_bytes': 1 << 30})(), writable=True, timing=timing)
    results, created = [], False
    try:
        with timing.measure('command.total', scope):
            lake._open()
            spark = lake.spark
            context = spark.sparkContext
            root = spark._jvm.org.apache.hadoop.fs.Path(args.root)
            if root.getFileSystem(context._jsc.hadoopConfiguration()).exists(root):
                # Never write into, or later delete, a namespace this run did not create.
                raise SystemExit('PROBE_ROOT_EXISTS')
            created = True
            context.setLocalProperty('pipeline.v2.scope_hash', digest(scope))
            from delta.tables import DeltaTable
            from pyspark.sql import functions as F
            rows = [(str(i), 'probe:%d' % i, payload(args.row_bytes, i)) for i in range(args.rows)]
            base = (spark.createDataFrame(rows, 'row_key string, product_key string, payload_json string')
                .withColumn('record_hash', F.sha2('payload_json', 256))
                .withColumn('bucket', F.pmod(F.xxhash64('product_key'), F.lit(128)).cast('int')))
            # Materialize before any timed write so the writes do not include computing the rows.
            frames = dict(one_row=base.limit(1).coalesce(1).persist(), one_file=base.coalesce(1).persist(),
                bucketed=base.repartition('bucket').persist())
            for frame in frames.values():
                frame.count()
            jvm = spark._jvm
            path_of = lambda table: args.root + '/' + table

            def log_files(table):
                folder = jvm.org.apache.hadoop.fs.Path(path_of(table) + '/_delta_log')
                fs = folder.getFileSystem(context._jsc.hadoopConfiguration())
                names = [status.getPath().getName() for status in fs.listStatus(folder)]
                return dict(commits=sum(n.endswith('.json') for n in names),
                    checkpoints=sum('.checkpoint' in n for n in names))

            def write(case, table, frame, partitioned):
                before = DeltaTable.forPath(spark, path_of(table)).history(1).first()['version'] \
                    if DeltaTable.isDeltaTable(spark, path_of(table)) else None
                stage = 'probe.' + case
                context.setLocalProperty('pipeline.v2.stage', stage)
                writer = frame.write.format('delta').mode('append')
                if partitioned:
                    writer = writer.partitionBy('bucket')
                started = time.perf_counter()
                writer.save(path_of(table))
                elapsed = (time.perf_counter() - started) * 1000
                context.setLocalProperty('pipeline.v2.stage', 'probe.inspect')
                history = DeltaTable.forPath(spark, path_of(table)).history(1).first().asDict(recursive=True)
                metrics = history.get('operationMetrics') or {}
                results.append(dict(case=case, table=table, write_ms=round(elapsed, 1),
                    version_before=before, version_after=int(history['version']),
                    committed=int(history['version']) == (0 if before is None else before + 1),
                    output_files=int(metrics['numFiles']) if 'numFiles' in metrics else None,
                    output_bytes=int(metrics['numOutputBytes']) if 'numOutputBytes' in metrics else None,
                    log=log_files(table)))

            for name, partitioned in (('one_row', False), ('one_file', False), ('bucketed', True)):
                write(name + '.create', name, frames[name], partitioned)
                for attempt in range(args.appends):
                    write('%s.append%d' % (name, attempt + 1), name, frames[name], partitioned)
    finally:
        try:
            if created and lake.spark is not None:
                folder = lake.spark._jvm.org.apache.hadoop.fs.Path(args.root)
                folder.getFileSystem(lake.spark.sparkContext._jsc.hadoopConfiguration()).delete(folder, True)
        finally:
            lake.close()
            timing.flush()
    print(json.dumps(dict(status='PROBE_DONE', scope=scope, rows=args.rows, row_bytes=args.row_bytes,
        results=results, note='write_ms includes Spark jobs and Driver work; see spark-metrics jobs_by_stage '
        'for the job part. Driver time outside jobs is not attributed further.')))


if __name__ == '__main__':
    main()
