"""Create/inspect an independent Silver vector table; never generate embeddings."""
import argparse
from dataclasses import dataclass
import json
import os
import re
import signal
import threading
from types import SimpleNamespace

from .bronze.config import location
from .contracts import digest
from .errors import PipelineError

CONTRACT = 'product-image-vectors-v1'
TABLE = 'product_image_vectors'
# A row represents an image result, not a product or recommendation approval.
FIELDS = (
    ('embedding_id', 'string', False),
    ('asset_id', 'string', True),  # Optional until the service UUID is published.
    ('image_checksum', 'string', False),
    ('preprocessed_checksum', 'string', False),
    ('model_version', 'string', False),
    ('preprocessing_version', 'string', False),
    ('dimension', 'integer', False),
    ('vector', 'array<float>', False),
    ('status', 'string', False),
    ('record_hash', 'string', False),
    ('created_at', 'timestamp', False),
)
PROPERTIES = {
    'delta.appendOnly': 'true',
    'pipeline.embeddingContract': CONTRACT,
    'delta.constraints.embedding_dimension': 'dimension > 0 AND size(vector) = dimension',
    'delta.constraints.embedding_ready': "status = 'READY'",
    'delta.constraints.image_checksum': "image_checksum RLIKE '^[0-9a-f]{64}$'",
    'delta.constraints.preprocessed_checksum': "preprocessed_checksum RLIKE '^[0-9a-f]{64}$'",
    'delta.constraints.record_hash': "record_hash RLIKE '^[0-9a-f]{64}$'",
}


@dataclass(frozen=True)
class Settings:
    environment_id: str
    silver_root: str
    checkpoint_path: str
    lease_database: str = 'pipeline_ops'
    lease_collection: str = 'pipeline_writer_leases'
    mongo_replica_set: str = 'rs-products'
    lease_seconds: int = 120
    lease_heartbeat_seconds: int = 10

    def __post_init__(self):
        for name in ('environment_id', 'lease_database', 'lease_collection', 'mongo_replica_set'):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', value):
                raise PipelineError('EMBEDDING_CONFIG_INVALID')
        root, checkpoint = location(self.silver_root), location(self.checkpoint_path)
        if root == checkpoint or root.startswith(checkpoint + '/') or checkpoint.startswith(root + '/'):
            raise PipelineError('EMBEDDING_PATHS_OVERLAP')
        if self.lease_database != 'pipeline_ops' or self.lease_collection != 'pipeline_writer_leases':
            raise PipelineError('EMBEDDING_SHARED_LEASE_REQUIRED')

    @property
    def table_path(self):
        return location(self.silver_root) + '/' + TABLE

    @property
    def fingerprint(self):
        return digest(dict(environment_id=self.environment_id, silver_root=location(self.silver_root),
                           checkpoint_path=location(self.checkpoint_path), contract=CONTRACT))

    def lease_settings(self):
        # The existing protocol compares exact URIs, not parent/child paths.
        # Claim the WHOLE Silver root, as the normal Silver Driver does.
        return SimpleNamespace(**self.__dict__, bronze_path=location(self.silver_root),
                               events_path=location(self.silver_root),
                               quarantine_path=location(self.silver_root), fingerprint=self.fingerprint)


def schema():
    from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType
    types = {'string': StringType(), 'integer': IntegerType(),
             'timestamp': TimestampType(), 'array<float>': ArrayType(FloatType(), containsNull=False)}
    return StructType([StructField(name, types[kind], nullable) for name, kind, nullable in FIELDS])


def inspect(spark, settings):
    from delta.tables import DeltaTable
    path = settings.table_path
    if not DeltaTable.isDeltaTable(spark, path):
        hpath = spark._jvm.org.apache.hadoop.fs.Path(path)
        if hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(hpath):
            raise PipelineError('EMBEDDING_NON_DELTA_TARGET_EXISTS')
        return dict(status='ABSENT', table_path=path)
    table = DeltaTable.forPath(spark, path)
    actual = spark.read.format('delta').load(path).schema
    properties = table.detail().select('properties').first()[0]
    if actual != schema() or any(properties.get(k) != v for k, v in PROPERTIES.items()):
        raise PipelineError('EMBEDDING_TABLE_CONTRACT_CONFLICT')
    version = int(table.history(1).select('version').first()[0])
    return dict(status='READY', table_path=path, contract=CONTRACT, version=version,
                schema=actual.simpleString())


def create(spark, settings, lease):
    """Idempotent empty-table creation. Existing data/schema are never replaced."""
    from delta.tables import DeltaTable
    lease.check(settings)
    before = inspect(spark, settings)
    if before['status'] == 'READY':
        return {**before, 'created': False}
    builder = DeltaTable.createIfNotExists(spark).location(settings.table_path).addColumns(schema())
    for key, value in PROPERTIES.items():
        builder = builder.property(key, value)
    lease.check(settings)
    builder.execute()
    lease.check(settings)
    return {**inspect(spark, settings), 'created': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Silver 이미지 임베딩 테이블 조회·빈 테이블 생성')
    parser.add_argument('--environment-id', required=True)
    parser.add_argument('--silver-root', required=True)
    parser.add_argument('--checkpoint-path', required=True)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--create', action='store_true', help='빈 테이블 생성. 생략하면 읽기 전용 조회')
    actions.add_argument('--import-jsonl', help='검증한 완료 벡터만 적재. API 호출 없음')
    actions.add_argument('--export-dir', help='고정 Delta 버전에서 NPY·매핑 파일 생성')
    parser.add_argument('--delta-version', type=int)
    parser.add_argument('--model', default='gemini-embedding-2')
    parser.add_argument('--preprocessing', default='exif-rgb-fit1024-png-l2-v1')
    parser.add_argument('--dimension', type=int, default=768)
    args = parser.parse_args(argv)
    spark = lease = None
    previous_signal = None
    writer = bool(args.create or args.import_jsonl)
    result, code = dict(status='FAILED'), 2
    try:
        settings = Settings(args.environment_id, args.silver_root, args.checkpoint_path)
        paths = (location(settings.silver_root), location(settings.checkpoint_path))
        if (any(not p.startswith('s3a://pj3-data/') for p in paths)
                or os.environ.get('PIPELINE_SPARK_S3_PROFILE') != 'garage_v241'):
            raise PipelineError('EMBEDDING_GARAGE_PROFILE_REQUIRED')
        options = json.loads(os.environ['EMBEDDING_SPARK_OPTIONS'])
        if (not isinstance(options, dict) or any(not isinstance(k, str) or not k.startswith('spark.')
                or not isinstance(v, str) for k, v in options.items())):
            raise PipelineError('EMBEDDING_SPARK_OPTIONS_INVALID')
        if any(re.search(r'(?i)secret|password|token|access[.]?key', k) for k in options):
            raise PipelineError('EMBEDDING_SPARK_SECRET_OPTION_FORBIDDEN')
        if threading.current_thread() is threading.main_thread():
            def terminate(*_):
                raise KeyboardInterrupt
            previous_signal = signal.signal(signal.SIGTERM, terminate)
        if writer:
            from .bronze.lease import MongoLease
            lease = MongoLease(settings.lease_settings(), os.environ['EMBEDDING_LEASE_MONGO_URI'])
            lease.acquire()
        from .spark_storage import storage_options, bind_spark_credentials
        from pyspark.sql import SparkSession
        options = storage_options(options, writer=writer, lease=lease)
        builder = SparkSession.builder.appName('product-image-vector-table')
        for key, value in options.items():
            builder = builder.config(key, value)
        spark = (builder.config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .getOrCreate())
        bind_spark_credentials(spark, writer=writer, lease=lease)
        if lease is not None:
            lease.start_heartbeat(lambda: spark.sparkContext.cancelAllJobs())
        spark.sparkContext.setLogLevel('ERROR')
        if args.import_jsonl:
            from .embedding_vectors import import_vectors
            result = import_vectors(spark, settings, lease, args.import_jsonl)
        elif args.export_dir:
            from .embedding_vectors import export_vectors
            result = export_vectors(spark, settings, version=args.delta_version, model=args.model,
                                    preprocessing=args.preprocessing, dimension=args.dimension, output=args.export_dir)
        else:
            result = create(spark, settings, lease) if args.create else inspect(spark, settings)
        code = 0
    except KeyboardInterrupt:
        result, code = dict(status='INTERRUPTED'), 130
    except Exception as exc:
        result['error_code'] = exc.code if isinstance(exc, PipelineError) else type(exc).__name__
    finally:
        stopped = True
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                stopped = False
        if lease is not None:
            try:
                lease.close(release=stopped)
            except Exception:
                stopped = False
        if not stopped:
            result, code = dict(status='FAILED', error_code='EMBEDDING_SHUTDOWN_UNCONFIRMED'), 2
        if previous_signal is not None:
            signal.signal(signal.SIGTERM, previous_signal)
    print(json.dumps(result, ensure_ascii=True))
    return code
