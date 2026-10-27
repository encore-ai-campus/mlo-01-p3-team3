"""Immutable image vectors: bounded imports and fixed-version serving exports."""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import struct
import tempfile
from uuid import NAMESPACE_URL, uuid5

from .contracts import digest
from .embedding_table import CONTRACT, create, schema
from .errors import PipelineError


def row(value):
    """Validate before any write; float32 canonicalization survives Delta round trips."""
    import re
    fields = ['image_checksum', 'preprocessed_checksum', 'model_version', 'preprocessing_version', 'dimension', 'vector']
    if not isinstance(value, dict) or any(k not in value for k in fields):
        raise PipelineError('EMBEDDING_ROW_INVALID')
    for key in ('image_checksum', 'preprocessed_checksum'):
        if not isinstance(value[key], str) or not re.fullmatch('[0-9a-f]{64}', value[key]):
            raise PipelineError('EMBEDDING_CHECKSUM_INVALID')
    dimension = value['dimension']
    if type(dimension) is not int or not 128 <= dimension <= 3072:
        raise PipelineError('EMBEDDING_DIMENSION_INVALID')
    for key in ('model_version', 'preprocessing_version'):
        if not isinstance(value[key], str) or not value[key].strip() or len(value[key]) > 191:
            raise PipelineError('EMBEDDING_VERSION_INVALID')
    if not isinstance(value['vector'], list) or len(value['vector']) != dimension:
        raise PipelineError('EMBEDDING_VECTOR_INVALID')
    vector = []
    for number in value['vector']:
        if isinstance(number, bool) or not isinstance(number, (float, int)) or not math.isfinite(number):
            raise PipelineError('EMBEDDING_VECTOR_INVALID')
        try:
            number = struct.unpack('f', struct.pack('f', number))[0]
        except (OverflowError, struct.error):
            raise PipelineError('EMBEDDING_VECTOR_INVALID') from None
        if not math.isfinite(number):
            raise PipelineError('EMBEDDING_VECTOR_INVALID')
        vector.append(number)
    if not math.isclose(sum(x*x for x in vector), 1, abs_tol=2e-4):
        raise PipelineError('EMBEDDING_L2_VECTOR_REQUIRED')
    identity = {k: value[k] for k in ('image_checksum', 'model_version', 'preprocessing_version', 'dimension')}
    embedding_id = uuid5(NAMESPACE_URL, CONTRACT + ':' + digest(identity)).hex
    body = {k: value[k] for k in fields if k != 'vector'}
    body.update(embedding_id=embedding_id, vector=vector, status='READY')
    record_hash = digest(body)
    if value.get('embedding_id', embedding_id) != embedding_id or value.get('record_hash', record_hash) != record_hash:
        raise PipelineError('EMBEDDING_ID_OR_HASH_CONFLICT')
    asset = value.get('asset_id')
    if asset is not None and (not isinstance(asset, str) or not re.fullmatch('[0-9a-f]{32}', asset)):
        raise PipelineError('EMBEDDING_ASSET_ID_INVALID')
    stamp = value.get('created_at')
    if isinstance(stamp, str):
        stamp = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    if not isinstance(stamp, datetime):
        raise PipelineError('EMBEDDING_CREATED_AT_REQUIRED')
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)  # Delta reads the UTC session as naive datetime.
    return {**body, 'asset_id': asset, 'record_hash': record_hash, 'created_at': stamp.astimezone(timezone.utc)}


def import_vectors(spark, settings, lease, input_path):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    # Validate the complete input once before creating or changing a table.
    path = Path(input_path)
    def values():
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                if len(line) > 256 * 1024:
                    raise PipelineError('EMBEDDING_INPUT_ROW_TOO_LARGE')
                if line.strip():
                    yield row(json.loads(line))
    expected_hash = file_hash(path)
    count = sum(1 for _ in values())
    if not count:
        raise PipelineError('EMBEDDING_INPUT_EMPTY')
    create(spark, settings, lease)
    inserted = reused = 0
    pending = []
    def flush(batch):
        nonlocal inserted, reused
        lease.check(settings)
        unique = {}
        for value in batch:
            previous = unique.setdefault(value['embedding_id'], value)
            if previous['record_hash'] != value['record_hash']:
                raise PipelineError('EMBEDDING_DUPLICATE_INPUT_CONFLICT')
        current = spark.read.format('delta').load(settings.table_path).filter(F.col('embedding_id').isin(list(unique)))
        old = {}
        for raw in current.toLocalIterator():
            value = row(raw.asDict(recursive=True))
            if value['embedding_id'] in old or value['record_hash'] != unique[value['embedding_id']]['record_hash']:
                raise PipelineError('EMBEDDING_EXISTING_ROW_CONFLICT')
            old[value['embedding_id']] = value
        new = [v for k, v in unique.items() if k not in old]
        if new:
            frame = spark.createDataFrame(new, schema())
            lease.check(settings)
            DeltaTable.forPath(spark, settings.table_path).alias('t').merge(frame.alias('s'),
                't.embedding_id=s.embedding_id').whenNotMatchedInsertAll().execute()
            lease.check(settings)
            stored = (spark.read.format('delta').load(settings.table_path)
                      .filter(F.col('embedding_id').isin(list(unique))).select('embedding_id', 'record_hash').collect())
            if len(stored) != len(unique) or {v['embedding_id']: v['record_hash'] for v in stored} != {k: v['record_hash'] for k, v in unique.items()}:
                raise PipelineError('EMBEDDING_COMMIT_UNCONFIRMED')
        inserted += len(new)
        reused += len(batch) - len(new)
    for value in values():
        pending.append(value)
        if len(pending) == 100:
            flush(pending)
            pending = []
    if pending:
        flush(pending)
    if file_hash(path) != expected_hash:
        raise PipelineError('EMBEDDING_INPUT_CHANGED')
    return dict(status='IMPORTED', inserted=inserted, reused=reused, input_checksum=expected_hash)


def file_hash(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def export_vectors(spark, settings, *, version, model, preprocessing, dimension, output):
    import numpy as np
    from pyspark.sql import functions as F
    if type(version) is not int or version < 0:
        raise PipelineError('FIXED_DELTA_VERSION_REQUIRED')
    output = Path(output).resolve()
    if output.exists():
        raise PipelineError('IMMUTABLE_VECTOR_EXPORT_EXISTS')
    selected = (spark.read.format('delta').option('versionAsOf', version).load(settings.table_path)
        .filter((F.col('status') == 'READY') & (F.col('model_version') == model)
                & (F.col('preprocessing_version') == preprocessing) & (F.col('dimension') == dimension)))
    count = selected.count()  # Full export explicitly needs its allocation size.
    if not count:
        raise PipelineError('NO_READY_VECTORS')
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.vectors-build-', dir=output.parent) as temporary:
        root = Path(temporary)
        ready = root / 'ready'
        ready.mkdir()
        matrix = np.lib.format.open_memmap(ready / 'vectors.npy', mode='w+', dtype=np.float32, shape=(count, dimension))
        seen = set()
        try:
            with (ready / 'mapping.jsonl').open('w', encoding='utf-8', newline='\n') as stream:
                for n, raw in enumerate(selected.orderBy('embedding_id').toLocalIterator()):
                    value = row(raw.asDict(recursive=True))
                    if value['embedding_id'] in seen:
                        raise PipelineError('EMBEDDING_DUPLICATE_DELTA_ROW')
                    seen.add(value['embedding_id'])
                    matrix[n] = value['vector']
                    meta = {k: value[k] for k in ('embedding_id', 'asset_id', 'image_checksum', 'preprocessed_checksum', 'record_hash')}
                    stream.write(json.dumps(dict(meta, row=n), sort_keys=True) + '\n')
            matrix.flush()
        finally:
            matrix._mmap.close()
        if len(seen) != count:
            raise PipelineError('EMBEDDING_EXPORT_INCOMPLETE')
        manifest = dict(contract='delta-image-vectors-export-v1', delta_path=settings.table_path, delta_version=version,
            model_version=model, preprocessing_version=preprocessing, dimension=dimension, count=count,
            vectors_file='vectors.npy', vectors_checksum=file_hash(ready / 'vectors.npy'),
            mapping_file='mapping.jsonl', mapping_checksum=file_hash(ready / 'mapping.jsonl'))
        (ready / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding='utf-8')
        ready.rename(output)
    return dict(status='EXPORTED', count=count, manifest=str(output / 'manifest.json'))
