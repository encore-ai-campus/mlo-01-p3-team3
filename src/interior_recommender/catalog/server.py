"""Read-only MySQL/Garage adapters and immutable CPU catalog snapshots.

No DDL, approvals, index activation, embedding generation or database writes.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile

import numpy as np

from ..assets import checksum, png_bytes, read_json, write_bytes, write_json
from ..domain.interior import product_profile
from ..domain.product_links import comparison_url
from ..domain.recommendation import Product, ProductIndex

CONTRACT = 'mysql-delta-catalog-v1'
VECTOR_CONTRACT = 'delta-image-vectors-export-v1'
ENV = 'INTERIOR_CATALOG_SERVER_CONFIG'
HEX = re.compile(r'^[0-9a-f]{64}$')
UUID = re.compile(r'^[0-9a-f]{32}$')


class ServerCatalogError(ValueError):
    pass


def canonical(value):
    def encode(item):
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, datetime):
            return item.replace(tzinfo=timezone.utc).isoformat() if item.tzinfo is None else item.astimezone(timezone.utc).isoformat()
        raise TypeError(type(item).__name__)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False, default=encode)


def parsed(value, kind=dict):
    try:
        result = json.loads(value) if isinstance(value, (str, bytes)) else value
    except (ValueError, TypeError):
        raise ServerCatalogError('SOURCE_JSON_INVALID') from None
    if not isinstance(result, kind):
        raise ServerCatalogError('SOURCE_JSON_INVALID')
    return result


def secret_file(path):
    path = Path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_size > 1024 * 1024:
        raise ServerCatalogError('PROTECTED_CONFIG_INVALID')
    if os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ServerCatalogError('PROTECTED_CONFIG_OWNER_OR_MODE_INVALID')
    return path.read_text(encoding='utf-8-sig')


def configuration(path=None):
    selected = path or os.environ.get(ENV)
    if not selected:
        raise ServerCatalogError('SERVER_CONFIG_REQUIRED')
    value = parsed(secret_file(selected))
    if value.get('contract') != CONTRACT:
        raise ServerCatalogError('SERVER_CONFIG_CONTRACT_INVALID')
    db = value.get('mysql', {})
    if (not re.fullmatch(r'[A-Za-z0-9_]+', db.get('database', '')) or not db.get('host')
            or not db.get('user') or not db.get('password') or not Path(db.get('ssl_ca', '')).is_file()
            or not isinstance(db.get('port', 3306), int)):
        raise ServerCatalogError('MYSQL_TLS_CONFIG_REQUIRED')
    for name in ('embedding_model', 'preprocessing_version', 'scope'):
        if not isinstance(value.get(name), str) or not value[name]:
            raise ServerCatalogError('SERVER_CONFIG_INVALID')
    if type(value.get('dimension')) is not int or not 128 <= value['dimension'] <= 3072:
        raise ServerCatalogError('SERVER_CONFIG_INVALID')
    return value


@contextmanager
def connect(config):
    import pymysql
    db = config['mysql']
    try:
        connection = pymysql.connect(host=db['host'], port=db.get('port', 3306), user=db['user'],
            password=db['password'], database=db['database'], charset='utf8mb4', autocommit=False,
            connect_timeout=5, read_timeout=10, write_timeout=10, cursorclass=pymysql.cursors.DictCursor,
            ssl_ca=db['ssl_ca'], ssl_verify_cert=True, ssl_verify_identity=True)
    except pymysql.MySQLError:
        raise ServerCatalogError('MYSQL_UNAVAILABLE') from None
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET time_zone = '+00:00'")
            cursor.execute('SET SESSION max_execution_time = 5000')
            cursor.execute('SET SESSION transaction_read_only = ON')
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
        yield MySQLSource(connection, config)
    except pymysql.MySQLError:
        raise ServerCatalogError('MYSQL_QUERY_UNAVAILABLE') from None
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


BASE_COLUMNS = '''p.id AS product_uuid,p.product_key,p.source_code,p.source_product_id,p.name,
 p.width_mm,p.depth_mm,p.height_mm,p.price_krw,p.comparison_url,p.attributes_json,
 p.field_status_json,p.field_lineage_json,p.gold_snapshot_ref_json,p.status AS product_status,
 p.catalog_ready,p.recommendation_ready,p.comparison_link_ready,p.representative_image_id,
 c.code AS category_code,c.active AS category_active,c.recognition_enabled,
 pi.id AS image_uuid,pi.asset_id,pi.active AS image_active,pi.gold_recommendation_approved,
 pi.usage_review_json,pi.source_version,pi.image_relation_key,
 a.bucket,a.object_key,a.checksum AS image_checksum,a.status AS asset_status,a.usage_policy_json'''
JOINS = ''' FROM products p JOIN categories c ON c.id=p.category_id
 JOIN product_images pi ON pi.product_id=p.id JOIN assets a ON a.id=pi.asset_id'''
SELECT = 'SELECT ' + BASE_COLUMNS + ''',e.id AS embedding_id,e.image_checksum AS embedding_checksum,e.model_version,
 e.preprocessing_version,e.dimension,e.vector_ref_json,e.status AS embedding_status
''' + JOINS + ' JOIN product_embeddings e ON e.asset_id=a.id'


class MySQLSource:
    def __init__(self, connection, config):
        self.connection, self.config = connection, config

    def all(self, sql, params=()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def health(self):
        row = self.all('SELECT @@hostname AS server,@@read_only AS replica,UTC_TIMESTAMP(3) AS checked_at')[0]
        row.update(self.all('SELECT EXISTS(SELECT 1 FROM products LIMIT 1) AS has_products, '
            'EXISTS(SELECT 1 FROM product_embeddings LIMIT 1) AS has_embeddings, '
            'EXISTS(SELECT 1 FROM pipeline_publications WHERE status=%s LIMIT 1) AS has_publication', ('COMPLETED',))[0])
        return row

    def index(self):
        rows = self.all('SELECT id,scope,model_version,preprocessing_version,dimension,manifest_json,status '
                        'FROM search_index_versions WHERE scope=%s AND status=%s LIMIT 2',
                        (self.config['scope'], 'ACTIVE'))
        if len(rows) != 1:
            raise ServerCatalogError('INDEX_NOT_READY')
        row = rows[0]
        if (row['model_version'], row['preprocessing_version'], row['dimension']) != (
                self.config['embedding_model'], self.config['preprocessing_version'], self.config['dimension']):
            raise ServerCatalogError('INDEX_EMBEDDING_CONTRACT_MISMATCH')
        return row

    def rows(self, image_ids=None):
        base = SELECT + ' WHERE e.model_version=%s AND e.preprocessing_version=%s AND e.dimension=%s'
        params = (self.config['embedding_model'], self.config['preprocessing_version'], self.config['dimension'])
        if image_ids is not None:
            if not image_ids or len(image_ids) > 100 or any(not UUID.fullmatch(x) for x in image_ids):
                raise ServerCatalogError('IMAGE_IDS_INVALID')
            for row in self.all(base + ' AND pi.id IN (' + ','.join('%s' for _ in image_ids) + ')', params + tuple(image_ids)):
                yield self.vector_identity(row)
            return
        # Keyset pagination stays inside the same repeatable-read transaction.
        after_image = after_embedding = ''
        while True:
            rows = self.all(base + ' AND (pi.id>%s OR (pi.id=%s AND e.id>%s)) '
                'ORDER BY pi.id,e.id LIMIT 200', params + (after_image, after_image, after_embedding))
            if not rows:
                break
            for row in rows:
                yield self.vector_identity(row)
            after_image, after_embedding = rows[-1]['image_uuid'], rows[-1]['embedding_id']

    @staticmethod
    def vector_identity(row):
        ref = parsed(row['vector_ref_json'])
        if not isinstance(ref.get('embedding_id'), str):
            raise ServerCatalogError('VECTOR_REFERENCE_MISMATCH')
        return {**row, 'embedding_registry_id': row['embedding_id'], 'embedding_id': ref['embedding_id']}

    def bootstrap_images(self):
        # Do not require recommendation_ready: initial vectors/index do not exist yet.
        after = ''
        while True:
            rows = self.all('SELECT ' + BASE_COLUMNS + JOINS + ' WHERE pi.id>%s ORDER BY pi.id LIMIT 200', (after,))
            if not rows:
                break
            yield from rows
            after = rows[-1]['image_uuid']

    def publication_complete(self, plan_id):
        return bool(self.all('SELECT plan_id FROM pipeline_publications WHERE plan_id=%s AND status=%s LIMIT 1',
                             (plan_id, 'COMPLETED')))

    def finish_snapshot(self):
        # Freeze metadata first; do not hold InnoDB snapshots during image downloads.
        self.connection.rollback()


def approved(value, scope, at=None):
    value = parsed(value)
    at = at or datetime.now(timezone.utc)
    scopes = value.get('scopes', [value.get('scope')])
    if not isinstance(scopes, list) or scope not in scopes or value.get('decision') != 'APPROVED' or not value.get('evidence_ref'):
        return False
    for field, lower in (('valid_from', True), ('expires_at', False)):
        if value.get(field):
            try:
                stamp = datetime.fromisoformat(value[field].replace('Z', '+00:00'))
            except (ValueError, TypeError, AttributeError):
                raise ServerCatalogError('REVIEW_TIMESTAMP_INVALID') from None
            if stamp.tzinfo is None or (stamp > at if lower else stamp <= at):
                return False
    return True


def eligible(row):
    if not (row['product_status'] == 'ACTIVE' and row['catalog_ready'] and row['recommendation_ready']
            and row['category_active'] and row['recognition_enabled'] and row['image_active']
            and row['gold_recommendation_approved'] and row['asset_status'] == 'AVAILABLE'
            and row['embedding_status'] == 'READY' and row['image_checksum'] == row['embedding_checksum']):
        return False
    return approved(row['usage_review_json'], 'RECOMMENDATION') and approved(row['usage_policy_json'], 'RECOMMENDATION')


def binding(row, index_id, index_manifest=None):
    return dict(image_uuid=row['image_uuid'], product_uuid=row['product_uuid'], asset_id=row['asset_id'],
                embedding_id=row['embedding_id'], embedding_registry_id=row.get('embedding_registry_id', row['embedding_id']),
                index_id=index_id, index_manifest_checksum=checksum(canonical(parsed(index_manifest)).encode()) if index_manifest is not None else None,
                row_hash=checksum(canonical(row).encode()))


class LiveGuard:
    def __init__(self, config=None):
        self.config = config or configuration()

    def validate_index(self, index_id, manifest_checksum):
        with connect(self.config) as source:
            active = source.index()
            if active['id'] != index_id or checksum(canonical(parsed(active['manifest_json'])).encode()) != manifest_checksum:
                raise ServerCatalogError('CURRENT_INDEX_CHANGED')

    def validate(self, bindings, *, link=False, scope='RECOMMENDATION'):
        if scope not in {'RECOMMENDATION', 'GENERATION_REFERENCE'}:
            raise ServerCatalogError('USAGE_SCOPE_UNSUPPORTED')
        if not bindings:
            raise ServerCatalogError('SERVER_BINDINGS_REQUIRED')
        with connect(self.config) as source:
            active = source.index()
            wanted = {b['image_uuid']: b for b in bindings}
            if len(wanted) != len(bindings):
                raise ServerCatalogError('DUPLICATE_SERVER_BINDING')
            rows = {}
            for row in source.rows(list(wanted)):
                expected = wanted[row['image_uuid']]
                if row['embedding_id'] != expected['embedding_id']:
                    continue
                if row['image_uuid'] in rows or not eligible(row) or binding(row, active['id'], active['manifest_json']) != expected:
                    raise ServerCatalogError('CURRENT_PRODUCT_OR_INDEX_CHANGED')
                if scope != 'RECOMMENDATION' and not (approved(row['usage_review_json'], scope)
                                                     and approved(row['usage_policy_json'], scope)):
                    raise ServerCatalogError('IMAGE_USAGE_NOT_APPROVED')
                snapshot = parsed(row['gold_snapshot_ref_json'])
                if not source.publication_complete(snapshot['plan_id']):
                    raise ServerCatalogError('PUBLICATION_NOT_COMPLETE')
                rows[row['image_uuid']] = row
            if set(rows) != set(wanted):
                raise ServerCatalogError('CURRENT_PRODUCT_OR_IMAGE_UNAVAILABLE')
            if link:
                if len(rows) != 1:
                    raise ServerCatalogError('LINK_TARGET_AMBIGUOUS')
                row = next(iter(rows.values()))
                if not row['comparison_link_ready']:
                    raise ServerCatalogError('LINK_NOT_READY')
                url = comparison_url(row['comparison_url'], 'danawa:' + row['source_product_id'])
                if not url:
                    raise ServerCatalogError('LINK_NOT_READY')
                observations = source.all('SELECT evidence_json FROM product_link_observations WHERE product_id=%s '
                    'AND source_url=%s AND link_kind=%s AND usable=1 ORDER BY created_at DESC LIMIT 1',
                    (row['product_uuid'], url, 'COMPARISON'))
                if not observations:
                    raise ServerCatalogError('LINK_EVIDENCE_REQUIRED')
                evidence = parsed(observations[0]['evidence_json'])
                expiry = evidence.get('expires_at')
                try:
                    stamp = datetime.fromisoformat(expiry.replace('Z', '+00:00')) if expiry else None
                except (ValueError, AttributeError, TypeError):
                    raise ServerCatalogError('LINK_EVIDENCE_INVALID') from None
                if stamp is None or stamp.tzinfo is None or stamp <= datetime.now(timezone.utc):
                    raise ServerCatalogError('LINK_EVIDENCE_EXPIRED')
                return url


class Garage:
    def __init__(self, config):
        import boto3
        from botocore.config import Config
        from urllib.parse import urlsplit
        cfg = config['s3']
        url = urlsplit(cfg['endpoint'])
        if url.scheme != 'https' or not url.hostname or url.username or url.password or url.path not in {'', '/'} or url.query or url.fragment:
            raise ServerCatalogError('S3_TLS_ENDPOINT_REQUIRED')
        if not isinstance(cfg.get('ssl_ca'), str) or not Path(cfg['ssl_ca']).is_file():
            raise ServerCatalogError('S3_CA_FILE_REQUIRED')
        fields = {}
        for line in secret_file(cfg['credentials_file']).splitlines():
            if not line.strip() or line.lstrip().startswith(('#', '!')):
                continue
            key, separator, value = line.partition('=')
            key = {'accessKey': 'aws_access_key_id', 'secretKey': 'aws_secret_access_key',
                   'aws_access_key_id': 'aws_access_key_id', 'aws_secret_access_key': 'aws_secret_access_key'}.get(key.strip())
            if key is None:
                continue
            if not separator or not value.strip() or key in fields:
                raise ServerCatalogError('S3_CREDENTIALS_INVALID')
            fields[key] = value.strip()
        if set(fields) != {'aws_access_key_id', 'aws_secret_access_key'}:
            raise ServerCatalogError('S3_CREDENTIALS_INVALID')
        self.bucket = cfg['bucket']
        self.client = boto3.client('s3', endpoint_url=cfg['endpoint'], region_name='us-east-1',
            aws_access_key_id=fields['aws_access_key_id'], aws_secret_access_key=fields['aws_secret_access_key'],
            verify=cfg['ssl_ca'], config=Config(signature_version='s3v4', connect_timeout=5, read_timeout=15,
                s3={'addressing_style': 'path'}, retries={'max_attempts': 2},
                request_checksum_calculation='when_required', response_checksum_validation='when_required'))

    def original(self, row):
        digest = row['image_checksum']
        if (row['bucket'] != self.bucket or not HEX.fullmatch(digest)
                or not re.fullmatch('images/sha256/' + digest[:2] + '/' + digest + r'\.(png|jpg|jpeg|webp)', row['object_key'])):
            raise ServerCatalogError('IMAGE_OBJECT_IDENTITY_INVALID')
        response = self.client.get_object(Bucket=self.bucket, Key=row['object_key'])
        try:
            if response['ContentLength'] > 15 * 1024 * 1024:
                raise ServerCatalogError('IMAGE_TOO_LARGE')
            raw = response['Body'].read(15 * 1024 * 1024 + 1)
        finally:
            response['Body'].close()
        if len(raw) > 15 * 1024 * 1024 or checksum(raw) != digest:
            raise ServerCatalogError('IMAGE_CHECKSUM_MISMATCH')
        return raw

    def image(self, row):
        return png_bytes(self.original(row), allow_webp=True)[0]


class DeltaVectors:
    """Read a fixed Delta export, never contact Spark during user requests."""
    def __init__(self, path, config):
        path = Path(path).resolve()
        self.manifest = read_json(path)
        m = self.manifest
        if (m.get('contract') != VECTOR_CONTRACT or (m['model_version'], m['preprocessing_version'], m['dimension']) !=
                (config['embedding_model'], config['preprocessing_version'], config['dimension'])):
            raise ServerCatalogError('VECTOR_EXPORT_CONTRACT_MISMATCH')
        def checked(name, digest):
            target = (path.parent / name).resolve()
            if not target.is_relative_to(path.parent):
                raise ServerCatalogError('VECTOR_PATH_ESCAPE')
            with target.open('rb') as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != digest:
                    raise ServerCatalogError('VECTOR_EXPORT_CHECKSUM_MISMATCH')
            return target
        self.vectors = np.load(checked(m['vectors_file'], m['vectors_checksum']), mmap_mode='r', allow_pickle=False)
        if self.vectors.dtype != np.float32 or self.vectors.shape != (m['count'], m['dimension']):
            raise ServerCatalogError('VECTOR_EXPORT_SHAPE_INVALID')
        self.rows = {}
        with checked(m['mapping_file'], m['mapping_checksum']).open(encoding='utf-8') as stream:
            for n, line in enumerate(stream):
                row = json.loads(line)
                if row['row'] != n or row['embedding_id'] in self.rows or not HEX.fullmatch(row['image_checksum']):
                    raise ServerCatalogError('VECTOR_MAPPING_INVALID')
                self.rows[row['embedding_id']] = row
        if len(self.rows) != m['count']:
            raise ServerCatalogError('VECTOR_MAPPING_INCOMPLETE')

    def vector(self, row):
        meta = self.rows.get(row['embedding_id'])
        ref = parsed(row['vector_ref_json'])
        m = self.manifest
        if (not meta or meta['image_checksum'] != row['image_checksum']
                or ref.get('embedding_id') != row['embedding_id'] or ref.get('version') != m['delta_version']
                or ref.get('table') != 'silver.product_image_vectors' or ref.get('path') != m['delta_path']):
            raise ServerCatalogError('VECTOR_REFERENCE_MISMATCH')
        vector = self.vectors[meta['row']]
        if not np.isfinite(vector).all() or not np.isclose(np.linalg.norm(vector), 1, atol=1e-4):
            raise ServerCatalogError('VECTOR_INVALID')
        return vector


@contextmanager
def output_matrix(path, shape):
    matrix = np.lib.format.open_memmap(path, mode='w+', dtype=np.float32, shape=shape)
    try:
        yield matrix
        matrix.flush()
    finally:
        # Windows cannot remove a failed build while its mmap is still open.
        matrix._mmap.close()


def product(row, plan_id, database):
    if row['source_code'] != 'danawa' or not re.fullmatch(r'[0-9]+', row['source_product_id']):
        raise ServerCatalogError('PRODUCT_SOURCE_UNSUPPORTED')
    attributes = parsed(row['attributes_json'], list)
    status = parsed(row['field_status_json'])
    values = {k: float(row[k]) if row[k] is not None and status.get(k) == 'CONFIRMED' else None
              for k in ('width_mm', 'depth_mm', 'height_mm', 'price_krw')}
    values['price_krw'] = float(row['price_krw']) if row['price_krw'] is not None else None
    value = Product(image_id=row['image_uuid'], product_id='danawa:' + row['source_product_id'], name=row['name'],
        category_code=row['category_code'], image='images/' + row['image_checksum'] + '.png',
        image_checksum=row['image_checksum'], attributes_json=attributes,
        metadata_source=f"mysql:{database}/products/{row['product_uuid']}#publication={plan_id}",
        comparison_url=row['comparison_url'] if row['comparison_link_ready'] else None, **values)
    profile = product_profile(value)
    return value.model_copy(update={k: vals[0] for k, vals in (
        ('color', profile.palette), ('material', profile.materials), ('shape', profile.shapes)) if len(vals) == 1})


def export_snapshot(source, vectors, objects, output):
    """Build into a private sibling; expose products.json only after full validation."""
    output = Path(output).resolve()
    if output.exists():
        raise ServerCatalogError('IMMUTABLE_OUTPUT_ALREADY_EXISTS')
    if not source.health()['has_products']:
        raise ServerCatalogError('MYSQL_CATALOG_EMPTY')
    active = source.index()
    index_manifest = parsed(active['manifest_json'])
    m = vectors.manifest
    if index_manifest.get('vector_export_checksum') != checksum(canonical(m).encode()):
        raise ServerCatalogError('ACTIVE_INDEX_VECTOR_EXPORT_MISMATCH')
    chosen, plans = {}, set()
    for row in source.rows():
        if not eligible(row):
            continue
        plan_id = parsed(row['gold_snapshot_ref_json'])['plan_id']
        if plan_id not in plans:
            if not source.publication_complete(plan_id):
                raise ServerCatalogError('PUBLICATION_NOT_COMPLETE')
            plans.add(plan_id)
        ref = parsed(row['vector_ref_json'])
        if row['embedding_id'] not in vectors.rows or ref.get('version') != m['delta_version']:
            continue  # A historical embedding outside the fixed export is not a candidate.
        vectors.vector(row)  # Corrupt or mismatched vectors must fail, not disappear.
        if row['image_uuid'] in chosen:
            raise ServerCatalogError('AMBIGUOUS_EMBEDDING_FOR_IMAGE')
        chosen[row['image_uuid']] = row
    if not chosen:
        raise ServerCatalogError('NO_READY_PRODUCT_IMAGES')
    if hasattr(source, 'finish_snapshot'):
        source.finish_snapshot()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.catalog-build-', dir=output.parent) as temporary:
        temp = Path(temporary)
        products, bindings = [], {}
        with output_matrix(temp / 'vectors.npy', (len(chosen), m['dimension'])) as matrix:
            for n, row in enumerate(chosen.values()):
                plan_id = parsed(row['gold_snapshot_ref_json'])['plan_id']
                p = product(row, plan_id, source.config['mysql']['database'])
                image = temp / p.image
                if not image.exists():
                    write_bytes(image, objects.image(row))
                p = p.model_copy(update={'image_checksum': checksum(image.read_bytes())})
                products.append(p.model_dump())
                bindings[p.image_id] = binding(row, active['id'], active['manifest_json'])
                matrix[n] = vectors.vector(row)
        with (temp / 'vectors.npy').open('rb') as stream:
            vector_checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
        data = dict(embedding_model=m['model_version'], preprocessing_version=m['preprocessing_version'],
            dimension=m['dimension'], vectors_file='vectors.npy', vectors_checksum=vector_checksum, products=products,
            server_catalog=dict(contract=CONTRACT, index_id=active['id'], bindings=bindings,
                publication_plan_ids=sorted(plans), delta_path=m['delta_path'], delta_version=m['delta_version']))
        write_json(temp / 'products.json', data)
        ProductIndex.load(temp / 'products.json', server_config=source.config).validate_settings(_settings(source.config))
        # Rename a nested final directory; TemporaryDirectory still removes failed builds.
        ready = temp / 'ready'
        ready.mkdir()
        for item in list(temp.iterdir()):
            if item != ready:
                shutil.move(str(item), str(ready / item.name))
        ready.rename(output)
    return output / 'products.json'


def _settings(config):
    from types import SimpleNamespace
    return SimpleNamespace(embedding_model=config['embedding_model'], dimension=config['dimension'],
                           preprocessing_version=config['preprocessing_version'])


def registration_plan(source, vectors):
    """Return proposed registry rows for review. Never execute INSERT/UPDATE."""
    from uuid import NAMESPACE_URL, uuid5
    m = vectors.manifest
    by_checksum = {}
    for meta in vectors.rows.values():
        if meta['image_checksum'] in by_checksum:
            raise ServerCatalogError('AMBIGUOUS_VECTOR_IDENTITY')
        by_checksum[meta['image_checksum']] = meta
    records, seen_plans = {}, set()
    created_at = datetime.now(timezone.utc).isoformat()
    skipped = 0
    for image in source.bootstrap_images():
        meta = by_checksum.get(image['image_checksum'])
        if (not meta or image['product_status'] != 'ACTIVE' or not image['catalog_ready']
                or not image['image_active'] or image['asset_status'] != 'AVAILABLE'
                or not approved(image['usage_review_json'], 'EMBEDDING')
                or not approved(image['usage_policy_json'], 'EMBEDDING')):
            skipped += 1
            continue
        plan = parsed(image['gold_snapshot_ref_json'])['plan_id']
        if plan not in seen_plans:
            if not source.publication_complete(plan):
                raise ServerCatalogError('PUBLICATION_NOT_COMPLETE')
            seen_plans.add(plan)
        registry_id = uuid5(NAMESPACE_URL, CONTRACT + ':' + image['asset_id'] + ':' + meta['embedding_id']).hex
        record = dict(id=registry_id, asset_id=image['asset_id'], image_checksum=image['image_checksum'],
            model_version=m['model_version'], preprocessing_version=m['preprocessing_version'], dimension=m['dimension'],
            vector_ref_json=dict(table='silver.product_image_vectors', path=m['delta_path'], version=m['delta_version'],
                                 embedding_id=meta['embedding_id']), status='READY', created_at=created_at)
        if image['asset_id'] in records and records[image['asset_id']] != record:
            raise ServerCatalogError('ASSET_VECTOR_IDENTITY_CONFLICT')
        records[image['asset_id']] = record
    if not records:
        raise ServerCatalogError('NO_APPROVED_EMBEDDING_REGISTRATIONS')
    vector_export_checksum = checksum(canonical(m).encode())
    index_id = uuid5(NAMESPACE_URL, CONTRACT + ':' + source.config['scope'] + ':' + vector_export_checksum).hex
    index = dict(id=index_id, scope=source.config['scope'], model_version=m['model_version'],
        preprocessing_version=m['preprocessing_version'], dimension=m['dimension'], status='READY', activated_at=None, created_at=created_at,
        manifest_json=dict(contract=CONTRACT, vector_export_checksum=vector_export_checksum,
            delta_path=m['delta_path'], delta_version=m['delta_version'], vectors_checksum=m['vectors_checksum'],
            mapping_checksum=m['mapping_checksum']))
    return dict(status='REGISTRATION_PLAN_ONLY', database_writes=0, skipped_images=skipped,
        product_embeddings=list(records.values()), search_index_version=index,
        required_followups=['owner-reviewed registry writes', 'explicit atomic index activation',
                            'Gold readiness reevaluation and publication'])


def prepare_images(source, objects, output):
    """Prepare approved originals for the existing resumable embedding CLI."""
    output = Path(output).resolve()
    if output.exists():
        raise ServerCatalogError('IMMUTABLE_IMAGE_PREPARATION_EXISTS')
    selected, plans = {}, set()
    for image in source.bootstrap_images():
        if (image['product_status'] != 'ACTIVE' or not image['catalog_ready'] or not image['image_active']
                or image['asset_status'] != 'AVAILABLE' or not approved(image['usage_review_json'], 'EMBEDDING')
                or not approved(image['usage_policy_json'], 'EMBEDDING')):
            continue
        plan = parsed(image['gold_snapshot_ref_json'])['plan_id']
        if plan not in plans:
            if not source.publication_complete(plan):
                raise ServerCatalogError('PUBLICATION_NOT_COMPLETE')
            plans.add(plan)
        selected.setdefault(image['image_checksum'], image)
    if not selected:
        raise ServerCatalogError('NO_APPROVED_EMBEDDING_IMAGES')
    if hasattr(source, 'finish_snapshot'):
        source.finish_snapshot()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.images-build-', dir=output.parent) as temporary:
        ready = Path(temporary) / 'ready'
        (ready / 'images').mkdir(parents=True)
        refs = []
        for image in selected.values():
            raw = objects.original(image)
            extension = Path(image['object_key']).suffix.lower()
            name = image['image_checksum'] + extension
            write_bytes(ready / 'images' / name, raw)
            refs.append(dict(image_checksum=image['image_checksum'], bucket=image['bucket'],
                object_key=image['object_key'], asset_id=image['asset_id'], file='images/' + name))
        write_json(ready / 'manifest.json', dict(contract='mysql-image-preparation-v1', images=refs,
            publication_plan_ids=sorted(plans), api_calls=0))
        ready.rename(output)
    return dict(status='IMAGES_PREPARED', images=len(selected), manifest=str(output / 'manifest.json'), api_calls=0)


def main(argv=None):
    parser = argparse.ArgumentParser(description='MySQL·Garage·Delta export 읽기 전용 카탈로그 연결')
    parser.add_argument('command', choices=['check', 'prepare-images', 'registration-plan', 'export'])
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--vectors', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    try:
        cfg = configuration(args.config)
        with connect(cfg) as source:
            if args.command == 'check':
                print(canonical(dict(status='MYSQL_READABLE', **source.health())))
            elif args.command == 'prepare-images':
                if args.output is None:
                    raise ServerCatalogError('OUTPUT_REQUIRED')
                print(canonical(prepare_images(source, Garage(cfg), args.output)))
            elif args.command == 'registration-plan':
                if args.vectors is None:
                    raise ServerCatalogError('VECTOR_EXPORT_REQUIRED')
                print(canonical(registration_plan(source, DeltaVectors(args.vectors, cfg))))
            else:
                if args.vectors is None or args.output is None:
                    raise ServerCatalogError('VECTOR_EXPORT_AND_OUTPUT_REQUIRED')
                print(canonical(dict(status='EXPORTED', products=str(export_snapshot(
                    source, DeltaVectors(args.vectors, cfg), Garage(cfg), args.output)))))
        return 0
    except Exception as exc:
        print(canonical(dict(status='FAILED', error_code=str(exc) if isinstance(exc, ServerCatalogError) else type(exc).__name__)))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
