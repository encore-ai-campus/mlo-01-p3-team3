import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest

from interior_recommender.assets import checksum, write_json
from interior_recommender.catalog import server
from interior_recommender.catalog.server import (CONTRACT, DeltaVectors, LiveGuard, ServerCatalogError,
    approved, binding, canonical, configuration, connect, eligible, export_snapshot, product, prepare_images, registration_plan)
from interior_recommender.domain.recommendation import ProductIndex, RecommendationContext
from interior_recommender.tests.test_pipeline import image_bytes


@pytest.fixture
def fixture(tmp_path):
    image = image_bytes()
    digest = checksum(image)
    cfg = dict(contract=CONTRACT, embedding_model='gemini-embedding-2', dimension=768,
               preprocessing_version='exif-rgb-fit1024-png-l2-v1', scope='PRODUCT_IMAGES', mysql={'database': 'test'})
    vec = tmp_path / 'delta-export'
    vec.mkdir()
    matrix = np.zeros((1, 768), dtype=np.float32)
    matrix[0, 0] = 1
    np.save(vec / 'vectors.npy', matrix)
    meta = dict(row=0, embedding_id='e'*32, image_checksum=digest, preprocessed_checksum=digest, record_hash='f'*64)
    (vec / 'mapping.jsonl').write_text(json.dumps(meta) + '\n', encoding='utf-8')
    manifest = dict(contract='delta-image-vectors-export-v1', delta_path='s3a://pj3-data/silver/product_image_vectors',
        delta_version=1, model_version=cfg['embedding_model'], preprocessing_version=cfg['preprocessing_version'],
        dimension=768, count=1, vectors_file='vectors.npy', vectors_checksum=checksum((vec / 'vectors.npy').read_bytes()),
        mapping_file='mapping.jsonl', mapping_checksum=checksum((vec / 'mapping.jsonl').read_bytes()))
    write_json(vec / 'manifest.json', manifest)
    expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    review = dict(decision='APPROVED', scope='RECOMMENDATION', evidence_ref='fixture://owner', expires_at=expiry)
    row = dict(product_uuid='1'*32, product_key='danawa:11', source_code='danawa', source_product_id='11', name='흰 의자',
        width_mm=500, depth_mm=None, height_mm=None, price_krw=10000, comparison_url='https://prod.danawa.com/info/?pcode=11',
        attributes_json=[dict(attribute_name='COLOR', codes=['white'], attribute_status='CONFIRMED', measurement_target='PRODUCT',
            option_status='NO_OPTION_CONFIRMED', source_evidence_key='fixture:color')],
        field_status_json={'width_mm': 'CONFIRMED'}, field_lineage_json={}, gold_snapshot_ref_json={'plan_id': 'a'*64},
        product_status='ACTIVE', catalog_ready=1, recommendation_ready=1, comparison_link_ready=1, representative_image_id='2'*32,
        category_code='CHAIR', category_active=1, recognition_enabled=1, image_uuid='2'*32, asset_id='3'*32, image_active=1,
        gold_recommendation_approved=1, usage_review_json=review, source_version='source-v1', image_relation_key='relation-v1',
        bucket='pj3-data', object_key=f'images/sha256/{digest[:2]}/{digest}.png', image_checksum=digest, asset_status='AVAILABLE',
        usage_policy_json=review, embedding_id='e'*32, embedding_checksum=digest, model_version=cfg['embedding_model'],
        preprocessing_version=cfg['preprocessing_version'], dimension=768, embedding_status='READY',
        vector_ref_json=dict(table='silver.product_image_vectors', path=manifest['delta_path'], version=1, embedding_id='e'*32))
    index = dict(id='4'*32, manifest_json={'vector_export_checksum': checksum(canonical(manifest).encode())})
    source = SimpleNamespace(config=cfg, health=lambda: {'has_products': 1}, index=lambda: index,
                             rows=lambda *args: iter([row]), publication_complete=lambda p: True)
    objects = SimpleNamespace(image=lambda r: image)
    return SimpleNamespace(config=cfg, image=image, row=row, vectors=DeltaVectors(vec / 'manifest.json', cfg),
                           manifest=manifest, source=source, objects=objects, index=index)


def test_server_snapshot_runs_existing_cpu_recommendation_and_preserves_binding(fixture, tmp_path, monkeypatch):
    f = fixture
    path = export_snapshot(f.source, f.vectors, f.objects, tmp_path / 'catalog')
    index = ProductIndex.load(path, server_config=f.config)
    guard = MagicMock()
    index.live_guard = guard
    result = index.rank([1] + [0]*767, RecommendationContext(decision='ACCEPTED', category_code='CHAIR'))
    assert result['items'][0]['product_id'] == 'danawa:11'
    assert result['items'][0]['metadata']['width_mm'] == 500
    assert result['items'][0]['metadata']['color'] == 'white'
    assert result['items'][0]['server_binding']['asset_id'] == '3'*32
    assert result['catalog_source']['delta_version'] == 1
    guard.validate.assert_called_once()
    empty = index.rank([1] + [0]*767, RecommendationContext(decision='ACCEPTED', category_code='SOFA'))
    assert empty['items'] == [] and empty['unverified_items'] == []
    guard.validate_index.assert_called_once()
    with pytest.raises(ServerCatalogError, match='IMMUTABLE_OUTPUT'):
        export_snapshot(f.source, f.vectors, f.objects, tmp_path / 'catalog')


@pytest.mark.parametrize('field,value', [('image_active', 0), ('recommendation_ready', 0),
    ('gold_recommendation_approved', 0), ('asset_status', 'MISSING'), ('embedding_checksum', '0'*64)])
def test_inactive_unapproved_or_mismatched_images_are_not_served(fixture, field, value):
    assert not eligible({**fixture.row, field: value})


def test_pending_owner_policy_is_not_autoapproved(fixture):
    assert not eligible({**fixture.row, 'usage_policy_json': {'source': 'OWNER_REVIEW_REQUIRED'}})
    assert not approved({'decision': 'APPROVED', 'scope': 'EMBEDDING', 'evidence_ref': 'fixture'}, 'RECOMMENDATION')


def test_vector_reference_version_and_checksum_must_match(fixture):
    f = fixture
    with pytest.raises(ServerCatalogError, match='VECTOR_REFERENCE_MISMATCH'):
        f.vectors.vector({**f.row, 'vector_ref_json': {**f.row['vector_ref_json'], 'version': 0}})
    with pytest.raises(ServerCatalogError, match='VECTOR_REFERENCE_MISMATCH'):
        f.vectors.vector({**f.row, 'image_checksum': '0'*64})


def test_empty_database_is_explicit_failure_and_creates_no_catalog(fixture, tmp_path):
    f = fixture
    f.source.health = lambda: {'has_products': 0}
    with pytest.raises(ServerCatalogError, match='MYSQL_CATALOG_EMPTY'):
        export_snapshot(f.source, f.vectors, f.objects, tmp_path / 'catalog')
    assert not (tmp_path / 'catalog').exists()


def test_failed_image_download_removes_partial_build(fixture, tmp_path):
    f = fixture
    def fail(row):
        raise ServerCatalogError('IMAGE_CHECKSUM_MISMATCH')
    with pytest.raises(ServerCatalogError, match='IMAGE_CHECKSUM_MISMATCH'):
        export_snapshot(f.source, f.vectors, SimpleNamespace(image=fail), tmp_path / 'catalog')
    assert not list(tmp_path.glob('.catalog-build-*'))
    assert not (tmp_path / 'catalog').exists()


def test_current_metadata_change_or_revoked_review_blocks_return(fixture, monkeypatch):
    from contextlib import contextmanager
    f = fixture
    expected = binding(f.row, f.index['id'], f.index['manifest_json'])
    @contextmanager
    def source(config):
        yield f.source
    monkeypatch.setattr(server, 'connect', source)
    guard = LiveGuard(f.config)
    guard.validate([expected])
    f.row['price_krw'] = 9000
    with pytest.raises(ServerCatalogError, match='CURRENT_PRODUCT_OR_INDEX_CHANGED'):
        guard.validate([expected])
    f.row['price_krw'] = 10000
    f.row['usage_review_json']['decision'] = 'REVOKED'
    with pytest.raises(ServerCatalogError, match='CURRENT_PRODUCT_OR_INDEX_CHANGED'):
        guard.validate([expected])


def test_source_options_are_not_promoted_to_product_properties(fixture):
    row = copy.deepcopy(fixture.row)
    row['attributes_json'][0]['option_status'] = 'UNKNOWN'
    row['field_status_json'] = {}
    p = product(row, 'a'*64, 'test')
    assert p.color is None and p.width_mm is None


def test_mysql_session_is_read_only_and_rolls_back_even_on_failure(monkeypatch):
    import pymysql
    db = MagicMock()
    monkeypatch.setattr(pymysql, 'connect', MagicMock(return_value=db))
    cfg = {'mysql': dict(host='host', user='reader', password='never-output', database='test', ssl_ca='ca.pem')}
    with pytest.raises(RuntimeError):
        with connect(cfg):
            raise RuntimeError('fail')
    sql = [c.args[0] for c in db.cursor.return_value.__enter__.return_value.execute.call_args_list]
    assert 'SET SESSION transaction_read_only = ON' in sql
    assert 'START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY' in sql
    assert not any(s.startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'GRANT')) for s in sql)
    db.commit.assert_not_called()
    db.rollback.assert_called_once()
    db.close.assert_called_once()
    assert pymysql.connect.call_args.kwargs['ssl_verify_identity'] is True


def test_database_error_does_not_expose_credentials(monkeypatch):
    import pymysql
    monkeypatch.setattr(pymysql, 'connect', MagicMock(side_effect=pymysql.OperationalError('password=never-output')))
    cfg = {'mysql': dict(host='host', user='reader', password='never-output', database='test', ssl_ca='ca.pem')}
    with pytest.raises(ServerCatalogError, match='^MYSQL_UNAVAILABLE$'):
        with connect(cfg):
            pass


def test_vector_mapping_tampering_is_rejected(fixture):
    f = fixture
    root = Path(f.vectors.vectors.filename).parent
    (root / 'mapping.jsonl').write_text('{}\n', encoding='utf-8')
    with pytest.raises(ServerCatalogError, match='CHECKSUM_MISMATCH'):
        DeltaVectors(root / 'manifest.json', f.config)


def test_bootstrap_does_not_require_recommendation_ready_or_generate_vectors(fixture, tmp_path):
    f = fixture
    f.row['recommendation_ready'] = 0
    f.row['usage_review_json']['scopes'] = ['EMBEDDING', 'RECOMMENDATION']
    f.source.bootstrap_images = lambda: iter([f.row, f.row])
    f.source.finish_snapshot = MagicMock()
    objects = SimpleNamespace(original=lambda r: f.image)
    result = prepare_images(f.source, objects, tmp_path / 'inputs')
    assert result['images'] == 1 and result['api_calls'] == 0
    f.source.finish_snapshot.assert_called_once()
    plan = registration_plan(f.source, f.vectors)
    assert plan['database_writes'] == 0 and len(plan['product_embeddings']) == 1
    assert plan['search_index_version']['status'] == 'READY'
    assert plan['search_index_version']['activated_at'] is None
    # Registry ID is asset-specific; the underlying Delta embedding can be shared.
    assert plan['product_embeddings'][0]['id'] != plan['product_embeddings'][0]['vector_ref_json']['embedding_id']


def test_recommendation_approval_alone_does_not_permit_embedding_or_generation(fixture, tmp_path, monkeypatch):
    f = fixture
    f.source.bootstrap_images = lambda: iter([f.row])
    with pytest.raises(ServerCatalogError, match='NO_APPROVED_EMBEDDING_IMAGES'):
        prepare_images(f.source, SimpleNamespace(original=MagicMock()), tmp_path / 'inputs')
    with pytest.raises(ServerCatalogError, match='NO_APPROVED_EMBEDDING_REGISTRATIONS'):
        registration_plan(f.source, f.vectors)
    from contextlib import contextmanager
    @contextmanager
    def source(config):
        yield f.source
    monkeypatch.setattr(server, 'connect', source)
    with pytest.raises(ServerCatalogError, match='IMAGE_USAGE_NOT_APPROVED'):
        LiveGuard(f.config).validate([binding(f.row, f.index['id'], f.index['manifest_json'])], scope='GENERATION_REFERENCE')


def test_registry_identity_and_delta_identity_are_decoded_separately(fixture):
    raw = copy.deepcopy(fixture.row)
    raw['embedding_id'] = '5'*32
    decoded = server.MySQLSource.vector_identity(raw)
    assert decoded['embedding_registry_id'] == '5'*32
    assert decoded['embedding_id'] == 'e'*32


def test_garage_uses_existing_java_properties_and_verifies_original_bytes(fixture, tmp_path, monkeypatch):
    import boto3
    credentials = tmp_path / 'spark.properties'
    credentials.write_text('accessKey=fixture-access\nsecretKey=fixture-secret\n', encoding='utf-8')
    credentials.chmod(0o600)
    client = MagicMock()
    client.get_object.return_value = {'ContentLength': len(fixture.image), 'Body': __import__('io').BytesIO(fixture.image)}
    connect_client = MagicMock(return_value=client)
    monkeypatch.setattr(boto3, 'client', connect_client)
    ca = tmp_path / 'ca.pem'
    ca.write_text('fixture CA', encoding='utf-8')
    config = {'s3': dict(endpoint='https://storage.example:8443', bucket='pj3-data', ssl_ca=str(ca), credentials_file=str(credentials))}
    objects = server.Garage(config)
    assert objects.original(fixture.row) == fixture.image
    options = connect_client.call_args.kwargs
    assert options['aws_access_key_id'] == 'fixture-access'
    assert options['verify'] == str(ca)
    client.get_object.return_value = {'ContentLength': 3, 'Body': __import__('io').BytesIO(b'bad')}
    with pytest.raises(ServerCatalogError, match='IMAGE_CHECKSUM_MISMATCH'):
        objects.original(fixture.row)
    config['s3']['ssl_ca'] = False
    with pytest.raises(ServerCatalogError, match='S3_CA_FILE_REQUIRED'):
        server.Garage(config)


def test_same_index_id_with_changed_manifest_is_not_silently_reused(fixture, monkeypatch):
    from contextlib import contextmanager
    f = fixture
    expected = binding(f.row, f.index['id'], f.index['manifest_json'])
    @contextmanager
    def source(config):
        yield f.source
    monkeypatch.setattr(server, 'connect', source)
    f.index['manifest_json']['changed'] = True
    with pytest.raises(ServerCatalogError, match='CURRENT_PRODUCT_OR_INDEX_CHANGED'):
        LiveGuard(f.config).validate([expected])
    with pytest.raises(ServerCatalogError, match='CURRENT_INDEX_CHANGED'):
        LiveGuard(f.config).validate_index(expected['index_id'], expected['index_manifest_checksum'])
