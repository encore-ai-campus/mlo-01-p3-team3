import io
import json

import pytest
from PIL import Image

from interior_recommender.assets import checksum, read_json
from interior_recommender.catalog.batch_embedding import BatchEmbedding, create_batch
from interior_recommender.catalog.server_embeddings import completed, convert, pixel_checksum
from interior_recommender.config import Settings
from interior_recommender.tests.test_pipeline import FakeAdapter


def test_webp_original_checksum_and_completed_batch_are_preserved_for_delta(tmp_path):
    raw = io.BytesIO()
    Image.new('RGB', (80, 60), 'white').save(raw, format='WEBP')
    source = tmp_path / 'images'
    source.mkdir()
    (source / 'product.webp').write_bytes(raw.getvalue())
    batch = create_batch(source, tmp_path / 'batch', Settings())
    adapter = FakeAdapter()
    BatchEmbedding(batch, lambda _: adapter).run(source, Settings(), progress=lambda _: None)
    before = dict(adapter.calls)
    values = list(completed(batch / 'index.json'))
    assert len(values) == 1 and values[0]['image_checksum'] == checksum(raw.getvalue())
    assert values[0]['image_checksum'] != values[0]['preprocessed_checksum']
    result = convert(batch / 'index.json', tmp_path / 'for-delta.jsonl')
    assert result['api_calls'] == 0 and dict(adapter.calls) == before
    payload = json.loads((tmp_path / 'for-delta.jsonl').read_text())
    assert payload['dimension'] == 768
    assert len(payload['preprocessed_pixel_checksum']) == 64
    assert payload['preprocessed_pixel_checksum'] == pixel_checksum(
        (batch / 'inputs' / f"{payload['image_checksum']}.png").read_bytes())
    with pytest.raises(ValueError, match='IMMUTABLE_IMPORT_FILE_EXISTS'):
        convert(batch / 'index.json', tmp_path / 'for-delta.jsonl')
    index = read_json(batch / 'index.json')
    (batch / index['files'][0]['embedding_file']).write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError, match='BATCH_OUTPUT_IDENTITY_MISMATCH'):
        list(completed(batch / 'index.json'))


def test_pixel_checksum_ignores_png_compression_bytes_but_checks_pixels():
    first, second, changed = io.BytesIO(), io.BytesIO(), io.BytesIO()
    Image.new('RGB', (32, 24), (20, 80, 140)).save(first, format='PNG', compress_level=1)
    Image.new('RGB', (32, 24), (20, 80, 140)).save(second, format='PNG', compress_level=9)
    Image.new('RGB', (32, 24), (20, 80, 141)).save(changed, format='PNG', compress_level=1)
    assert checksum(first.getvalue()) != checksum(second.getvalue())
    assert pixel_checksum(first.getvalue()) == pixel_checksum(second.getvalue())
    assert pixel_checksum(first.getvalue()) != pixel_checksum(changed.getvalue())
