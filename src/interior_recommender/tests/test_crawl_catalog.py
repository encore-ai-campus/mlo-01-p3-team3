import json

import numpy as np
import pytest

from interior_recommender.assets import checksum, read_json
from interior_recommender.batch_embedding import BatchEmbedding, create_batch
from interior_recommender.config import Settings
from interior_recommender.crawl_catalog import export, prepare
from interior_recommender.recommendation import ProductIndex, RecommendationContext
from interior_recommender.tests.test_pipeline import FakeAdapter, image_bytes


def save_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


@pytest.fixture
def crawl(tmp_path):
    source = tmp_path / "crawl"
    media = source / "media"
    media.mkdir(parents=True)
    data = image_bytes()
    (media / "shared.png").write_bytes(data)
    products, raw, enriched = [], [], []
    for pid, category in (("1", "식탁의자"), ("2", "소파"), ("3", "책상")):
        row = {"source": "danawa", "product_id": pid, "name": "상품" + pid,
               "categories": [{"category_name": category}], "raw_payload_hash": "hash-" + pid,
               "specifications": None, "images": [], "product_url": f"https://prod.danawa.com/info/?pcode={pid}"}
        products.append(row)
        raw.append({"source": "danawa", "product_id": pid, "payload_hash": row["raw_payload_hash"], "payload": {}})
        image = {"source": "danawa", "product_id": pid, "role": "representative", "order": 0,
                 "status": "downloaded", "local_path": "shared.png", "sha256": checksum(data)}
        enriched.append({**row, "images": [image] if pid != "3" else []})
    save_jsonl(source / "products.jsonl", products)
    save_jsonl(source / "product_raw.jsonl", raw)
    save_jsonl(media / "products_with_local_images.jsonl", enriched)
    return source, tmp_path / "work"


def embed_fixture(work, limit=None):
    settings = Settings()
    directory = create_batch(work / "embedding_inputs", work / "embeddings", settings)
    adapter = FakeAdapter()
    BatchEmbedding(directory, lambda _: adapter).run(work / "embedding_inputs", settings, limit=limit,
                                                    requests_per_minute=1000000, progress=lambda _: None)
    return adapter


def test_shared_crawl_image_is_embedded_once_but_keeps_both_products(crawl):
    source, work = crawl
    prepared = prepare(source, work, limit=None)
    assert prepared["report"]["selected_products"] == 2
    assert prepared["report"]["unique_images"] == 1
    assert prepared["report"]["products_without_images"] == ["3"]
    assert prepare(source, work, limit=None) == prepared
    adapter = embed_fixture(work)
    assert adapter.calls["embed"] == 1
    manifest = export(work)
    catalog = ProductIndex.load(manifest)
    assert {p.product_id for p in catalog.products} == {"danawa:1", "danawa:2"}
    assert len({p.image_id for p in catalog.products}) == 2
    np.testing.assert_array_equal(catalog.vectors[0], catalog.vectors[1])
    for category, pid in (("CHAIR", "danawa:1"), ("SOFA", "danawa:2")):
        result = catalog.rank(catalog.vectors[0], RecommendationContext(decision="ACCEPTED", category_code=category))
        assert [row["product_id"] for row in result["items"]] == [pid]
    assert export(work) == manifest
    linked = ProductIndex.load(export(work, with_links=True))
    assert [p.comparison_url for p in linked.products] == [f"https://prod.danawa.com/info/?pcode={p.product_id.split(':')[1]}" for p in linked.products]
    np.testing.assert_array_equal(linked.vectors, catalog.vectors)
    assert adapter.calls["embed"] == 1
    assert export(work, with_links=True).parent.name == "catalog-with-links"


def test_prepare_refuses_modified_source_or_prepared_images(crawl):
    source, work = crawl
    prepared = prepare(source, work)
    with pytest.raises(ValueError, match="different inputs"):
        prepare(source, work, limit=1)
    (work / prepared["images"][0]["input_file"]).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="Prepared image changed"):
        prepare(source, work)


def test_prepare_rejects_mismatched_product_raw_identity(crawl):
    source, work = crawl
    raw = (source / "product_raw.jsonl").read_text(encoding="utf-8")
    (source / "product_raw.jsonl").write_text(raw.replace("hash-1", "wrong-hash"), encoding="utf-8")
    with pytest.raises(ValueError, match="raw payload hash mismatch"):
        prepare(source, work)
    assert not work.exists()


def test_export_never_uses_old_or_missing_embedding_directories(crawl):
    source, work = crawl
    prepare(source, work)
    with pytest.raises(ValueError, match="not ready"):
        export(work)
    assert not (work / "catalog").exists()


def test_category_selection_and_gallery_fallback_are_explicit(crawl):
    source, work = crawl
    path = source / "media/products_with_local_images.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[0]["images"][0]["role"] = "gallery"
    save_jsonl(path, rows)
    prepared = prepare(source, work, categories=["CHAIR"], limit=1)
    assert prepared["report"]["selected_categories"] == {"CHAIR": 1}
    assert prepared["products"][0]["role"] == "gallery"
