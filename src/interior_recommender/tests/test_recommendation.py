import json
from uuid import uuid4

import numpy as np
import pytest
from django.conf import settings as django_settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, override_settings

from interior_recommender.assets import checksum, read_json, write_json
from interior_recommender.config import Settings
from interior_recommender.pipeline import contract_hash
from interior_recommender.recommendation import (Condition, ProductIndex, RankingPolicy, RecommendationContext,
                                    parse_context, validate_context)
from interior_recommender.tests.test_pipeline import image_bytes
from interior_recommender.tests.test_web_workflow import post, wait
from interior_recommender.tests.test_danawa_references import saved_index
from interior_recommender.web import views
from interior_recommender.web.__main__ import configure
from interior_recommender.web.direct import DirectDemoAdapter, DirectStudio


def product(tmp_path, name, **metadata):
    image = tmp_path / (name + ".png")
    image.write_bytes(image_bytes())
    return {"image_id": name, "product_id": name, "name": name, "image": str(image),
            "image_checksum": checksum(image.read_bytes()), "category_code": "CHAIR", "metadata_source": "test", **metadata}


def index(rows, vectors):
    return ProductIndex(rows, np.asarray(vectors, dtype=np.float32), model="test", preprocessing="test", version="test-v1")


def context(**kwargs):
    return RecommendationContext(decision="ACCEPTED", category_code="CHAIR", **kwargs)


def test_initial_weights_change_actual_order_without_labels(tmp_path):
    catalog = index([product(tmp_path, "similar", color="white"), product(tmp_path, "preferred", color="brown")], [[1, 0], [.9, .43589]])
    result = catalog.rank([1, 0], context(preferred_color="brown"))
    assert [p["product_id"] for p in result["items"]] == ["preferred", "similar"]
    assert result["weight_source"] == "MANUAL_INITIAL"
    assert result["items"][0]["score"] == pytest.approx(.85 * .9 + .15)
    pure = catalog.rank([1, 0], context(preferred_color="brown"), RankingPolicy(similarity_weight=1, color_weight=0))
    assert pure["items"][0]["product_id"] == "similar"
    assert catalog.rank([1, 0], context())["items"][0]["score"] == pytest.approx(1)


def test_hard_limits_unknowns_negative_conditions_and_estimated_size(tmp_path):
    rows = [product(tmp_path, "match", width_mm=500, material="wood"),
            product(tmp_path, "wide", width_mm=700, material="wood"),
            product(tmp_path, "missing", material="wood"),
            product(tmp_path, "metal", width_mm=500, material="metal"),
            product(tmp_path, "sofa", category_code="SOFA", width_mm=500, material="wood")]
    c = context(required=[Condition(field="width_mm", operator="LE", value=600., source_quote="폭 60cm 이하"),
                          Condition(field="material", operator="NE", value="metal", source_quote="금속 제외")],
                size_estimate={"basis": "TYPICAL_OBJECT_SIZE", "estimated_width_mm": 100, "explanation": "약한 추정"})
    result = index(rows, [[1, 0]] * len(rows)).rank([1, 0], c)
    assert [p["product_id"] for p in result["items"]] == ["match"]
    assert [p["product_id"] for p in result["unverified_items"]] == ["missing"]
    assert result["excluded_images"] == 3
    assert result["items"][0]["size_fit"] == "UNKNOWN"


def test_missing_preference_does_not_gain_weight_and_unsupported_is_unverified(tmp_path):
    result = index([product(tmp_path, "unknown"), product(tmp_path, "match", color="brown")], [[1, 0], [1, 0]]).rank([1, 0], context(preferred_color="brown"))
    assert result["items"][0]["product_id"] == "match"
    assert result["items"][1]["score"] == pytest.approx(.85)
    result = index([product(tmp_path, "x")], [[1, 0]]).rank([1, 0], context(unapplied_conditions=["반려동물 안전성 필수"]))
    assert not result["items"] and len(result["unverified_items"]) == 1


def test_product_dedup_and_stable_ties(tmp_path):
    a = product(tmp_path, "image-a", product_id="product-a")
    b = {**a, "image_id": "image-b"}
    z = product(tmp_path, "product-z")
    result = index([z, b, a], [[1, 0]] * 3).rank([1, 0], context())
    assert [p["product_id"] for p in result["items"]] == ["product-a", "product-z"]
    assert result["items"][0]["image_id"] == "image-a"


@pytest.mark.parametrize("upper,group", [(900, "items"), (810, "items"), (700, "unverified_items"), (600, None)])
def test_adjustable_dimensions_only_match_when_entire_recorded_range_meets_limit(tmp_path, upper, group):
    row = product(tmp_path, "desk", height_mm=None,
                  dimension_ranges_mm={"height_mm": {"minimum": 675, "maximum": 810}},
                  component_dimensions_mm={"seat_width_mm": {"minimum": 400, "maximum": 400}})
    request = context(required=[Condition(field="height_mm", operator="LE", value=upper, source_quote="test")])
    result = index([row], [[1, 0]]).rank([1, 0], request)
    if group is None:
        assert result["excluded_images"] == 1 and not result["items"] and not result["unverified_items"]
    else:
        assert len(result[group]) == 1
    request = context(required=[Condition(field="width_mm", operator="LE", value=500, source_quote="test")])
    assert index([row], [[1, 0]]).rank([1, 0], request)["unverified_items"]


def test_scalar_dimensions_cannot_hide_a_conflicting_adjustable_range(tmp_path):
    row = product(tmp_path, "desk", height_mm=675,
                  dimension_ranges_mm={"height_mm": {"minimum": 675, "maximum": 810}})
    with pytest.raises(ValueError, match="conflicts"):
        index([row], [[1, 0]])


@pytest.mark.parametrize("policy", [{"similarity_weight": .9}, {"color_weight": -1}, {"similarity_weight": float("nan")}, {"color_weight": float("inf")}])
def test_invalid_weights_rejected(policy):
    with pytest.raises(ValueError):
        RankingPolicy(**policy)


@pytest.mark.parametrize("vector", [[0, 0], [float("nan"), 1], [1], [float("inf"), 1]])
def test_invalid_query_rejected(tmp_path, vector):
    with pytest.raises(ValueError):
        index([product(tmp_path, "x")], [[1, 0]]).rank(vector, context())


def test_context_requires_original_quote_and_matching_units():
    c = context(required=[Condition(field="width_mm", operator="LE", value=600, source_quote="폭 60cm 이하")])
    assert validate_context(c.model_dump(), "의자 폭 60cm 이하", ["CHAIR"]).required[0].value == 600
    with pytest.raises(ValueError, match="quote"):
        validate_context(c.model_dump(), "의자", ["CHAIR"])
    c.required[0].value = 6000.0
    with pytest.raises(ValueError, match="number/unit"):
        validate_context(c.model_dump(), "폭 60cm 이하", ["CHAIR"])


@pytest.mark.parametrize("space,region,expected", [
    ("normalized_1000_xyxy", {"x_min": 528, "y_min": 570, "x_max": 735, "y_max": 888},
     {"x_min": .528, "y_min": .570, "x_max": .735, "y_max": .888}),
    ("normalized_1_xyxy", {"x_min": .1, "y_min": .2, "x_max": .8, "y_max": .9},
     {"x_min": .1, "y_min": .2, "x_max": .8, "y_max": .9}),
    ("normalized_1000_xyxy", None, None),
])
def test_direct_context_converts_declared_coordinates_without_changing_receipt(space, region, expected):
    receipt = {"text": json.dumps({**context().model_dump(), "placement_region": region}),
               "coordinate_space": space}
    original = receipt["text"]
    parsed = parse_context(receipt, "", ["CHAIR"])
    assert parsed.model_dump()["placement_region"] == expected
    assert receipt["text"] == original


@pytest.mark.parametrize("value", [-1, 1001, True, float("nan"), float("inf")])
def test_direct_context_rejects_invalid_provider_coordinates(value):
    data = {**context().model_dump(), "placement_region":
            {"x_min": value, "y_min": 100, "x_max": 800, "y_max": 900}}
    with pytest.raises(ValueError):
        parse_context({"text": json.dumps(data), "coordinate_space": "normalized_1000_xyxy"}, "", ["CHAIR"])


def test_direct_context_never_guesses_coordinate_units_or_accepts_ocr_as_text_quote():
    data = {**context().model_dump(), "placement_region":
            {"x_min": 528, "y_min": 570, "x_max": 735, "y_max": 888}}
    with pytest.raises(ValueError):
        parse_context({"text": json.dumps(data)}, "", ["CHAIR"])
    with pytest.raises(ValueError, match="coordinate space"):
        parse_context({"text": json.dumps(data), "coordinate_space": "pixels"}, "", ["CHAIR"])
    data["required"] = [{"field": "color", "operator": "EQ", "value": "black", "source_quote": "검은색 의자"}]
    with pytest.raises(ValueError, match="quote"):
        parse_context({"text": json.dumps(data), "coordinate_space": "normalized_1000_xyxy"}, "", ["CHAIR"])


def test_mmap_catalog_checksum_paths_and_consistency(tmp_path):
    rows = [product(tmp_path, "x")]
    rows[0]["image"] = "x.png"
    matrix = tmp_path / "vectors.npy"
    np.save(matrix, np.asarray([[1, 0]], dtype=np.float32))
    data = {"embedding_model": "test", "dimension": 2, "preprocessing_version": "test", "products": rows,
            "vectors_file": "vectors.npy", "vectors_checksum": checksum(matrix.read_bytes())}
    path = tmp_path / "products.json"
    write_json(path, data)
    assert ProductIndex.load(path).rank([1, 0], context())["items"][0]["product_id"] == "x"
    data["vectors_checksum"] = "0" * 64
    write_json(path, data)
    with pytest.raises(ValueError, match="checksum"):
        ProductIndex.load(path)
    data["products"][0]["image"] = "../outside.png"
    write_json(path, data)
    with pytest.raises(ValueError, match="escapes"):
        ProductIndex.load(path)
    with pytest.raises(ValueError, match="Conflicting"):
        index([product(tmp_path, "a", product_id="same", color="brown"), product(tmp_path, "b", product_id="same", color="white")], [[1, 0], [1, 0]])


class CountingAdapter(DirectDemoAdapter):
    calls = []

    def call(self, kind, *args, **kwargs):
        self.calls.append(kind)
        return super().call(kind, *args, **kwargs)


def test_direct_workflow_reuses_api_and_freezes_policy_without_catalog_copy(tmp_path):
    CountingAdapter.calls = []
    service = DirectStudio(tmp_path / "web", demo=True, adapter_factory=CountingAdapter)
    owner, event = str(uuid4()), str(uuid4())
    try:
        run = service.submit(image_bytes(), None, "의자", [], None, owner, event)
        assert service.submit(image_bytes(), None, "의자", [], None, owner, event) == run
        with pytest.raises(ValueError, match="입력"):
            service.submit(image_bytes(), None, "다른 의자", [], None, owner, event)
        service.launch(run, owner, "prepare")
        result = wait(service, run, owner)
        assert result["status"] == "COMPLETED" and len(result["recommendations"]["items"]) == 3
        assert CountingAdapter.calls == ["direct_context", "embed_text"]
        directory = service.directory(run, owner)
        manifest = read_json(directory / "manifest.json")
        assert manifest["templates"] == []
        assert not any("template" in k or "vector" in k for k in manifest["assets"])
        service.policy = RankingPolicy(similarity_weight=1, color_weight=0)
        service.launch(run, owner, "prepare", retry=True)
        result = wait(service, run, owner)
        assert CountingAdapter.calls == ["direct_context", "embed_text"]
        assert result["recommendations"]["policy"]["color_weight"] == .15
        with pytest.raises(PermissionError):
            service.state(run, str(uuid4()))
        with pytest.raises(ValueError):
            service.launch(run, owner, "generate")
        changed = dict(manifest)
        changed["direct_contract"] = {**manifest["direct_contract"], "index_version": "tampered"}
        assert contract_hash(changed) != manifest["input_hash"]
        service.index.version = "new-index"
        service.launch(run, owner, "prepare", retry=True)
        for f in service.futures:
            f.result(timeout=5)
        assert service.state(run, owner)["status"] == "INDEX_NOT_READY"
        assert CountingAdapter.calls == ["direct_context", "embed_text"]
    finally:
        service.pool.shutdown(wait=True)


def test_sketch_only_provider_coordinates_reach_recommendations_and_keep_raw_receipt(tmp_path):
    class SketchAdapter(DirectDemoAdapter):
        calls = []

        def call(self, kind, prompt, images, schema=None):
            self.calls.append(kind)
            if kind == "direct_context":
                raw = context(placement_region=None).model_dump()
                raw["placement_region"] = {"x_min": 528, "y_min": 570, "x_max": 735, "y_max": 888}
                return {"payload": {"text": json.dumps(raw), "coordinate_space": "normalized_1000_xyxy"}}
            return super().call(kind, prompt, images, schema)

    service = DirectStudio(tmp_path, demo=True, adapter_factory=SketchAdapter)
    owner = str(uuid4())
    try:
        run = service.submit(image_bytes(), image_bytes(), "", [], None, owner, str(uuid4()))
        service.launch(run, owner, "prepare")
        result = wait(service, run, owner)
        assert result["status"] == "COMPLETED"
        assert result["context"]["placement_region"]["x_min"] == .528
        assert result["context"]["required"] == []
        receipt = read_json(service.directory(run, owner) / "receipts/context.json")
        assert json.loads(receipt["payload"]["text"])["placement_region"]["x_min"] == 528
        service.launch(run, owner, "prepare", retry=True)
        assert wait(service, run, owner)["status"] == "COMPLETED"
        assert SketchAdapter.calls == ["direct_context", "embed_text"]
    finally:
        service.pool.shutdown(wait=True)


def test_ranking_failure_retries_local_step_without_paid_calls(tmp_path, monkeypatch):
    CountingAdapter.calls = []
    service = DirectStudio(tmp_path, demo=True, adapter_factory=CountingAdapter)
    owner = str(uuid4())
    original = service.index.rank
    def fail(*args, **kwargs):
        raise ValueError("simulated local failure")
    try:
        run = service.submit(image_bytes(), None, "의자", [], None, owner, str(uuid4()))
        monkeypatch.setattr(service.index, "rank", fail)
        service.launch(run, owner, "prepare")
        for future in service.futures:
            future.result(timeout=5)
        assert service.state(run, owner)["status"] == "RANKING_FAILED"
        monkeypatch.setattr(service.index, "rank", original)
        service.launch(run, owner, "prepare", retry=True)
        assert wait(service, run, owner)["status"] == "COMPLETED"
        assert CountingAdapter.calls == ["direct_context", "embed_text"]
    finally:
        service.pool.shutdown(wait=True)


def test_default_direct_http_api_and_owned_product_images(tmp_path, monkeypatch):
    if not django_settings.configured:
        configure(tmp_path / "django")
    service = DirectStudio(tmp_path / "web", demo=True)
    monkeypatch.setattr(views, "_studio", service)
    try:
        with override_settings(WEB_DEMO=True, WEB_BRIDGE_TOKEN="", ALLOWED_HOSTS=["testserver"]):
            client = Client(enforce_csrf_checks=True)
            page = client.get("/")
            assert b"direct.js" in page.content and b"studio.js" not in page.content
            response = post(client, "/api/runs", {"photo": SimpleUploadedFile("room.png", image_bytes()), "text": "의자", "request_id": str(uuid4())})
            assert response.status_code == 202
            run = response.json()["run_id"]
            owner = client.session["mvp_owner"]
            result = wait(service, run, owner)
            assert client.get(f"/api/runs/{run}").json()["flow"] == "direct"
            image = result["recommendations"]["items"][0]["image"]
            response = client.get(f"/assets/{run}/{image}")
            assert response.status_code == 200
            response.close()
            assert Client().get(f"/assets/{run}/{image}").status_code == 403
            assert post(client, f"/api/runs/{run}/actions/generate", {}, json_body=True).status_code == 400
            assert "image_checksum" not in json.dumps(result["recommendations"])
    finally:
        service.pool.shutdown(wait=True)


def test_server_factory_defaults_to_direct(tmp_path, monkeypatch):
    if not django_settings.configured:
        configure(tmp_path / "django")
    monkeypatch.setattr(views, "_studio", None)
    with override_settings(WEB_OUTPUT_ROOT=tmp_path / "web", WEB_DEMO=True, WEB_LEGACY_GENERATION=False,
                           WEB_PRODUCTS=None, WEB_DANAWA_INDEX=None, WEB_RANKING_POLICY=None):
        instance = views.studio()
        try:
            assert isinstance(instance, DirectStudio)
        finally:
            instance.pool.shutdown(wait=True)


def test_saved_danawa_export_reuses_vectors_and_keeps_unmapped_unknown(saved_index, tmp_path):
    from interior_recommender.product_catalog import export_catalog
    from interior_recommender.web.danawa import DanawaCatalog
    source = DanawaCatalog(saved_index)
    chosen = source.templates[0]["id"]
    metadata = tmp_path / "metadata.json"
    write_json(metadata, {"products": [{"image_id": chosen, "product_id": "verified-chair", "name": "원장 의자",
                "category_code": "CHAIR", "width_mm": 500, "metadata_source": "test product source"}]})
    target = tmp_path / "catalog-v1"
    path = export_catalog(saved_index, metadata, target)
    catalog = ProductIndex.load(path)
    catalog.validate_settings(Settings())
    result = catalog.rank(source.vectors[chosen], context())
    assert [p["product_id"] for p in result["items"]] == ["verified-chair"]
    assert len(result["unverified_items"]) == 6
    assert catalog.vectors[0].tolist() == source.vectors[chosen]
    with pytest.raises(ValueError, match="immutable"):
        export_catalog(saved_index, metadata, target)


def test_similarity_never_allocates_catalog_by_catalog_matrix(tmp_path, monkeypatch):
    import interior_recommender.recommendation as module
    original = module.cosine_similarity
    shapes = []
    def record(x, y):
        shapes.append((x.shape, y.shape))
        return original(x, y)
    monkeypatch.setattr(module, "cosine_similarity", record)
    row = product(tmp_path, "shared")
    rows = [{**row, "image_id": f"image-{i}", "product_id": f"product-{i}"} for i in range(5000)]
    result = index(rows, np.ones((5000, 2), dtype=np.float32)).rank([1, 0], context())
    assert len(result["items"]) == 6
    assert len(shapes) == 3
    assert all(x == (1, 2) and y[0] <= 2048 for x, y in shapes)
