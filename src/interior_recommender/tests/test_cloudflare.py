import base64
import io
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from PIL import Image

from interior_recommender.adapters.cloudflare import CloudflareAdapter, CloudflareError
from interior_recommender.adapters.router import ModelAdapter
from interior_recommender.assets import generation_reference, read_json
from interior_recommender.config import Settings
from interior_recommender.pipeline import Pipeline
from interior_recommender.tests.test_pipeline import FakeAdapter, image_bytes, run


@pytest.fixture
def credentials(monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "a" * 32)
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "fake-test-token")


def test_multipart_fields_and_neuron_estimate(credentials):
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(200, json={"success": True, "result": {
            "image": base64.b64encode(image_bytes((512, 512))).decode()}}, headers={"cf-ray": "test-request"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    adapter = CloudflareAdapter(Settings(), client)
    result = adapter.call("generate", "place a chair", [image_bytes(), image_bytes()])
    request = captured[0]
    assert request.headers["content-type"].startswith("multipart/form-data; boundary=")
    assert request.headers["authorization"] == "Bearer fake-test-token"
    assert request.url.path.endswith("/@cf/black-forest-labs/flux-2-klein-4b")
    for name in ("prompt", "width", "height", "input_image_0", "input_image_1"):
        assert f'name="{name}"'.encode() in request.content
    assert b'name="steps"' not in request.content
    assert result["usage_json"]["estimated_neurons"] == 36.79
    assert result["provider_request_id"] == "test-request"
    adapter.close()


def test_limits_rejected_before_network(credentials):
    client = Mock()
    adapter = CloudflareAdapter(Settings(), client)
    with pytest.raises(ValueError, match="four"):
        adapter.call("generate", "", [image_bytes()] * 5)
    with pytest.raises(ValueError, match="480"):
        adapter.call("generate", "", [image_bytes((600, 100))])
    client.post.assert_not_called()


def test_quota_error_is_budget_wait(credentials):
    client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(
        429, json={"success": False, "errors": [{"code": 3036, "message": "quota"}]})))
    with pytest.raises(CloudflareError) as caught:
        CloudflareAdapter(Settings(), client).call("generate", "", [])
    assert caught.value.budget_exhausted
    client.close()


def test_padding_transform_preserves_aspect_ratio():
    data, mapping = generation_reference(image_bytes((1000, 500)))
    with Image.open(io.BytesIO(data)) as image:
        assert image.size == (480, 480)
    assert mapping["scale"] == [0.48, 0.48]
    assert mapping["offset"] == [0, 120]


def test_router_uses_cloudflare_only_for_generation(monkeypatch):
    google, cloudflare = Mock(), Mock()
    monkeypatch.setattr("interior_recommender.adapters.router.GeminiAdapter", lambda _: google)
    monkeypatch.setattr("interior_recommender.adapters.router.CloudflareAdapter", lambda _: cloudflare)
    adapter = ModelAdapter(Settings())
    adapter.call("interpret", "", [])
    adapter.call("generate", "", [])
    adapter.call("embed", "", [])
    assert google.call.call_count == 2
    assert cloudflare.call.call_count == 1
    adapter.close()


def test_output_size_mismatch_stops_analysis(run):
    adapter = FakeAdapter()
    original = adapter.call

    def wrong_size(kind, *args):
        response = original(kind, *args)
        if kind == "generate":
            response["payload"]["image_base64"] = base64.b64encode(image_bytes((1024, 1024))).decode()
        return response

    adapter.call = wrong_size
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    assert adapter.calls["analyze"] == 0
    assert (run / "receipts/generate.json").exists()
    assert not (run / "generated.png").exists()


def test_pipeline_quota_wait_does_not_retry_or_regenerate(run):
    adapter = FakeAdapter()
    original = adapter.call

    def exhausted(kind, *args):
        if kind == "generate":
            adapter.calls[kind] += 1
            raise CloudflareError(429, SimpleNamespace(headers={}), 3036)
        return original(kind, *args)

    adapter.call = exhausted
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    manifest = read_json(run / "manifest.json")
    assert manifest["status"] == "BUDGET_WAIT"
    assert manifest["stages"]["generate"]["status"] == "BUDGET_WAIT"
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    assert adapter.calls["generate"] == 1
    assert read_json(run / "manifest.json")["status"] == "BUDGET_WAIT"


def test_1024_new_run_keeps_snapshot_when_env_changes(tmp_path, monkeypatch):
    from interior_recommender.assets import write_json
    from interior_recommender.pipeline import create_run
    from interior_recommender.tests.test_cli import prepare
    prepare(tmp_path)
    directory = create_run(tmp_path / "request.json", tmp_path / "templates.json", tmp_path / "outputs",
                           Settings(output_width=1024, output_height=1024))
    adapter = FakeAdapter()
    original = adapter.call
    def large(kind, *args):
        response = original(kind, *args)
        if kind == "generate":
            response["payload"]["image_base64"] = base64.b64encode(image_bytes((1024, 1024))).decode()
        return response
    adapter.call = large
    monkeypatch.setenv("AI_OUTPUT_WIDTH", "512")
    result = Pipeline(directory, lambda _: adapter).execute("generate")
    assert result["settings"]["output_width"] == 1024
    assert result["assets"]["generated.png"]["size"] == (1024, 1024)


def test_region_mapping_does_not_overwrite_original(run):
    from interior_recommender.assets import write_json
    from interior_recommender.pipeline import contract_hash
    manifest = read_json(run / "manifest.json")
    region = {"x_min": 0, "y_min": 0, "x_max": 1, "y_max": 1}
    manifest["request"]["requested_region"] = region
    manifest["input_hash"] = contract_hash(manifest)
    write_json(run / "manifest.json", manifest)
    result = Pipeline(run, lambda _: FakeAdapter()).execute("generate")
    assert result["request"]["requested_region"] == region
    assert result["generation_input_refs"]["api_requested_region"] == {
        "x_min": 0, "x_max": 1, "y_min": 0.1, "y_max": 0.9}


def test_legacy_run_preserves_gemini_provider(run):
    from interior_recommender.assets import write_json
    from interior_recommender.pipeline import contract_hash
    manifest = read_json(run / "manifest.json")
    for name in ("image_provider", "output_width", "output_height", "generation_preprocessing_version"):
        manifest["settings"].pop(name)
    manifest["settings"]["image_model"] = "gemini-3.1-flash-lite-image"
    manifest["input_hash"] = contract_hash(manifest)
    write_json(run / "manifest.json", manifest)
    pipeline = Pipeline(run, lambda _: FakeAdapter())
    assert pipeline.settings.image_provider == "gemini"
    result = pipeline.execute("generate")
    assert result["settings"]["image_model"] == "gemini-3.1-flash-lite-image"
    assert "generation_input_refs" not in result
