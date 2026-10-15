import base64
import io
import json
from collections import Counter

import pytest
from PIL import Image

from interior_recommender.assets import read_json, write_json
from interior_recommender.config import Settings
from interior_recommender.pipeline import Pipeline, create_run
from interior_recommender.schemas import Box, normalized_vector


def image_bytes(size=(100, 80)):
    output = io.BytesIO()
    Image.new("RGB", size, "white").save(output, format="PNG")
    return output.getvalue()


class FakeAdapter:
    def __init__(self):
        self.calls = Counter()
        self.fail_analysis = False
        self.decision = "ACCEPTED"

    def call(self, kind, prompt, images, schema=None):
        self.calls[kind] += 1
        if kind == "interpret":
            payload = {"text": json.dumps({"decision": self.decision,
                        "category_code": "CHAIR" if self.decision == "ACCEPTED" else None,
                        "candidates": ["CHAIR"], "color": None, "material": None,
                        "shape": None, "reason": "test"})}
        elif kind == "generate":
            payload = {"image_base64": base64.b64encode(image_bytes((512, 512))).decode(), "mime_type": "image/png"}
        elif kind == "analyze":
            if self.fail_analysis:
                raise ValueError("invalid upstream response")
            payload = {"text": json.dumps({"objects": [{"category_code": "CHAIR", "box": {
                "x_min": 0.1, "y_min": 0.2, "x_max": 0.5, "y_max": 0.7}}]})}
        else:
            payload = {"values": [1.0] * 768}
        return {"payload": payload, "usage_json": {"total_tokens": 10}, "provider_request_id": "test"}

    def close(self):
        pass


@pytest.fixture
def run(tmp_path):
    (tmp_path / "room.png").write_bytes(image_bytes())
    (tmp_path / "template.png").write_bytes(image_bytes())
    request = tmp_path / "request.json"
    write_json(request, {"room_image": "room.png", "text": "chair", "template_id": "chair-01"})
    templates = tmp_path / "templates.json"
    write_json(templates, {"templates": [{"id": "chair-01", "version": 1,
               "category_code": "CHAIR", "image": "template.png", "review_status": "APPROVED",
               "active": True, "provenance": "test fixture"}]})
    return create_run(request, templates, tmp_path / "outputs", Settings())


def test_complete_flow_requires_selection_and_reuses_calls(run):
    adapter = FakeAdapter()
    result = Pipeline(run, lambda _: adapter).execute()
    assert result["status"] == "NEEDS_OBJECT_SELECTION"
    assert adapter.calls["embed"] == 0
    obj = read_json(run / "analyze.json")["objects"][0]
    result = Pipeline(run, lambda _: adapter).execute("embed", obj["id"])
    assert result["status"] == "SUCCEEDED"
    embedding = read_json(run / f"embed-{obj['id']}.json")
    assert len(embedding["values"]) == 768
    assert sum(v * v for v in embedding["values"]) == pytest.approx(1)
    assert embedding["asset_id"] == result["assets"][f"crops/{obj['id']}.png"]["asset_id"]
    Pipeline(run, lambda _: adapter).execute("embed", obj["id"])
    assert adapter.calls == Counter(interpret=1, generate=1, analyze=1, embed=1)


def test_analysis_failure_resumes_without_regeneration(run):
    adapter = FakeAdapter()
    adapter.fail_analysis = True
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    assert (run / "generated.png").exists()
    adapter.fail_analysis = False
    Pipeline(run, lambda _: adapter).execute()
    assert adapter.calls == Counter(interpret=1, generate=1, analyze=2)


def test_persistence_failure_recovers_saved_response(run, monkeypatch):
    adapter = FakeAdapter()
    pipeline = Pipeline(run, lambda _: adapter)
    original = pipeline.persist_asset
    def fail_result_only(name, *args):
        if name == "generated.png":
            raise OSError("disk error")
        return original(name, *args)
    monkeypatch.setattr(pipeline, "persist_asset", fail_result_only)
    with pytest.raises(ValueError):
        pipeline.execute("generate")
    assert (run / "receipts/generate.json").exists()
    monkeypatch.setattr(pipeline, "persist_asset", original)
    pipeline.execute("generate")
    assert adapter.calls["generate"] == 1


def test_uncertain_category_never_generates(run):
    adapter = FakeAdapter()
    adapter.decision = "NEEDS_CATEGORY_SELECTION"
    assert Pipeline(run, lambda _: adapter).execute()["status"] == "NEEDS_CATEGORY_SELECTION"
    assert adapter.calls == Counter(interpret=1)


def test_input_mutation_stops_before_call(run):
    (run / "inputs/room_image.png").write_bytes(b"modified")
    adapter = FakeAdapter()
    with pytest.raises(ValueError, match="checksum"):
        Pipeline(run, lambda _: adapter).execute()
    assert not adapter.calls


def test_settings_mutation_requires_new_run(run):
    manifest = read_json(run / "manifest.json")
    manifest["settings"]["dimension"] = 1536
    write_json(run / "manifest.json", manifest)
    with pytest.raises(ValueError, match="modified"):
        Pipeline(run, lambda _: FakeAdapter()).execute()


@pytest.mark.parametrize("values,dimension", [([0, 0], 2), ([float('nan')], 1),
                                              ([1], 2), ([float('inf')], 1), ([True], 1)])
def test_invalid_vectors_rejected(values, dimension):
    with pytest.raises(ValueError):
        normalized_vector(values, dimension)


def test_reversed_box_rejected():
    with pytest.raises(ValueError):
        Box(x_min=0.8, y_min=0, x_max=0.2, y_max=1)


def test_lock_prevents_duplicate_call(run):
    (run / ".lock").touch()
    adapter = FakeAdapter()
    with pytest.raises(ValueError, match="locked"):
        Pipeline(run, lambda _: adapter).execute()
    assert not adapter.calls


def test_invalid_json_requires_explicit_retry(run):
    adapter = FakeAdapter()
    original = adapter.call

    def malformed(kind, *args):
        response = original(kind, *args)
        if kind == "analyze":
            response["payload"] = {"text": "not JSON"}
        return response

    adapter.call = malformed
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute()
    assert adapter.calls["analyze"] == 1
    adapter.call = original
    Pipeline(run, lambda _: adapter).execute("analyze", retry_rejected=True)
    assert adapter.calls == Counter(interpret=1, generate=1, analyze=2)
    assert list((run / "receipts").glob("analyze.rejected-*.json"))


def test_output_tampering_is_detected(run):
    adapter = FakeAdapter()
    Pipeline(run, lambda _: adapter).execute()
    write_json(run / "analyze.json", {"objects": []})
    with pytest.raises(ValueError, match="checksum"):
        Pipeline(run, lambda _: adapter).execute()
    assert adapter.calls["analyze"] == 1


def test_rate_limit_retry_and_usage_history(run, monkeypatch):
    adapter = FakeAdapter()
    original = adapter.call
    attempts = []

    class RateLimitError(Exception):
        code = 429
        response = None

    def limited(kind, *args):
        attempts.append(kind)
        if len(attempts) == 1:
            raise RateLimitError("secret URL must never be recorded")
        return original(kind, *args)

    adapter.call = limited
    monkeypatch.setattr("interior_recommender.pipeline.time.sleep", lambda _: None)
    result = Pipeline(run, lambda _: adapter).execute("interpret")
    history = result["stages"]["interpret"]["attempts"]
    assert [a["status"] for a in history] == ["FAILED", "SUCCEEDED"]
    assert history[1]["usage_json"] == {"total_tokens": 10}
    assert "secret URL" not in (run / "manifest.json").read_text()


def test_permission_error_does_not_retry(run):
    calls = []

    class AuthError(Exception):
        code = 403

    adapter = FakeAdapter()

    def denied(*args):
        calls.append(1)
        raise AuthError("key should not leak")

    adapter.call = denied
    with pytest.raises(ValueError):
        Pipeline(run, lambda _: adapter).execute("interpret")
    assert len(calls) == 1
    assert "key should not leak" not in (run / "manifest.json").read_text()


def test_missing_template_does_not_generate(run):
    # Create a valid new snapshot with no approved templates.
    from interior_recommender.pipeline import contract_hash
    manifest = read_json(run / "manifest.json")
    manifest["templates"] = []
    manifest["input_hash"] = contract_hash(manifest)
    write_json(run / "manifest.json", manifest)
    adapter = FakeAdapter()
    result = Pipeline(run, lambda _: adapter).execute()
    assert result["status"] == "TEMPLATE_UNAVAILABLE"
    assert adapter.calls == Counter(interpret=1)
