import json
from collections import Counter

import pytest

from interior_recommender.assets import read_json
from interior_recommender.schemas import ScenePlan
from interior_recommender.services import scene_plan_prompt
from interior_recommender.web.workflow import DemoAdapter
from interior_recommender.tests.test_web_workflow import web, post, submit, wait


def selected_run(client, service, **extra):
    run = submit(client, **extra).json()["run_id"]
    owner = client.session["mvp_owner"]
    parent = wait(service, run, owner)
    assert "reference" not in parent["stages"]
    child = post(client, f"/api/runs/{run}/selection", {"template_id": "chair-demo"}, json_body=True).json()["run_id"]
    return child, owner, wait(service, child, owner)


def test_placement_rectangle_does_not_become_furniture_shape():
    base = {"decision": "ACCEPTED", "category_code": "CHAIR", "candidates": [], "color": "black",
            "material": None, "shape": "square", "reason": "rectangle annotation", "search_text": "black chair",
            "placement_intent": "right", "keep_elements": [], "sketch_role": "PLACEMENT_ONLY",
            "shape_evidence": "SKETCH_DETAIL"}
    assert ScenePlan(**base).shape is None
    assert ScenePlan(**{**base, "shape_evidence": "NONE"}).shape is None
    assert ScenePlan(**{**base, "shape_evidence": "TEXT"}).shape == "square"
    assert "placement/size annotation" in scene_plan_prompt({})


def test_reference_analysis_is_cached_and_controls_generation_prompt(web, monkeypatch):
    client, service = web
    calls = Counter()

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            result = super().call(kind, *args, **kwargs)
            if kind == "reference":
                value = json.loads(result["payload"]["text"])
                value.update(support_structure="central pedestal and radial caster base",
                             components=["seat", "pedestal", "caster base"],
                             connections=["seat attaches to pedestal; pedestal attaches to caster base"])
                result["payload"]["text"] = json.dumps(value)
            return result

    service.factory = Observed
    from django.core.files.uploadedfile import SimpleUploadedFile
    from interior_recommender.tests.test_pipeline import image_bytes
    child, owner, state = selected_run(client, service, sketch=SimpleUploadedFile("sketch.png", image_bytes(), content_type="image/png"))
    preview = state["preview"]
    assert calls["reference"] == 1
    assert calls["generate"] == 0
    assert preview["reference_structure"]["exact_caster_count"] is None
    assert "central pedestal and radial caster base" in preview["prompt"]
    assert "Adapt only placement, scale, perspective" in preview["prompt"]
    assert "Reference image 2: reference furniture" in preview["prompt"]
    assert "placement annotations" in preview["prompt"]
    assert '"exact_caster_count": null' in preview["prompt"]
    second, _, other = selected_run(client, service)
    assert calls["reference"] == 1
    assert other["stages"]["reference"]["cache_reused"] is True
    # Changing the analysis model must not reuse another model's structure profile.
    original_settings = service.directory(second, owner) / "manifest.json"
    assert read_json(original_settings)["web_prompt_version"] == preview["prompt_version"]
    monkeypatch.setenv("AI_ANALYSIS_MODEL", "test-different-analysis-model")
    selected_run(client, service)
    assert calls["reference"] == 2


@pytest.mark.parametrize("status", ["MATCH", "MISMATCH", "UNCERTAIN"])
def test_structure_check_uses_reference_and_never_regenerates_automatically(web, status):
    client, service = web
    calls = Counter()
    verdict = [status]
    compare_inputs = []

    class Observed(DemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            calls[kind] += 1
            result = super().call(kind, prompt, images, schema)
            if kind == "compare":
                compare_inputs.append(images)
                value = json.loads(result["payload"]["text"])
                value["reference_fidelity"] = {"status": verdict[0], "differences": ["added seat-to-floor support"] if verdict[0] == "MISMATCH" else [], "reason": "test"}
                result["payload"]["text"] = json.dumps(value)
            return result

    service.factory = Observed
    child, owner, state = selected_run(client, service)
    post(client, f"/api/runs/{child}/actions/generate", {}, json_body=True)
    result = wait(service, child, owner)
    assert len(compare_inputs[0]) == 3
    manifest = read_json(service.directory(child, owner) / "manifest.json")
    assert compare_inputs[0][2] == (service.directory(child, owner) / manifest["selected_template"]["image"]).read_bytes()
    assert result["result_image"] == "generated.png"
    assert result["status"] == ("COMPLETED" if status == "MATCH" else "NEEDS_STRUCTURE_REVIEW")
    assert result["comparison"]["objects"][0]["eligible_for_selection"] == (status == "MATCH")
    assert calls["generate"] == calls["reference"] == 1
    verdict[0] = "MATCH"
    post(client, f"/api/runs/{child}/actions/compare", {"retry": True}, json_body=True)
    assert wait(service, child, owner)["status"] == "COMPLETED"
    assert calls["generate"] == calls["reference"] == 1
    assert calls["compare"] == 2


def test_reference_adapter_uses_inline_schema():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from interior_recommender.adapters.gemini import GeminiAdapter
    from interior_recommender.config import Settings
    from interior_recommender.schemas import ReferenceStructure
    client = Mock()
    client.models.generate_content.return_value = SimpleNamespace(text="{}", usage_metadata=None, response_id=None)
    GeminiAdapter(Settings(), client).call("reference", "inspect", [], ReferenceStructure.model_json_schema())
    config = client.models.generate_content.call_args.kwargs["config"]
    assert "exact_caster_count" in config.response_json_schema["properties"]
    assert "$defs" not in config.response_json_schema


def test_comparison_failure_keeps_generated_image_and_retry_does_not_generate(web):
    client, service = web
    calls = Counter()
    fail = [True]

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            if kind == "compare" and fail[0]:
                raise ValueError("simulated comparison failure")
            return super().call(kind, *args, **kwargs)

    service.factory = Observed
    child, owner, _ = selected_run(client, service)
    post(client, f"/api/runs/{child}/actions/generate", {}, json_body=True)
    for future in service.futures:
        future.result(timeout=5)
    failed = service.state(child, owner)
    assert failed["job"]["status"] == "FAILED"
    assert failed["result_image"] == "generated.png"
    fail[0] = False
    post(client, f"/api/runs/{child}/actions/compare", {"retry": True}, json_body=True)
    assert wait(service, child, owner)["status"] == "COMPLETED"
    assert calls["generate"] == 1


def test_bad_reference_response_blocks_generation_until_explicit_prepare_retry(web):
    client, service = web
    fail = [True]
    calls = Counter()

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            if kind == "reference" and fail[0]:
                return {"payload": {"text": "{}"}, "usage_json": {}, "provider_request_id": None}
            return super().call(kind, *args, **kwargs)

    service.factory = Observed
    run = submit(client).json()["run_id"]
    owner = client.session["mvp_owner"]
    wait(service, run, owner)
    child = post(client, f"/api/runs/{run}/selection", {"template_id": "chair-demo"}, json_body=True).json()["run_id"]
    for future in service.futures:
        future.result(timeout=5)
    assert service.state(child, owner)["job"]["status"] == "FAILED"
    assert not (service.directory(child, owner) / "generation_request.json").exists()
    assert calls["generate"] == 0
    fail[0] = False
    post(client, f"/api/runs/{child}/actions/prepare", {"retry": True}, json_body=True)
    assert wait(service, child, owner)["status"] == "READY_TO_GENERATE"
    assert calls["reference"] == 2
    assert calls["generate"] == 0
