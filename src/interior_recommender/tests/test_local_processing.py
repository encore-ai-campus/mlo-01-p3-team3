import copy
import json
from collections import Counter

import pytest
from pydantic import ValidationError

from interior_recommender.assets import read_json, write_json
from interior_recommender.local_processing import (LOCAL_PROCESSING_VERSION, assess_comparison,
                                     map_region, rank_candidates, search_query)
from interior_recommender.pipeline import contract_hash
from interior_recommender.schemas import SceneUnderstanding
from interior_recommender.services import WEB_PROMPT_VERSION
from interior_recommender.tests.test_reference_preservation import selected_run
from interior_recommender.tests.test_scale_policy import fixture
from interior_recommender.tests.test_web_workflow import web, post, submit, wait
from interior_recommender.web.workflow import DemoAdapter


def test_query_keeps_semantic_negation_but_excludes_placement_and_model_query():
    plan = {"category_code": "CHAIR", "color": "black", "material": None, "shape": None,
            "search_terms": ["BLACK", "without  armrests", "office chair"],
            "placement_intent": "beside the window", "search_text": "draw a rectangle"}
    assert search_query(plan) == "CHAIR black without armrests office chair"
    assert "search_text" not in SceneUnderstanding.model_json_schema()["properties"]


def test_top_k_matches_full_sort_with_stable_ties_and_missing_attributes():
    templates = [{"id": name, "version": 1, "category_code": "CHAIR", "image": name,
                  "attributes": {"color": "white"} if name == "b" else {}}
                 for name in ("c", "b", "a")]
    vectors = {name: [1., 0.] for name in ("a", "b", "c")}
    reused = dict.fromkeys(vectors, True)
    all_results = rank_candidates([1., 0.], templates, vectors, reused, {"color": "black"})
    assert [item["template_id"] for item in all_results] == ["a", "b", "c"]
    assert rank_candidates([1., 0.], templates, vectors, reused, {"color": "black"}, 2) == all_results[:2]
    assert all_results[0]["differences"][0]["status"] == "UNKNOWN"
    assert all_results[1]["differences"][0]["status"] == "DIFFERENT"


def test_coordinate_conversion_validates_and_preserves_original():
    box = dict(x_min=.1, y_min=.2, x_max=.9, y_max=.8)
    original = copy.deepcopy(box)
    mapping = dict(original_size=[1000, 500], size=[480, 480], scale=[.48, .48], offset=[0, 120])
    assert map_region(box, mapping) == pytest.approx(dict(x_min=.1, y_min=.35, x_max=.9, y_max=.65))
    assert box == original
    with pytest.raises(ValidationError):
        map_region({**box, "x_max": .05}, mapping)


def test_local_verdict_does_not_promote_visual_uncertainty_or_mutate_evidence():
    raw, scale = fixture("compare"), fixture("scale")
    raw["annotation_check"]["status"] = "UNCERTAIN"
    raw["objects"][0]["box"] = dict(x_min=0, y_min=0, x_max=.05, y_max=.05)
    original = copy.deepcopy(raw)
    result = assess_comparison(raw, scale, scale, "CHAIR")
    assert raw == original
    assert result["scale_check"]["status"] == "TOO_SMALL"
    assert result["local_processing"]["status"] == "NEEDS_ANNOTATION_REVIEW"
    assert result["local_processing"]["visual_scale_status"] == "APPROPRIATE"
    assert not result["objects"][0]["eligible_for_selection"]


def test_new_run_composes_query_locally_and_preserves_raw_provider_receipt(web):
    client, service = web
    calls = Counter()

    class Observed(DemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            calls[kind] += 1
            response = super().call(kind, prompt, images, schema)
            if kind == "plan":
                assert "search_text" not in schema["properties"]
                value = json.loads(response["payload"]["text"])
                value.update(color="black", search_terms=["without armrests"])
                response["payload"]["text"] = json.dumps(value)
            if kind == "embed_text":
                assert prompt == "task: search result | query: CHAIR black without armrests"
            return response

    service.factory = Observed
    run = submit(client).json()["run_id"]
    owner = client.session["mvp_owner"]
    state = wait(service, run, owner)
    directory = service.directory(run, owner)
    plan = read_json(directory / "plan.json")
    receipt = read_json(directory / "receipts/plan.json")
    assert plan["search_text"] == "CHAIR black without armrests"
    assert "search_text" not in json.loads(receipt["payload"]["text"])
    search = read_json(directory / "template_candidates.json")
    assert search["local_processing_version"] == LOCAL_PROCESSING_VERSION
    assert search["ranking_duration_ms"] >= 0
    assert state["status"] == "NEEDS_TEMPLATE_SELECTION"
    before = calls.copy()
    service.launch(run, owner, "prepare", retry=True)
    wait(service, run, owner)
    assert calls == before  # local recomputation must not add an AI request


def test_v4_comparison_retry_reuses_frozen_scale_without_generation(web):
    client, service = web
    child, owner, _ = selected_run(client, service)
    directory = service.directory(child, owner)
    post(client, f"/api/runs/{child}/actions/generate", {}, json_body=True)
    assert wait(service, child, owner)["status"] == "COMPLETED"
    manifest = read_json(directory / "manifest.json")
    generated_attempts = copy.deepcopy(manifest["stages"]["generate"]["attempts"])
    manifest["web_prompt_version"] = "scene-context-scale-v4"
    manifest["input_hash"] = contract_hash(manifest)
    write_json(directory / "manifest.json", manifest)
    post(client, f"/api/runs/{child}/actions/compare", {"retry": True}, json_body=True)
    result = wait(service, child, owner)
    assert result["status"] == "COMPLETED"
    assert result["comparison"]["local_processing"]["version"] == LOCAL_PROCESSING_VERSION
    assert result["stages"]["generate"]["attempts"] == generated_attempts
    assert WEB_PROMPT_VERSION != "scene-context-scale-v4"
