import json
from collections import Counter

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile

from interior_recommender.assets import read_json
from interior_recommender.tests.test_pipeline import image_bytes
from interior_recommender.tests.test_reference_preservation import selected_run
from interior_recommender.tests.test_web_workflow import web, post, submit, wait
from interior_recommender.web.workflow import DemoAdapter


@pytest.mark.parametrize("role,send", [("PLACEMENT_ONLY", False), ("FURNITURE_SHAPE", True),
                                      ("UNCERTAIN", False), ("NONE", False)])
def test_sketch_used_for_interpretation_but_only_detailed_shape_sent_to_generator(web, role, send):
    client, service = web
    inputs = {}

    class Observed(DemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            inputs[kind] = images
            value = super().call(kind, prompt, images, schema)
            if kind == "plan":
                plan = json.loads(value["payload"]["text"])
                plan.update(sketch_role=role, color="black", placement_region={"x_min": .2, "y_min": .3, "x_max": .6, "y_max": .8})
                value["payload"]["text"] = json.dumps(plan)
            return value

    service.factory = Observed
    raw = image_bytes()
    child, owner, state = selected_run(client, service, sketch=SimpleUploadedFile("sketch.png", raw, content_type="image/png"))
    assert len(inputs["plan"]) == 2
    preview = state["preview"]
    assert preview["sketch_policy"]["sent_to_generator"] is send
    assert len(preview["images"]) == (3 if send else 2)
    assert preview["placement_region_source"] == "plan"
    assert preview["api_requested_region"] == pytest.approx({"x_min": .2, "y_min": .34, "x_max": .6, "y_max": .74})
    assert preview["original_placement_region"] == state["plan"]["placement_region"]
    assert f"Reference image {3 if send else 2}: reference furniture" in preview["prompt"]
    assert '"color": "black"' in preview["prompt"]
    directory = service.directory(child, owner)
    manifest = read_json(directory / "manifest.json")
    assert manifest["request"]["requested_region"] is None
    assert manifest["request"]["sketch_image"] == "inputs/sketch_image.png"
    assert (directory / "inputs/original-sketch.bin").read_bytes() == raw
    post(client, f"/api/runs/{child}/actions/generate", {}, json_body=True)
    assert wait(service, child, owner)["status"] == "COMPLETED"
    assert len(inputs["generate"]) == (3 if send else 2)
    assert len(inputs["compare"]) == 3


def test_explicit_placement_coordinates_override_inferred_box(web):
    client, service = web
    box = {"x_min": .1, "y_min": .2, "x_max": .4, "y_max": .6}
    _, _, state = selected_run(client, service, requested_region=json.dumps(box),
                              sketch=SimpleUploadedFile("sketch.png", image_bytes(), content_type="image/png"))
    assert state["preview"]["placement_region_source"] == "request"
    assert state["preview"]["original_placement_region"] == box
    assert state["preview"]["api_requested_region"] == pytest.approx({"x_min": .1, "y_min": .26, "x_max": .4, "y_max": .58})


@pytest.mark.parametrize("box", [
    {"x_min": -.1, "y_min": .2, "x_max": .4, "y_max": .6},
    {"x_min": .8, "y_min": .2, "x_max": .4, "y_max": .6},
    {"x_min": 100, "y_min": 200, "x_max": 400, "y_max": 600},
])
def test_invalid_inferred_box_stops_before_search_or_generation(web, box):
    client, service = web
    calls = Counter()

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            result = super().call(kind, *args, **kwargs)
            if kind == "plan":
                value = json.loads(result["payload"]["text"])
                value["placement_region"] = box
                result["payload"]["text"] = json.dumps(value)
            return result

    service.factory = Observed
    run = submit(client).json()["run_id"]
    for future in service.futures:
        future.result(timeout=5)
    state = service.state(run, client.session["mvp_owner"])
    assert state["job"]["status"] == "FAILED"
    assert state["stages"]["plan"]["status"] == "FAILED"
    assert calls["embed_text"] == calls["generate"] == 0


def test_unknown_placement_does_not_invent_coordinates(web):
    client, service = web
    _, _, state = selected_run(client, service)
    assert state["preview"]["api_requested_region"] is None
    assert state["preview"]["original_placement_region"] is None
    assert state["preview"]["placement_region_source"] == "placement_intent_only"


@pytest.mark.parametrize("verdict,expected", [("CLEAR", "COMPLETED"), ("DETECTED", "NEEDS_ANNOTATION_REVIEW"),
                                             ("UNCERTAIN", "NEEDS_ANNOTATION_REVIEW"), (None, "NEEDS_ANNOTATION_REVIEW")])
def test_added_annotations_require_review_without_automatic_regeneration(web, verdict, expected):
    client, service = web
    current_verdict = [verdict]
    calls = Counter()

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            result = super().call(kind, *args, **kwargs)
            if kind == "compare":
                value = json.loads(result["payload"]["text"])
                if current_verdict[0] is None:
                    value.pop("annotation_check")
                else:
                    value["annotation_check"] = {"status": current_verdict[0],
                        "findings": ["new dimension line and gibberish label beside chair"] if current_verdict[0] == "DETECTED" else [],
                        "reason": "compared with original room"}
                result["payload"]["text"] = json.dumps(value)
            return result

    service.factory = Observed
    child, owner, _ = selected_run(client, service)
    post(client, f"/api/runs/{child}/actions/generate", {}, json_body=True)
    state = wait(service, child, owner)
    assert state["status"] == expected
    assert state["result_image"] == "generated.png"
    assert state["comparison"]["objects"][0]["eligible_for_selection"] == (verdict == "CLEAR")
    current_verdict[0] = "CLEAR"
    post(client, f"/api/runs/{child}/actions/compare", {"retry": True}, json_body=True)
    assert wait(service, child, owner)["status"] == "COMPLETED"
    assert calls["generate"] == 1
    assert calls["compare"] == 2
