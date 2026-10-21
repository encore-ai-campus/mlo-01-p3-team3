import json

import pytest
from google.genai.errors import ClientError
from pydantic import ValidationError

from interior_recommender.adapters.gemini import analysis_wire_schema
from interior_recommender.assets import read_json
from interior_recommender.errors import provider_message
from interior_recommender.pipeline import Pipeline
from interior_recommender.schemas import ObjectAnalysis
from interior_recommender.tests.test_pipeline import FakeAdapter, run


def test_analysis_wire_schema_is_inline_and_small():
    wire = analysis_wire_schema(ObjectAnalysis.model_json_schema())
    text = json.dumps(wire)
    for unsupported in ("$ref", "$defs", "maxItems", "minimum", "maximum"):
        assert unsupported not in text
    box = wire["properties"]["objects"]["items"]["properties"]["box"]
    assert set(box["required"]) == {"x_min", "y_min", "x_max", "y_max"}
    assert box["properties"]["x_min"]["type"] == "integer"


def test_explicit_provider_coordinates_are_converted(run):
    result = Pipeline(run, lambda _: None).structured("analyze", {
        "coordinate_space": "normalized_1000_xyxy",
        "text": json.dumps({"objects": [{"category_code": "CHAIR", "box": {
            "x_min": 451, "y_min": 569, "x_max": 664, "y_max": 854}}]})})
    assert result["objects"][0]["box"] == {"x_min": 0.451, "y_min": 0.569, "x_max": 0.664, "y_max": 0.854}


def test_unknown_units_are_not_guessed(run):
    payload = {"text": json.dumps({"objects": [{"category_code": "CHAIR", "box": {
        "x_min": 451, "y_min": 569, "x_max": 664, "y_max": 854}}]})}
    with pytest.raises(ValidationError):
        Pipeline(run, lambda _: None).structured("analyze", payload)


def test_local_object_limit_is_still_enforced():
    obj = {"category_code": "CHAIR", "box": {"x_min": 0, "y_min": 0, "x_max": 1, "y_max": 1}}
    with pytest.raises(ValidationError):
        ObjectAnalysis.model_validate({"objects": [obj] * 51})


def test_provider_message_redacts_secrets_and_urls(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "private-test-key")
    error = ClientError(400, {"error": {"message": "Invalid schema private-test-key https://host/path?key=another-secret"}})
    message = provider_message(error)
    assert "Invalid schema" in message
    assert "private-test-key" not in message and "another-secret" not in message
    assert provider_message(ValueError("unknown private details")) is None


def test_validation_failure_does_not_keep_old_api_error(run):
    adapter = FakeAdapter()
    original = adapter.call
    def broken(kind, *args):
        if kind == "analyze":
            adapter.calls[kind] += 1
            raise ClientError(400, {"error": {"message": "Old API schema error"}})
        return original(kind, *args)
    adapter.call = broken
    with pytest.raises(ValueError, match="Old API schema error"):
        Pipeline(run, lambda _: adapter).execute()

    def invalid_output(kind, *args):
        response = original(kind, *args)
        if kind == "analyze":
            response["payload"] = {"text": '{"objects":[{"category_code":"CHAIR","box":{"x_min":2,"y_min":0,"x_max":3,"y_max":1}}]}'}
        return response
    adapter.call = invalid_output
    with pytest.raises(ValueError) as caught:
        Pipeline(run, lambda _: adapter).execute()
    assert "Old API schema error" not in str(caught.value)
    assert read_json(run / "manifest.json")["stages"]["analyze"]["error_code"] == "OUTPUT_VALIDATION_ERROR"
