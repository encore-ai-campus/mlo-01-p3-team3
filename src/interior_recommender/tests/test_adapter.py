import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.genai import types

from interior_recommender.adapters.gemini import GeminiAdapter
from interior_recommender.config import Settings
from interior_recommender.schemas import Interpretation
from interior_recommender.recommendation import RecommendationContext, parse_context
from interior_recommender.tests.test_pipeline import image_bytes


def test_sdk_requests_use_separate_models_and_schemas():
    client = Mock()
    client.models.generate_content.return_value = SimpleNamespace(text='{}', usage_metadata=None, response_id=None)
    adapter = GeminiAdapter(Settings(image_provider="gemini", image_model="gemini-3.1-flash-lite-image"), client)
    adapter.call("interpret", "request", [image_bytes()], Interpretation.model_json_schema())
    kwargs = client.models.generate_content.call_args.kwargs
    assert kwargs["model"] == "gemini-3.5-flash-lite"
    assert kwargs["config"].response_mime_type == "application/json"
    assert kwargs["config"].response_json_schema["properties"]["decision"]
    client.models.generate_content.return_value = types.GenerateContentResponse(candidates=[types.Candidate(
        content=types.Content(parts=[types.Part.from_bytes(data=image_bytes(), mime_type="image/png")]))])
    result = adapter.call("generate", "request", [image_bytes()])
    assert result["payload"]["image_base64"]
    assert client.models.generate_content.call_args.kwargs["model"] == "gemini-3.1-flash-lite-image"


def test_direct_context_provider_contract_normalizes_coordinates_at_application_boundary():
    client = Mock()
    raw = {"decision": "ACCEPTED", "category_code": "CHAIR", "placement_region":
           {"x_min": 528, "y_min": 570, "x_max": 735, "y_max": 888}}
    client.models.generate_content.return_value = SimpleNamespace(
        text=json.dumps(raw), usage_metadata=None, response_id=None)
    response = GeminiAdapter(Settings(), client).call(
        "direct_context", "Interpret room and sketch", [image_bytes()], RecommendationContext.model_json_schema())
    request = client.models.generate_content.call_args.kwargs
    assert "0 to 1000" in request["contents"][0]
    region = request["config"].response_json_schema["properties"]["placement_region"]
    box = next(node for node in region["anyOf"] if node.get("type") == "object")
    assert box["properties"]["x_min"]["type"] == "integer"
    assert box["properties"]["x_max"]["maximum"] == 1000
    assert response["payload"]["coordinate_space"] == "normalized_1000_xyxy"
    assert json.loads(response["payload"]["text"])["placement_region"] == raw["placement_region"]
    assert parse_context(response["payload"], "", ["CHAIR"]).placement_region.x_min == .528


def test_missing_image_is_failure():
    client = Mock()
    client.models.generate_content.return_value = types.GenerateContentResponse()
    with pytest.raises(ValueError, match="no image"):
        GeminiAdapter(Settings(), client).call("generate", "request", [])


def test_image_embedding_has_no_text_and_explicit_dimension():
    client = Mock()
    client.models.embed_content.return_value = types.EmbedContentResponse(
        embeddings=[types.ContentEmbedding(values=[1.0] * 768)])
    adapter = GeminiAdapter(Settings(), client)
    adapter.call("embed", "", [image_bytes()])
    kwargs = client.models.embed_content.call_args.kwargs
    assert kwargs["model"] == "gemini-embedding-2"
    assert kwargs["config"].output_dimensionality == 768
    assert len(kwargs["contents"].parts) == 1
    assert kwargs["contents"].parts[0].inline_data.mime_type == "image/png"
    with pytest.raises(ValueError, match="one image"):
        adapter.call("embed", "", [image_bytes(), image_bytes()])
