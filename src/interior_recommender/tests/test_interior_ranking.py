"""Behavioral coverage of room-aware ordering without DB or provider calls."""
from uuid import uuid4

import numpy as np
import pytest

from interior_recommender.assets import checksum
from interior_recommender.domain.interior import InteriorProfile, compatibility, product_profile
from interior_recommender.domain.recommendation import Condition, Product, ProductIndex, RankingPolicy, RecommendationContext
from interior_recommender.application.direct import DirectDemoAdapter, DirectStudio
from interior_recommender.tests.test_pipeline import image_bytes
from interior_recommender.tests.test_web_workflow import wait


def row(tmp_path, name, **metadata):
    path = tmp_path / (name + ".png")
    path.write_bytes(image_bytes())
    return dict(image_id=name, product_id=name, name=name, category_code="CHAIR", metadata_source="test-catalog",
                image=str(path), image_checksum=checksum(path.read_bytes()), **metadata)


def profile(**values):
    return InteriorProfile(evidence=["Visible wood floor and light walls"], **values)


def catalog(rows, vectors=None):
    return ProductIndex(rows, np.asarray(vectors or [[1., 0.]] * len(rows), dtype=np.float32),
                        model="test", preprocessing="test", version="test-room")


def test_room_observation_changes_order_without_hard_exclusion(tmp_path):
    idx = catalog([row(tmp_path, "a-black", color="black", material="metal"),
                   row(tmp_path, "z-light", color="ivory", material="wood")])
    context = RecommendationContext(decision="ACCEPTED", category_code="CHAIR",
                                    room_profile=profile(palette=["WHITE"], materials=["WOOD"]))
    result = idx.rank([1, 0], context)
    assert [p["product_id"] for p in result["items"]] == ["z-light", "a-black"]
    light = result["items"][0]
    assert light["interior_match"]["checks"]["palette"]["affinity"] == .7
    assert light["score"] == pytest.approx(sum(light["score_components"].values()))
    assert light["match_explanation"] and light["size_fit"] == "UNKNOWN"
    assert result["excluded_images"] == 0
    legacy = idx.rank([1, 0], context, RankingPolicy(version="weighted-content-v2"))
    assert legacy["items"][0]["product_id"] == "a-black"
    assert "interior_match" not in legacy["items"][0]


def test_missing_room_or_product_features_never_fabricate_a_match(tmp_path):
    idx = catalog([row(tmp_path, "unknown")])
    context = RecommendationContext(decision="ACCEPTED", category_code="CHAIR")
    assert idx.rank([1, 0], context)["items"][0]["score"] == pytest.approx(1)
    context.room_profile = profile(palette=["white"], materials=["wood"])
    item = idx.rank([1, 0], context)["items"][0]
    assert item["score"] == pytest.approx(.8)
    assert item["score_components"]["room_compatibility"] == 0
    assert all(v["state"] == "UNKNOWN" for v in item["interior_match"]["checks"].values())


def test_explicit_color_requirement_beats_room_palette(tmp_path):
    idx = catalog([row(tmp_path, "black", color="black"), row(tmp_path, "white", color="white")])
    context = RecommendationContext(decision="ACCEPTED", category_code="CHAIR",
        required=[Condition(field="color", operator="EQ", value="black", source_quote="black chair")],
        room_profile=profile(palette=["white"]))
    assert [p["product_id"] for p in idx.rank([1, 0], context)["items"]] == ["black"]


@pytest.mark.parametrize("change", [{"attribute_status": "SEMANTICS_UNKNOWN"},
    {"option_status": "UNKNOWN"}, {"option_key": "white-option"}, {"measurement_target": "SEAT"},
    {"component": "FRAME"}, {"codes": ["WHITE", "BLACK"]}])
def test_gold_unknown_component_and_option_codes_are_not_whole_product_features(tmp_path, change):
    attr = dict(attribute_name="COLOR", codes=["WHITE"], attribute_status="CONFIRMED",
                measurement_target="PRODUCT", option_status="NO_OPTION_CONFIRMED", source_evidence_key="proof")
    attr.update(change)
    product = Product.model_validate(row(tmp_path, "x", attributes_json=[attr]))
    assert product_profile(product).palette == []
    assert compatibility(profile(palette=["white"]), product)["checks"]["palette"]["state"] == "UNKNOWN"


def test_gold_confirmed_codes_and_conflicts(tmp_path):
    attr = dict(attribute_name="MATERIAL", codes=["WOOD"], attribute_status="CONFIRMED",
                measurement_target="PRODUCT", option_status="NO_OPTION_CONFIRMED", source_evidence_key="proof")
    product = Product.model_validate(row(tmp_path, "x", attributes_json=[attr]))
    assert product_profile(product).materials == ["wood"]
    conflicting = product.model_copy(update={"attributes_json": [attr, {**attr, "codes": ["METAL"]}]})
    assert product_profile(conflicting).materials == []


def test_style_is_not_invented_from_material(tmp_path):
    product = Product.model_validate(row(tmp_path, "wood", material="wood"))
    result = compatibility(profile(styles=["natural"]), product)
    assert result["checks"]["styles"]["state"] == "UNKNOWN"
    product = product.model_copy(update={"interior_profile": profile(styles=["natural"])})
    assert compatibility(profile(styles=["natural"]), product)["score"] == 1


def test_legacy_scalar_cannot_bypass_detailed_unknown_source_status(tmp_path):
    product = Product.model_validate(row(tmp_path, "white", color="white", attributes_json=[
        dict(attribute_name="COLOR", codes=["WHITE"], attribute_status="SEMANTICS_UNKNOWN")]))
    assert product_profile(product).palette == []


def test_room_features_require_evidence():
    with pytest.raises(ValueError, match="evidence"):
        InteriorProfile(palette=["white"])


@pytest.mark.parametrize("weights", [{"palette": 1},
    {"palette": float("nan"), "materials": 0, "shapes": 0, "styles": 0},
    {"palette": .5, "materials": .5, "shapes": .5, "styles": .5}])
def test_incomplete_or_invalid_room_weights_are_rejected(weights):
    with pytest.raises(ValueError, match="Room feature weights"):
        RankingPolicy(room_feature_weights=weights)


def test_application_uses_room_features_in_query_and_saves_explanations(tmp_path):
    seen = []
    class Adapter(DirectDemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            seen.append((kind, prompt))
            if kind == "direct_context":
                context = RecommendationContext(decision="ACCEPTED", category_code="CHAIR",
                    search_terms=["minimal chair"], room_profile=profile(palette=["white"], materials=["wood"]))
                return {"payload": {"text": context.model_dump_json()}, "usage_json": {}, "provider_request_id": None}
            return super().call(kind, prompt, images, schema)
    service = DirectStudio(tmp_path / "runs-root", demo=True, adapter_factory=Adapter)
    try:
        owner = str(uuid4())
        run = service.submit(image_bytes(), None, "의자", [], None, owner, str(uuid4()))
        service.launch(run, owner, "prepare")
        result = wait(service, run, owner)
        assert result["status"] == "COMPLETED"
        query = next(prompt for kind, prompt in seen if kind == "embed_text")
        assert "palette: white" in query and "materials: wood" in query
        ranking = result["recommendations"]
        assert ranking["ranking_version"] == "interior-content-v3"
        assert ranking["items"][0]["interior_match"]["room_features"]["evidence"]
        assert ranking["items"][0]["match_explanation"]
        before = len(seen)
        service.launch(run, owner, "prepare", retry=True)
        wait(service, run, owner)
        assert len(seen) == before
    finally:
        service.pool.shutdown(wait=True)
