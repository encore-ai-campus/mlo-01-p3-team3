import io
import json
from collections import Counter
from uuid import uuid4

import pytest
from PIL import Image

from interior_recommender.application import preview
from interior_recommender.application.direct import DirectDemoAdapter, DirectStudio
from interior_recommender.assets import read_json, write_json
from interior_recommender.pipeline import Pipeline


@pytest.fixture
def prepared(tmp_path):
    calls, prompts = Counter(), {}

    class Offline(DirectDemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            calls[kind] += 1
            prompts[kind] = (prompt, images)
            return super().call(kind, prompt, images, schema)

    engine = DirectStudio(tmp_path, demo=True, adapter_factory=Offline)
    calls.clear()
    owner = str(uuid4())
    output = io.BytesIO()
    Image.new("RGB", (100, 50), "white").save(output, "PNG")
    parent = engine.submit(output.getvalue(), None, "의자", [],
                           {"x_min": 0, "y_min": 0, "x_max": 1, "y_max": 1}, owner, str(uuid4()))
    pipeline = Pipeline(engine.directory(parent, owner), engine.factory)
    engine.prepare(pipeline)
    state = engine.state(parent, owner)
    yield engine, owner, parent, state["recommendations"]["items"][0], calls, prompts
    engine.pool.shutdown(wait=True)


def test_preview_reuses_selected_snapshot_and_receipt_after_local_failure(prepared, monkeypatch):
    engine, owner, parent, item, calls, prompts = prepared
    child = engine.submit_preview(parent, owner, item["image_id"])
    directory = engine.directory(child, owner)
    p = Pipeline(directory, engine.factory)
    preview.validate(p, True)
    original = p.persist_asset

    def fail(*args):
        raise OSError("local persistence failure after API response")

    monkeypatch.setattr(p, "persist_asset", fail)
    with pytest.raises(ValueError):
        preview.prepare(p)
    assert (directory / "receipts/preview.json").exists()
    monkeypatch.setattr(p, "persist_asset", original)
    preview.prepare(p)
    assert engine.state(child, owner)["result_image"] == "generated.png"
    assert calls == {"direct_context": 1, "embed_text": 1, "generate": 1}
    assert engine.submit_preview(parent, owner, item["image_id"]) == child
    prompt, images = prompts["generate"]
    assert len(images) == 3  # Room, product and room with the placement guide.
    assert 'every visible part' in prompt
    assert 'Do not zoom, crop' in prompt
    assert "Image 2 is the selected product" in prompt
    assert "For a chair, preserve the backrest and seat outlines" in prompt
    assert "For a desk or table, preserve the tabletop outline" in prompt
    assert "For a bed, preserve the exact headboard and footboard outlines" in prompt
    assert "Never stretch, squash, bend, crop or shorten structural parts" in prompt
    assert "primary layout constraint" in prompt
    data = json.loads(prompt[prompt.index('{"product_name"'):])
    assert data["placement_region"] == {"x_min": 0, "y_min": .25, "x_max": 1, "y_max": .75}
    assert data['placement_target']['room_normalized_box'] == {'x_min':0, 'y_min':0, 'x_max':1, 'y_max':1}
    assert data['placement_target']['reference_pixel_box'] == {'x_min':0, 'y_min':25, 'x_max':100, 'y_max':75}
    assert data['placement_target']['reference_bottom_anchor'] == {'x':.5, 'y':.75}
    assert data['placement_target']['output_pixel_box'] == {'x_min':0, 'y_min':128, 'x_max':512, 'y_max':384}
    assert data['placement_target']['guide_image_number'] == 3
    assert read_json(directory / "manifest.json")["request"]["parent_run_id"] == parent


def test_preview_rejects_changed_product_and_foreign_owner(prepared):
    engine, owner, parent, item, calls, _ = prepared
    with pytest.raises(PermissionError):
        engine.submit_preview(parent, str(uuid4()), item["image_id"])
    with pytest.raises(FileNotFoundError):
        engine.submit_preview(parent, owner, "arbitrary image URL")
    directory = engine.directory(parent, owner)
    (directory / item["image"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        engine.submit_preview(parent, owner, item["image_id"])
    assert calls["generate"] == 0


def test_changed_prompt_policy_creates_a_new_preview_without_overwriting_old_result(prepared, monkeypatch):
    engine, owner, parent, item, calls, prompts = prepared
    current_policy = preview.PROMPT_POLICY
    monkeypatch.setattr(preview, "PROMPT_POLICY", "legacy-test-policy")
    old_child = engine.submit_preview(parent, owner, item["image_id"])
    old_directory = engine.directory(old_child, owner)
    preview.prepare(Pipeline(old_directory, engine.factory))
    old_manifest = (old_directory / "manifest.json").read_bytes()
    monkeypatch.setattr(preview, "PROMPT_POLICY", current_policy)
    new_child = engine.submit_preview(parent, owner, item["image_id"])
    assert new_child != old_child
    assert (old_directory / "manifest.json").read_bytes() == old_manifest
    assert read_json(engine.directory(new_child, owner) / "manifest.json")["preview_contract"]["prompt_policy"] == preview.PROMPT_POLICY
    assert calls["generate"] == 1  # New admission does not call a provider.


def test_legacy_preview_keeps_its_original_prompt_and_remains_readable(prepared):
    engine, owner, parent, item, calls, prompts = prepared
    child = engine.submit_preview(parent, owner, item["image_id"])
    directory = engine.directory(child, owner)
    manifest = read_json(directory / "manifest.json")
    manifest["preview_contract"] = {"version": preview.VERSION}
    manifest["input_hash"] = preview.contract_hash(manifest)
    write_json(directory / "manifest.json", manifest)
    pipeline = Pipeline(directory, engine.factory)
    preview.validate(pipeline, True)
    preview.prepare(pipeline)
    assert "place ONE matching product" in prompts["generate"][0]
    assert "For a bed" not in prompts["generate"][0]
    assert engine.state(child, owner)["result_image"] == "generated.png"


def test_geometry_v2_preview_keeps_its_original_placement_policy(prepared):
    engine, owner, parent, item, calls, prompts = prepared
    child = engine.submit_preview(parent, owner, item['image_id'])
    directory = engine.directory(child, owner)
    manifest = read_json(directory / 'manifest.json')
    manifest['preview_contract']['prompt_policy'] = preview.GEOMETRY_POLICY_V2
    manifest['input_hash'] = preview.contract_hash(manifest)
    write_json(directory / 'manifest.json', manifest)
    pipeline = Pipeline(directory, engine.factory)
    preview.validate(pipeline, True)
    preview.prepare(pipeline)
    prompt = prompts['generate'][0]
    assert 'soft location hint' in prompt
    assert 'placement_target' not in prompt


def test_placement_v3_keeps_its_original_prompt_and_reference_count(prepared):
    engine, owner, parent, item, calls, prompts = prepared
    child = engine.submit_preview(parent, owner, item['image_id'])
    directory = engine.directory(child, owner)
    manifest = read_json(directory / 'manifest.json')
    manifest['preview_contract']['prompt_policy'] = preview.PLACEMENT_POLICY_V3
    manifest['input_hash'] = preview.contract_hash(manifest)
    write_json(directory / 'manifest.json', manifest)
    pipeline = Pipeline(directory, engine.factory)
    preview.validate(pipeline, True)
    preview.prepare(pipeline)
    prompt, images = prompts['generate']
    assert 'as large as possible inside' in prompt
    assert 'MAGENTA' not in prompt
    assert len(images) == 2


def test_small_region_guide_uses_room_padding_and_does_not_enlarge_the_box(prepared):
    engine, owner, parent, item, calls, prompts = prepared
    child = engine.submit_preview(parent, owner, item['image_id'])
    directory = engine.directory(child, owner)
    manifest = read_json(directory / 'manifest.json')
    manifest['request']['placement_region'] = {'x_min':.2, 'y_min':.4, 'x_max':.5, 'y_max':.55}
    # Exercise all four supported references, with the guide after the optional sketch.
    manifest['request']['sketch_image'] = 'inputs/room.png'
    manifest['input_hash'] = preview.contract_hash(manifest)
    write_json(directory / 'manifest.json', manifest)
    preview.prepare(Pipeline(directory, engine.factory))
    prompt, images = prompts['generate']
    target = json.loads(prompt[prompt.index('{"product_name"'):])['placement_target']
    assert len(images) == target['guide_image_number'] == 4
    assert target['maximum_canvas_width_percent'] == 30
    assert target['maximum_canvas_height_percent'] == 7.5
    assert 'as large as possible inside' not in prompt
    with Image.open(io.BytesIO(images[-1])) as guide:
        assert guide.size == (100, 100)
        assert guide.getpixel((20, 45)) == (255, 0, 255)
        assert guide.getpixel((50, 52)) == (255, 0, 255)
        assert guide.getpixel((19, 45)) == (255, 255, 255)
        assert guide.getpixel((35, 48)) == (255, 255, 255)


def test_preview_policy_tampering_is_rejected(prepared):
    engine, owner, parent, item, calls, _ = prepared
    child = engine.submit_preview(parent, owner, item["image_id"])
    directory = engine.directory(child, owner)
    manifest = read_json(directory / "manifest.json")
    manifest["preview_contract"]["version"] = "changed"
    write_json(directory / "manifest.json", manifest)
    with pytest.raises(ValueError):
        preview.validate(Pipeline(directory, engine.factory), True)
    assert calls["generate"] == 0
