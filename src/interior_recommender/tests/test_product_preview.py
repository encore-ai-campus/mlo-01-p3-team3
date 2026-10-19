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
    assert len(images) == 2
    assert "Image 2 is the selected product" in prompt
    data = json.loads(prompt[prompt.index('{"product_name"'):])
    assert data["placement_region"] == {"x_min": 0, "y_min": .25, "x_max": 1, "y_max": .75}
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
