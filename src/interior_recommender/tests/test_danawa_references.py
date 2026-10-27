from collections import Counter
from uuid import uuid4

import pytest

from interior_recommender.assets import read_json
from interior_recommender.batch_embedding import BatchEmbedding, create_batch
from interior_recommender.config import Settings
from interior_recommender.tests.test_pipeline import FakeAdapter, image_bytes
from interior_recommender.tests.test_web_workflow import wait
from interior_recommender.web.danawa import DanawaCatalog
from interior_recommender.web.workflow import DemoAdapter, Studio


@pytest.fixture
def saved_index(tmp_path):
    source = tmp_path / "images"
    source.mkdir()
    for i in range(7):
        (source / f"chair-{i}.png").write_bytes(image_bytes((100 + i, 80)))
    output = create_batch(source, tmp_path / "saved", Settings())
    adapter = FakeAdapter()
    original = adapter.call

    def embed(*args):
        response = original(*args)
        response["payload"]["values"] = [float(i == adapter.calls["embed"] - 1) for i in range(768)]
        return response

    adapter.call = embed
    BatchEmbedding(output, lambda _: adapter).run(source, Settings(), requests_per_minute=1000000, progress=lambda _: None)
    return output / "index.json"


def test_saved_vectors_rank_and_survive_selection_without_image_embedding(saved_index, tmp_path):
    catalog = DanawaCatalog(saved_index)
    desired = catalog.templates[-1]["id"]
    calls = Counter()

    class QueryAdapter(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            if kind == "embed":
                pytest.fail("Saved Danawa images must not be embedded again")
            if kind == "embed_text":
                return {"payload": {"values": catalog.vectors[desired]}, "usage_json": {}, "provider_request_id": None}
            return super().call(kind, *args, **kwargs)

    service = Studio(tmp_path / "web", demo=True, adapter_factory=QueryAdapter, danawa_index=saved_index)
    owner = str(uuid4())
    try:
        run_id = service.submit(image_bytes(), None, "검은색 의자", [], None, owner, str(uuid4()))
        service.launch(run_id, owner, "prepare")
        state = wait(service, run_id, owner)
        assert state["reference_catalog"]["source"] == "danawa"
        assert len(state["candidates"]) == 6
        assert state["candidates"][0]["template_id"] == desired
        assert state["candidates"][0]["score"] == pytest.approx(1)
        assert all(c["vector_reused"] for c in state["candidates"])
        assert all(c["template_id"] != "chair-01" for c in state["candidates"])
        assert all(c["attributes"]["category_verified"] == "false" for c in state["candidates"])
        # External corpus mutations cannot affect a run's frozen input vectors/images.
        external_image = catalog.templates[-1]["image"]
        from pathlib import Path
        Path(external_image).write_bytes(b"changed after submission")
        child = service.clone_selection(run_id, owner, desired)
        service.launch(child, owner, "prepare")
        preview = wait(service, child, owner)
        assert preview["status"] == "READY_TO_GENERATE"
        manifest = read_json(service.directory(child, owner) / "manifest.json")
        assert manifest["selected_template"]["id"] == desired
        assert len(preview["preview"]["images"]) == 2
        service.launch(child, owner, "generate")
        assert wait(service, child, owner)["status"] == "COMPLETED"
        assert calls["embed"] == 0
        assert calls["generate"] == 1
    finally:
        service.pool.shutdown(wait=True)


def test_corrupt_completed_embedding_is_excluded_and_model_mismatch_rejected(saved_index):
    catalog = DanawaCatalog(saved_index)
    with pytest.raises(ValueError, match="모델"):
        catalog.validate_settings(Settings(dimension=128))
    index = read_json(saved_index)
    (saved_index.parent / index["files"][0]["embedding_file"]).write_text('{"values": []}')
    loaded = DanawaCatalog(saved_index)
    assert len(loaded.templates) == 6
    assert loaded.skipped == 1
