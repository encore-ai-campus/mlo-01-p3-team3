from pathlib import Path

import pytest

from interior_recommender import cli
from interior_recommender.assets import read_json
from interior_recommender.batch_embedding import BatchEmbedding, create_batch, inspect_directory
from interior_recommender.config import Settings
from interior_recommender.tests.test_pipeline import FakeAdapter, image_bytes


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    directory = tmp_path / "images"
    directory.mkdir()
    (directory / "chair.jpg").write_bytes(image_bytes())
    (directory / "duplicate.JPG").write_bytes(image_bytes())
    (directory / "nested").mkdir()
    (directory / "nested/table.png").write_bytes(image_bytes((150, 120)))
    (directory / "notes.txt").write_text("Not an image", encoding="utf-8")
    monkeypatch.setattr("interior_recommender.batch_embedding.time.sleep", lambda _: None)
    return directory


def test_dry_run_checks_images_without_model_or_outputs(corpus, tmp_path, monkeypatch, capsys):
    def forbidden(_):
        raise AssertionError("Dry-run must not initialize any API client")
    monkeypatch.setattr(cli, "ModelAdapter", forbidden)
    output = tmp_path / "vectors"
    assert cli.main(["embed-directory", "--input-dir", str(corpus), "--output-dir", str(output), "--dry-run"]) == 0
    assert not output.exists()
    assert '"unique_images": 2' in capsys.readouterr().out


def test_batch_deduplicates_and_resumes_after_limit(corpus, tmp_path):
    output = create_batch(corpus, tmp_path / "vectors", Settings())
    adapter = FakeAdapter()
    batch = BatchEmbedding(output, lambda _: adapter)
    result = batch.run(corpus, Settings(), limit=1, progress=lambda _: None)
    assert result["status"] == "PARTIAL"
    assert adapter.calls["embed"] == 1
    # Original directory may disappear; the saved inputs remain the fixed batch input.
    (corpus / "chair.jpg").unlink()
    result = BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert result["status"] == "SUCCEEDED"
    assert result["completed_images"] == 3
    assert adapter.calls["embed"] == 2
    index = read_json(output / "index.json")
    assert len(index["files"]) == 3
    assert index["files"][0]["embedding_file"] == index["files"][1]["embedding_file"]
    for record in index["files"]:
        vector = read_json(output / record["embedding_file"])
        assert vector["dimension"] == 768
        assert sum(v * v for v in vector["values"]) == pytest.approx(1)
    BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert adapter.calls["embed"] == 2


def test_corrupt_file_is_recorded_without_blocking_good_files(corpus, tmp_path):
    (corpus / "bad.jpg").write_bytes(b"corrupt")
    assert len(inspect_directory(corpus)["errors"]) == 1
    output = create_batch(corpus, tmp_path / "vectors", Settings())
    adapter = FakeAdapter()
    result = BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert result["status"] == "COMPLETED_WITH_INPUT_ERRORS"
    assert result["input_errors"] == 1
    assert adapter.calls["embed"] == 2


def test_batch_reuses_provider_receipt_after_local_storage_failure(corpus, tmp_path, monkeypatch):
    from interior_recommender import pipeline
    output = create_batch(corpus, tmp_path / "vectors", Settings())
    adapter = FakeAdapter()
    real_write = pipeline.write_json
    failed = []
    def failing_write(path, value):
        if Path(path).name.startswith("embed-") and Path(path).parent == output and not failed:
            failed.append(True)
            raise OSError("Simulated disk failure")
        return real_write(path, value)
    monkeypatch.setattr(pipeline, "write_json", failing_write)
    with pytest.raises(ValueError):
        BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert adapter.calls["embed"] == 1
    monkeypatch.setattr(pipeline, "write_json", real_write)
    BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert adapter.calls["embed"] == 2


def test_batch_stops_on_api_error_and_resumes(corpus, tmp_path):
    output = create_batch(corpus, tmp_path / "vectors", Settings())
    adapter = FakeAdapter()
    original = adapter.call
    def failure(*args):
        raise ValueError("API request failed")
    adapter.call = failure
    with pytest.raises(ValueError):
        BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)
    assert all(r["status"] != "SUCCEEDED" for r in read_json(output / "index.json")["files"])
    adapter.call = original
    assert BatchEmbedding(output, lambda _: adapter).run(corpus, Settings(), progress=lambda _: None)["status"] == "SUCCEEDED"


def test_model_changes_require_separate_output(corpus, tmp_path):
    output = create_batch(corpus, tmp_path / "vectors", Settings())
    with pytest.raises(ValueError, match="settings changed"):
        BatchEmbedding(output, lambda _: FakeAdapter()).run(corpus, Settings(dimension=1536), progress=lambda _: None)
