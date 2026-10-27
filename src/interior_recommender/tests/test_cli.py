import json
from pathlib import Path

from interior_recommender import cli
from interior_recommender.assets import read_json, write_json
from interior_recommender.tests.test_pipeline import FakeAdapter, image_bytes


def prepare(tmp_path):
    (tmp_path / "room.png").write_bytes(image_bytes())
    (tmp_path / "template.png").write_bytes(image_bytes())
    request = tmp_path / "request.json"
    templates = tmp_path / "templates.json"
    write_json(request, {"room_image": "room.png", "text": "chair", "template_id": "chair-01"})
    write_json(templates, {"templates": [{"id": "chair-01", "version": 1,
               "category_code": "CHAIR", "image": "template.png", "review_status": "APPROVED",
               "active": True, "provenance": "fixture"}]})
    return ["start", "--request", str(request), "--templates", str(templates),
            "--output-root", str(tmp_path / "outputs")]


def test_cli_start_select_embed(tmp_path, monkeypatch, capsys):
    adapter = FakeAdapter()
    monkeypatch.setattr(cli, "ModelAdapter", lambda _: adapter)
    assert cli.main(prepare(tmp_path)) == 0
    output = capsys.readouterr().out.splitlines()
    result = json.loads(output[-1])
    assert result["status"] == "NEEDS_OBJECT_SELECTION"
    run = Path(result["run_dir"])
    obj = read_json(run / "analyze.json")["objects"][0]
    assert cli.main(["embed", "--run-dir", str(run), "--object-id", obj["id"]]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "SUCCEEDED"


def test_cli_missing_key_is_actionable_and_preserves_run(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    assert cli.main(prepare(tmp_path)) == 2
    captured = capsys.readouterr()
    assert "GEMINI_API_KEY is not configured" in captured.err
    run = Path(captured.out.splitlines()[0].removeprefix("run_dir="))
    assert read_json(run / "manifest.json")["status"] == "FAILED"


def test_cli_embed_requires_selection(capsys):
    assert cli.main(["embed", "--run-dir", "unused"]) == 2
    assert "--object-id is required" in capsys.readouterr().err


def test_default_template_manifest_is_resolved_from_installed_package(tmp_path, monkeypatch, capsys):
    args = prepare(tmp_path)
    request = args[args.index("--request") + 1]
    monkeypatch.setattr(cli, "ModelAdapter", lambda _: FakeAdapter())
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert cli.main(["start", "--request", request, "--output-root", str(tmp_path / "runs"),
                     "--until", "interpret"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "INTERPRETED"
