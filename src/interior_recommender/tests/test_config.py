import os
from pathlib import Path
from unittest.mock import patch

from interior_recommender import cli
from interior_recommender.adapters.gemini import GeminiAdapter
from interior_recommender.assets import read_json
from interior_recommender.config import Settings
from interior_recommender.tests.test_cli import prepare
from interior_recommender.tests.test_pipeline import FakeAdapter


def test_load_dotenv_from_module_path_with_bom(isolated_dotenv, tmp_path, monkeypatch):
    isolated_dotenv.write_text("GEMINI_API_KEY=unit-test-key\nAI_EMBEDDING_DIMENSION=1536\n",
                               encoding="utf-8-sig")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / ".env").write_text("GEMINI_API_KEY=wrong-cwd-key\n", encoding="utf-8")
    monkeypatch.chdir(elsewhere)
    settings = Settings.from_env()
    assert os.getenv("GEMINI_API_KEY") == "unit-test-key"
    assert settings.dimension == 1536


def test_shell_environment_has_priority(isolated_dotenv, monkeypatch):
    isolated_dotenv.write_text("GEMINI_API_KEY=file-key\nAI_EMBEDDING_DIMENSION=1536\n", encoding="utf-8")
    monkeypatch.setenv("GEMINI_API_KEY", "shell-key")
    monkeypatch.setenv("AI_EMBEDDING_DIMENSION", "768")
    assert Settings.from_env().dimension == 768
    assert os.getenv("GEMINI_API_KEY") == "shell-key"


def test_adapter_reads_key_without_cli(isolated_dotenv):
    isolated_dotenv.write_text("GEMINI_API_KEY=adapter-test-key\n", encoding="utf-8")
    with patch("interior_recommender.adapters.gemini.genai.Client") as client:
        adapter = GeminiAdapter(Settings())
        assert client.call_args.kwargs["api_key"] == "adapter-test-key"
        adapter.close()


def test_cli_start_and_resume_load_key_without_recording_it(isolated_dotenv, tmp_path, monkeypatch, capsys):
    isolated_dotenv.write_text("GEMINI_API_KEY=start-test-key\nAI_TIMEOUT_SECONDS=60\n", encoding="utf-8")
    adapter = FakeAdapter()
    observed_keys = []

    def factory(settings):
        observed_keys.append(os.getenv("GEMINI_API_KEY"))
        return adapter

    monkeypatch.setattr(cli, "ModelAdapter", factory)
    assert cli.main(prepare(tmp_path) + ["--until", "interpret"]) == 0
    run = Path(capsys.readouterr().out.splitlines()[0].removeprefix("run_dir="))
    assert read_json(run / "manifest.json")["settings"]["timeout_seconds"] == 60
    monkeypatch.delenv("GEMINI_API_KEY")
    isolated_dotenv.write_text("GEMINI_API_KEY=resume-test-key\nAI_TIMEOUT_SECONDS=30\n", encoding="utf-8")
    assert cli.main(["resume", "--run-dir", str(run), "--until", "generate"]) == 0
    assert observed_keys == ["start-test-key", "resume-test-key"]
    manifest_text = (run / "manifest.json").read_text(encoding="utf-8")
    assert "start-test-key" not in manifest_text and "resume-test-key" not in manifest_text
    assert read_json(run / "manifest.json")["settings"]["timeout_seconds"] == 60
