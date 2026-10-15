import pytest

from interior_recommender import config


ENV_KEYS = ("GEMINI_API_KEY", "AI_ANALYSIS_MODEL", "AI_IMAGE_MODEL", "AI_EMBEDDING_MODEL",
            "AI_EMBEDDING_DIMENSION", "AI_TIMEOUT_SECONDS", "AI_MAX_ATTEMPTS", "AI_IMAGE_PROVIDER",
            "AI_OUTPUT_WIDTH", "AI_OUTPUT_HEIGHT", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN")


@pytest.fixture(autouse=True)
def isolated_dotenv(tmp_path, monkeypatch):
    """Tests must never load developer credentials or use their model settings."""
    path = tmp_path / ".env"
    path.write_text("", encoding="utf-8")
    monkeypatch.setattr(config, "ENV_FILE", path)
    for key in ENV_KEYS:
        # Record original values even when initially absent, for proper teardown.
        monkeypatch.setenv(key, "")
        monkeypatch.delenv(key)
    monkeypatch.delenv("PYTHON_DOTENV_DISABLED", raising=False)
    return path
