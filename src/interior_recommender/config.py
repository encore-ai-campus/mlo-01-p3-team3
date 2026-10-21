import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field


ENV_FILE = Path(__file__).resolve().parent / ".env"


def load_environment():
    """Load the MVP's own secret file, independent of the working directory."""
    return load_dotenv(dotenv_path=ENV_FILE, override=False, encoding="utf-8-sig", interpolate=False)


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    analysis_model: str = "gemini-3.5-flash-lite"
    image_provider: Literal["cloudflare", "gemini"] = "cloudflare"
    image_model: str = "@cf/black-forest-labs/flux-2-klein-4b"
    output_width: Literal[512, 1024] = 512
    output_height: Literal[512, 1024] = 512
    generation_preprocessing_version: str = "rgb-fit480-square-pad-v1"
    embedding_model: str = "gemini-embedding-2"
    dimension: int = Field(default=768, ge=128, le=3072)
    timeout_seconds: float = Field(default=120, gt=0, le=600)
    max_attempts: int = Field(default=3, ge=1, le=3)
    preprocessing_version: str = "exif-rgb-fit1024-png-l2-v1"

    @classmethod
    def from_env(cls):
        load_environment()
        fields = {
            "analysis_model": "AI_ANALYSIS_MODEL", "image_model": "AI_IMAGE_MODEL",
            "embedding_model": "AI_EMBEDDING_MODEL", "dimension": "AI_EMBEDDING_DIMENSION",
            "timeout_seconds": "AI_TIMEOUT_SECONDS", "max_attempts": "AI_MAX_ATTEMPTS",
            "image_provider": "AI_IMAGE_PROVIDER", "output_width": "AI_OUTPUT_WIDTH",
            "output_height": "AI_OUTPUT_HEIGHT",
        }
        values = {key: os.environ[env] for key, env in fields.items() if env in os.environ}
        for key in ("output_width", "output_height"):
            if key in values:
                values[key] = int(values[key])
        return cls(**values)
