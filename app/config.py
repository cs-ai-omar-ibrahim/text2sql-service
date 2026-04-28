from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Resolve `.env` from the repo root (one level above `app/`) regardless of cwd.
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[1] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Azure OpenAI
    azure_openai_endpoint: str
    azure_openai_api_key: str
    azure_openai_deployment: str
    azure_openai_api_version: str

    # Postgres
    database_url: str

    # Schema exposure controls (comma-separated, case-sensitive on DB side).
    # If allowlist is set, only these tables are exposed (in addition to denylist filtering).
    schema_table_allowlist: Optional[str] = None
    schema_table_denylist: Optional[str] = None

    # In-memory schema cache TTL (seconds). Default: 5 minutes.
    schema_cache_ttl_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    return Settings()
