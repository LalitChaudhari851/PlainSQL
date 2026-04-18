"""
PlainSQL Configuration — Pydantic Settings with environment-based config.
All secrets loaded from .env, with sensible defaults for local dev.
"""

from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from typing import Optional
from functools import lru_cache

_JWT_DEFAULT = "change-me-in-production-use-openssl-rand-hex-32"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # ── App ──────────────────────────────────────────────
    APP_NAME: str = "PlainSQL"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = False
    ENV: str = Field(default="development", description="development | staging | production")

    # ── Database ─────────────────────────────────────────
    DB_URI: str = Field(..., description="MySQL connection URI")
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_QUERY_TIMEOUT: int = 30

    # ── Redis ────────────────────────────────────────────
    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    CACHE_TTL_SECONDS: int = 300

    # ── Authentication ───────────────────────────────────
    JWT_SECRET_KEY: str = Field(default=_JWT_DEFAULT)
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRY_HOURS: int = 8

    # ── LLM Providers ───────────────────────────────────
    HUGGINGFACEHUB_API_TOKEN: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    DEFAULT_LLM_PROVIDER: str = Field(default="huggingface", description="huggingface | openai | anthropic | ollama")
    DEFAULT_MODEL: str = "Qwen/Qwen2.5-Coder-32B-Instruct"

    # ── RAG ──────────────────────────────────────────────
    CHROMA_PERSIST_DIR: str = "./chroma_db"
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    RAG_TOP_K: int = 5

    # ── Safety ───────────────────────────────────────────
    MAX_QUERY_ROWS: int = 1000
    QUERY_TIMEOUT_SECONDS: int = 30

    # ── Rate Limiting ────────────────────────────────────
    RATE_LIMIT_RPM: int = 60

    # ── Observability ────────────────────────────────────
    LANGSMITH_API_KEY: Optional[str] = None
    LANGSMITH_PROJECT: str = "plainsql"
    LOG_LEVEL: str = "INFO"

    # ── CORS ─────────────────────────────────────────────
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:5500"]

    model_config = {
        "env_file": [".env", "../.env", "../../.env"],
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
    }

    @model_validator(mode="after")
    def _validate_production_secrets(self) -> "Settings":
        """Prevent deployment with default secrets in production/staging."""
        if self.ENV in ("production", "staging"):
            if self.JWT_SECRET_KEY == _JWT_DEFAULT:
                raise ValueError(
                    "FATAL: JWT_SECRET_KEY must be changed from the default value "
                    "in production/staging. Generate one with: openssl rand -hex 32"
                )
            if len(self.JWT_SECRET_KEY) < 32:
                raise ValueError(
                    "FATAL: JWT_SECRET_KEY must be at least 32 characters for production."
                )
        return self


@lru_cache()
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
