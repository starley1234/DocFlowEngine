"""Настройки приложения. Значения читаются из окружения и необязательного .env."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

EMBEDDING_DIMENSIONS = 1024


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DOCFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    environment: Literal["development", "production"] = "development"
    database_url: str = "postgresql+asyncpg://docflow:docflow_dev_only@localhost:5432/docflow"
    storage_root: Path = Path("storage")
    pipeline_dir: Path = Path("config/pipelines")
    default_pipeline: str = "incoming_letter"
    api_key: SecretStr | None = None

    max_upload_bytes: int = Field(default=20 * 1024 * 1024, ge=1)
    max_pdf_pages: int = Field(default=100, ge=1, le=1000)
    max_image_pixels: int = Field(default=40_000_000, ge=1)
    conversion_timeout: int = Field(default=120, ge=1)
    max_llm_input_chars: int = Field(default=60_000, ge=1000)

    provider_timeout: float = Field(default=120, gt=0)
    provider_max_response_bytes: int = Field(default=5 * 1024 * 1024, ge=1024)
    ocr_url: HttpUrl = HttpUrl("http://localhost:8001/predict")
    llm_base_url: HttpUrl = HttpUrl("http://localhost:11434/v1")
    llm_api_key: SecretStr | None = None
    llm_model: str = "qwen2.5:14b"
    embedding_base_url: HttpUrl | None = None
    embedding_api_key: SecretStr | None = None
    embedding_model: str = "bge-m3"
    embedding_dimensions: Literal[1024] = 1024

    worker_poll_interval: float = Field(default=1.0, gt=0)
    worker_lease_seconds: int = Field(default=300, ge=30)
    worker_heartbeat_seconds: float = Field(default=30, gt=0)
    max_retries: int = Field(default=3, ge=1, le=10)
    retry_base_delay: float = Field(default=5, ge=0)
    retry_max_delay: float = Field(default=300, ge=0)

    folder_scan_limit: int = Field(default=1000, ge=1)
    odata_feed_url: HttpUrl | None = None
    odata_username: str | None = None
    odata_password: SecretStr | None = None
    odata_page_size: int = Field(default=50, ge=1, le=500)
    odata_max_documents: int = Field(default=1000, ge=1)
    log_level: str = "INFO"

    @field_validator("api_key", "llm_api_key", "embedding_api_key", "odata_password", mode="before")
    @classmethod
    def empty_secret_is_none(cls, value: object) -> object:
        return None if value == "" else value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        url = make_url(value)
        if url.drivername != "postgresql+asyncpg":
            raise ValueError("database_url must use postgresql+asyncpg")
        return value

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        value = value.upper()
        if value not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("unsupported log level")
        return value

    @model_validator(mode="after")
    def validate_constraints(self) -> "Settings":
        if self.worker_heartbeat_seconds >= self.worker_lease_seconds / 2:
            raise ValueError("heartbeat interval must be less than half of the lease")
        if self.retry_max_delay < self.retry_base_delay:
            raise ValueError("retry_max_delay must be >= retry_base_delay")
        if self.environment == "production" and (
            self.api_key is None or len(self.api_key.get_secret_value()) < 16
        ):
            raise ValueError("production requires DOCFLOW_API_KEY with at least 16 characters")
        return self

    @property
    def inbox_dir(self) -> Path:
        return self.storage_root / "inbox"

    @property
    def resolved_embedding_base_url(self) -> str:
        return str(self.embedding_base_url or self.llm_base_url).rstrip("/")

    @property
    def resolved_embedding_api_key(self) -> SecretStr | None:
        if self.embedding_api_key is not None:
            return self.embedding_api_key
        if str(self.embedding_base_url or self.llm_base_url) == str(self.llm_base_url):
            return self.llm_api_key
        return None


@lru_cache
def get_settings() -> Settings:
    return Settings()
