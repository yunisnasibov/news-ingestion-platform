from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "news-ingestor"
    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    database_url: str = Field(
        default="postgresql+asyncpg://news_ingestor:news_ingestor@localhost:5432/news_ingestor",
        alias="DATABASE_URL",
    )

    telegram_api_id: int = Field(alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(alias="TELEGRAM_API_HASH")
    telegram_phone: str = Field(alias="TELEGRAM_PHONE")
    telegram_session_name: str = Field(default="news_ingestor", alias="TELEGRAM_SESSION_NAME")
    telegram_session_dir: Path = Field(default=Path("./state/telethon"), alias="TELEGRAM_SESSION_DIR")
    telegram_backfill_limit: int = Field(default=200, alias="TELEGRAM_BACKFILL_LIMIT")
    telegram_audit_size: int = Field(default=10, alias="TELEGRAM_AUDIT_SIZE")
    telegram_refresh_seconds: int = Field(default=30, alias="TELEGRAM_REFRESH_SECONDS")

    control_api_host: str = Field(default="0.0.0.0", alias="CONTROL_API_HOST")
    control_api_port: int = Field(default=8080, alias="CONTROL_API_PORT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @property
    def telethon_session_path(self) -> Path:
        return self.telegram_session_dir / self.telegram_session_name


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.telegram_session_dir.mkdir(parents=True, exist_ok=True)
    return settings

