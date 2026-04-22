from __future__ import annotations

from telethon import TelegramClient

from news_ingestor.settings import get_settings


def build_client() -> TelegramClient:
    settings = get_settings()
    return TelegramClient(
        str(settings.telethon_session_path),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
