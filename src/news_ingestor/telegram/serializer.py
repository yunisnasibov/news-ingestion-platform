from __future__ import annotations

from telethon.tl.types import MessageEntityTextUrl

from news_ingestor.utils.json import json_safe
from news_ingestor.utils.text import extract_urls, looks_like_image_url, normalize_whitespace


def build_telegram_message_url(source_identifier: str, message_id: int) -> str:
    identifier = (source_identifier or "").strip()
    if not identifier or message_id <= 0:
        return ""

    if identifier.startswith("peer:"):
        peer_value = identifier.split(":", 1)[1].strip()
        digits = str(abs(int(peer_value)))
        internal_id = digits[3:] if digits.startswith("100") else digits
        return f"https://t.me/c/{internal_id}/{message_id}"

    return f"https://t.me/{identifier}/{message_id}"


def serialize_message(message, *, source_identifier: str) -> dict:
    raw_dict = message.to_dict() if hasattr(message, "to_dict") else {"id": getattr(message, "id", 0)}
    text = normalize_whitespace(getattr(message, "raw_text", "") or getattr(message, "message", ""))
    message_urls = extract_urls(text)

    for entity in getattr(message, "entities", []) or []:
        if isinstance(entity, MessageEntityTextUrl):
            message_urls.append(entity.url)

    webpage = getattr(getattr(message, "media", None), "webpage", None)
    if webpage and getattr(webpage, "url", ""):
        message_urls.append(webpage.url)

    image_urls = [url for url in dict.fromkeys(message_urls) if looks_like_image_url(url)]
    permalink = build_telegram_message_url(source_identifier, getattr(message, "id", 0))

    return {
        "message_type": type(message).__name__,
        "message_id": getattr(message, "id", 0),
        "date": getattr(message, "date", None),
        "edit_date": getattr(message, "edit_date", None),
        "text": text,
        "post_author": normalize_whitespace(getattr(message, "post_author", "")),
        "media_type": type(getattr(message, "media", None)).__name__ if getattr(message, "media", None) else "",
        "urls": list(dict.fromkeys(message_urls)),
        "image_urls": image_urls,
        "permalink": permalink,
        "raw": json_safe(raw_dict),
    }
