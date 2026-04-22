from __future__ import annotations

from datetime import datetime

from news_ingestor.db.models import Source
from news_ingestor.schemas import NormalizedNewsPayload
from news_ingestor.services.dedup import DedupService
from news_ingestor.utils.text import (
    canonicalize_url,
    first_non_empty,
    normalize_whitespace,
    sha256_text,
    summarize_text,
)


class NormalizerService:
    def __init__(self):
        self.dedup = DedupService()

    def normalize_telegram_message(self, source: Source, payload: dict) -> NormalizedNewsPayload:
        text = normalize_whitespace(payload.get("text", ""))
        media_type = normalize_whitespace(payload.get("media_type", ""))
        has_media = bool(media_type)
        fallback_title = ""
        fallback_body_text = text
        if not text and has_media:
            fallback_title = f"[telegram media post] {media_type}"
            fallback_body_text = f"[telegram media post without caption] {media_type}"

        title = first_non_empty(payload.get("title", ""), text.split(". ")[0], text[:160], fallback_title)
        body_text = fallback_body_text
        canonical_url = canonicalize_url(payload.get("permalink", ""))
        published_at = payload.get("date") or payload.get("fetched_at")
        if not isinstance(published_at, datetime):
            published_at = payload["fetched_at"]

        missing_fields: list[str] = []
        quality_flags: list[str] = []

        if not title:
            missing_fields.append("title")
            quality_flags.append("title_missing")
        if not body_text:
            missing_fields.append("body_text")
            quality_flags.append("body_missing")
        if has_media and not text:
            quality_flags.append("media_without_caption")

        parse_status = "success"
        if missing_fields:
            parse_status = "partial_success"
        if "title" in missing_fields and "body_text" in missing_fields:
            parse_status = "raw_only"

        normalized = NormalizedNewsPayload(
            source_item_id=str(payload.get("message_id", "")),
            canonical_url=canonical_url,
            title=title,
            body_text=body_text,
            summary=summarize_text(body_text),
            author_name=normalize_whitespace(payload.get("post_author", "")),
            published_at=published_at,
            ingested_at=payload["fetched_at"],
            primary_image_url="",
            image_urls=[],
            language="",
            parse_status=parse_status,
            missing_fields=missing_fields,
            quality_flags=quality_flags,
            content_hash=sha256_text(f"{title}\n{body_text}"),
            dedupe_key="",
        )
        normalized.dedupe_key = self.dedup.build_dedupe_key(normalized)
        return normalized
