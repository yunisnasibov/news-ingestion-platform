from __future__ import annotations

from news_ingestor.schemas import NormalizedNewsPayload
from news_ingestor.utils.text import canonicalize_url, normalize_whitespace, sha256_text


class DedupService:
    def build_dedupe_key(self, payload: NormalizedNewsPayload) -> str:
        canonical_url = canonicalize_url(payload.canonical_url)
        if canonical_url:
            return f"url:{canonical_url}"
        if payload.content_hash:
            return f"hash:{payload.content_hash}"
        title_key = normalize_whitespace(payload.title).lower()
        published_key = payload.published_at.date().isoformat()
        return f"sim:{sha256_text(f'{title_key}|{published_key}')}"
