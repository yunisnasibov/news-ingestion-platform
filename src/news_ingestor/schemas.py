from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class RawIngestPayload:
    source_item_id: str
    source_event_type: str
    fetched_at: datetime
    observed_at: datetime
    raw_payload: dict[str, Any]
    raw_html: str = ""
    raw_text: str = ""
    origin_url: str = ""
    content_hash: str = ""
    primary_image_url: str = ""
    image_urls: list[str] = field(default_factory=list)
    parser_error: str = ""
    parse_status: str = "pending"


@dataclass(slots=True)
class NormalizedNewsPayload:
    source_item_id: str
    canonical_url: str
    title: str
    body_text: str
    summary: str
    author_name: str
    published_at: datetime
    ingested_at: datetime
    primary_image_url: str
    image_urls: list[str]
    language: str
    parse_status: str
    missing_fields: list[str]
    quality_flags: list[str]
    content_hash: str
    dedupe_key: str


@dataclass(slots=True)
class AuditPayload:
    audit_type: str
    live_latest_item_id: str
    db_latest_item_id: str
    status: str
    details: dict[str, Any]
