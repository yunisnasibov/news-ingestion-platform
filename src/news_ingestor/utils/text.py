from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def first_non_empty(*values: str) -> str:
    for value in values:
        cleaned = normalize_whitespace(value)
        if cleaned:
            return cleaned
    return ""


def sha256_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def extract_urls(value: str) -> list[str]:
    return list(dict.fromkeys(URL_RE.findall(value or "")))


def looks_like_image_url(value: str) -> bool:
    lower = (value or "").lower().split("?")[0]
    return lower.endswith(IMAGE_EXTENSIONS)


def canonicalize_url(value: str) -> str:
    raw = normalize_whitespace(value)
    if not raw:
        return ""

    parsed = urlparse(raw)
    query_items = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith(("utm_", "fbclid", "gclid"))
    ]
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
        query=urlencode(query_items),
    )
    return urlunparse(normalized).rstrip("/")


def summarize_text(value: str, limit: int = 280) -> str:
    normalized = normalize_whitespace(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."

