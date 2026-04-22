from __future__ import annotations

import json
from datetime import UTC, datetime

from bs4 import BeautifulSoup

from news_ingestor.utils.text import canonicalize_url, first_non_empty, looks_like_image_url, normalize_whitespace


class GenericWebsiteParser:
    def parse(self, url: str, html: str) -> dict:
        soup = BeautifulSoup(html, "lxml")
        title = first_non_empty(
            self._meta_content(soup, "property", "og:title"),
            self._meta_content(soup, "name", "twitter:title"),
            soup.title.string if soup.title and soup.title.string else "",
            self._first_heading(soup),
        )
        body_text = normalize_whitespace(" ".join(node.get_text(" ", strip=True) for node in soup.select("article p, main p")))
        published_at = self._extract_published_at(soup)
        image_urls = self._extract_image_urls(soup)
        return {
            "canonical_url": canonicalize_url(
                first_non_empty(
                    self._meta_content(soup, "property", "og:url"),
                    url,
                )
            ),
            "title": title,
            "body_text": body_text,
            "summary": body_text[:280],
            "published_at": published_at,
            "primary_image_url": image_urls[0] if image_urls else "",
            "image_urls": image_urls,
        }

    def _meta_content(self, soup: BeautifulSoup, attr: str, key: str) -> str:
        tag = soup.find("meta", attrs={attr: key})
        return normalize_whitespace(tag.get("content", "")) if tag else ""

    def _first_heading(self, soup: BeautifulSoup) -> str:
        heading = soup.find(["h1", "h2"])
        return normalize_whitespace(heading.get_text(" ", strip=True)) if heading else ""

    def _extract_published_at(self, soup: BeautifulSoup) -> datetime:
        candidates = [
            self._meta_content(soup, "property", "article:published_time"),
            self._meta_content(soup, "name", "pubdate"),
            self._json_ld_published_at(soup),
        ]
        for item in candidates:
            if not item:
                continue
            try:
                return datetime.fromisoformat(item.replace("Z", "+00:00"))
            except ValueError:
                continue
        return datetime.now(tz=UTC)

    def _json_ld_published_at(self, soup: BeautifulSoup) -> str:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                payload = json.loads(script.string or "{}")
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return normalize_whitespace(str(payload.get("datePublished", "")))
        return ""

    def _extract_image_urls(self, soup: BeautifulSoup) -> list[str]:
        candidates = []
        meta_candidates = [
            self._meta_content(soup, "property", "og:image"),
            self._meta_content(soup, "name", "twitter:image"),
        ]
        candidates.extend([item for item in meta_candidates if item])
        for image in soup.select("article img, main img, img"):
            for attr in ("src", "data-src", "data-original", "data-lazy-src"):
                value = normalize_whitespace(image.get(attr, ""))
                if value:
                    candidates.append(value)
        return list(dict.fromkeys([item for item in candidates if item and (item in meta_candidates or looks_like_image_url(item))]))
