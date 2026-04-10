from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    AZERBAIJAN_TZ,
    extract_ikisahil_article_id,
    extract_ikisahil_slug,
    is_valid_ikisahil_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_datetime,
    sha256_text,
    unique_preserving_order,
)


class IkiSahilClient:
    source_name = "ikisahil.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://ikisahil.az"
        self.default_image_url = (
            "https://ikisahil.az/photo/800x500_2/upload/2021/06/18/-b58f7d18162400563645964034510193167.jpg"
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept-Language": "az,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    def discover_listing_candidates(
        self,
        page_count: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        candidates: dict[str, ListingCandidate] = {}
        errors: list[str] = []

        try:
            for candidate in self._discover_from_rss(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:rss: {exc}")

        for label, url in self._listing_urls(page_count):
            try:
                page_candidates = self._fetch_listing_candidates(label, url)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")
                continue
            for candidate in page_candidates:
                self._merge_candidate(candidates, candidate)

        return candidates, errors

    def discover_probe_candidates(
        self,
        max_article_id: int,
        window: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        return {}, []

    def fetch_article(self, candidate: ListingCandidate) -> ArticleRecord:
        soup, final_url = self._get_soup(candidate.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_ikisahil_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            self._extract_current_post_id(soup)
            or extract_ikisahil_article_id(canonical_url)
            or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing IkiSahil article id for {candidate.url}")

        article = soup.select_one("div.content.lead") or soup.select_one("div.content")
        if article is None:
            raise ValueError(f"Missing article content for {candidate.url}")

        title_node = soup.select_one("h1") or soup.select_one("h2.my-4") or soup.select_one("h2")
        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        title = title or candidate.title
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = self._extract_active_category(soup) or candidate.category or "uncategorized"
        category = normalize_space(category)

        visible_date_text = self._extract_visible_date_text(soup)
        schema = self._extract_news_article_schema(soup)
        schema_published = str(schema.get("datePublished", "")).strip()
        published_at = (
            parse_azerbaijani_datetime(visible_date_text, visible_date_text)
            or self._parse_schema_datetime(schema_published)
            or candidate.published_at
            or visible_date_text
            or schema_published
        )
        published_date_raw = visible_date_text or schema_published or candidate.list_date_text
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_content(soup, "description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(article) or teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        featured_image = self._extract_image_url(soup.select_one("div.featured-image img"))
        body_images = [self._extract_image_url(node) for node in article.select("img")]
        gallery_image_urls = unique_preserving_order(
            [og_image, featured_image, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(article)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_ikisahil_slug(canonical_url) or candidate.slug,
            url=normalize_url(canonical_url),
            canonical_url=normalize_url(canonical_url),
            title=title,
            category=category,
            published_date_raw=published_date_raw,
            published_at=published_at,
            list_date_text=candidate.list_date_text,
            teaser=teaser,
            content_text=content_text,
            hero_image_url=hero_image_url,
            gallery_image_urls=gallery_image_urls,
            video_embed_url=video_embed_url,
            list_image_url=candidate.list_image_url or hero_image_url,
            discovery_sources=sorted(candidate.discovery_sources),
            content_hash=content_hash,
        )

    def _discover_from_rss(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            f"{self.base_url}/rss",
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 30, 120)
        for item in root.findall("./channel/item"):
            link_node = item.find("link")
            if link_node is None or not link_node.text:
                continue

            article_url = normalize_url(link_node.text.strip())
            if not is_valid_ikisahil_article_url(article_url):
                continue
            slug = extract_ikisahil_slug(article_url)
            if slug and slug[0].isdigit():
                # RSS emits slug-only links, and numeric-leading slugs can resolve to homepage.
                # We rely on `/lent` pagination for those items because it carries the stable id URL.
                continue

            title_node = item.find("title")
            enclosure_node = item.find("enclosure")
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=slug,
                    title=normalize_space(title_node.text) if title_node is not None and title_node.text else "",
                    list_image_url=enclosure_node.get("url", "").strip() if enclosure_node is not None else "",
                    discovery_sources={"rss"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("lent-page-1", f"{self.base_url}/lent")]
        for page_number in range(2, page_count + 1):
            urls.append((f"lent-page-{page_number}", f"{self.base_url}/lent/p-{page_number}"))
        return urls

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, final_url = self._get_soup(url)
        if normalize_url(final_url) != normalize_url(url):
            return []

        container = soup.select_one("div.article-container")
        if container is None:
            return []

        candidates: list[ListingCandidate] = []
        for row in container.select("div.row.my-4"):
            anchor = row.select_one("div.col-md-9 > a[href]") or row.select_one("div.col-md-3 a[href]")
            if anchor is None:
                continue

            article_url = normalize_url(make_absolute_url(self.base_url, anchor.get("href", "").strip()))
            if not is_valid_ikisahil_article_url(article_url):
                continue

            title_node = row.select_one("div.col-md-9 > a[href] span") or anchor
            badge_node = row.select_one("a.badge")
            date_node = row.select_one("small.text-secondary")
            image_node = row.select_one("div.col-md-3 img")
            date_text = normalize_space(date_node.get_text(" ", strip=True)) if date_node else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_ikisahil_slug(article_url),
                    source_article_id=extract_ikisahil_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=normalize_space(badge_node.get_text(" ", strip=True)) if badge_node else "",
                    published_at=parse_azerbaijani_datetime(date_text, date_text) if date_text else "",
                    list_date_text=date_text,
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self.session.get(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml"), normalize_url(response.url)

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(make_absolute_url(self.base_url, node["href"].strip()))
        return normalize_url(final_url)

    def _extract_current_post_id(self, soup: BeautifulSoup) -> int | None:
        for script in soup.find_all("script"):
            script_text = script.get_text("\n", strip=True)
            if "currentPostId" not in script_text:
                continue
            marker = "currentPostId ="
            if marker not in script_text:
                continue
            suffix = script_text.split(marker, 1)[1].strip().lstrip()
            digits: list[str] = []
            for char in suffix:
                if char.isdigit():
                    digits.append(char)
                    continue
                if digits:
                    break
            if digits:
                return int("".join(digits))
        return None

    def _extract_active_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one('#categoryMenu li.nav-item.active a[href^="/cat/"]')
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date_text(self, soup: BeautifulSoup) -> str:
        for node in soup.select("div.bg-light.p-2.text-secondary div.col-auto"):
            if node.select_one("i.fa-calendar-alt"):
                return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_news_article_schema(self, soup: BeautifulSoup) -> dict[str, Any]:
        for node in soup.select('script[type="application/ld+json"]'):
            raw = node.string or node.get_text("\n", strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            schema = self._find_news_article_schema(payload)
            if schema:
                return schema
        return {}

    def _find_news_article_schema(self, payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            schema_type = str(payload.get("@type", "")).strip().lower()
            if schema_type == "newsarticle":
                return payload
            for value in payload.values():
                schema = self._find_news_article_schema(value)
                if schema:
                    return schema
        if isinstance(payload, list):
            for item in payload:
                schema = self._find_news_article_schema(item)
                if schema:
                    return schema
        return {}

    def _parse_schema_datetime(self, raw_value: str) -> str:
        cleaned = normalize_space(raw_value)
        if not cleaned:
            return ""
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(cleaned, fmt).replace(tzinfo=AZERBAIJAN_TZ).isoformat()
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            return ""
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=AZERBAIJAN_TZ)
        return parsed.isoformat()

    def _extract_meta_content(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_meta_property_raw(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_meta_property_url(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return make_absolute_url(self.base_url, node["content"].strip())
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            value = image_node.get(attribute, "").strip()
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_video_embed_url(self, article: Tag) -> str:
        iframe = article.select_one("iframe[src]")
        if iframe and iframe.get("src"):
            return make_absolute_url(self.base_url, iframe["src"].strip())
        return ""

    def _extract_content_text(self, article: Tag) -> str:
        paragraphs: list[str] = []
        for child in article.children:
            if not isinstance(child, Tag):
                continue
            if child.name != "p":
                continue
            text = normalize_space(child.get_text(" ", strip=True))
            if text:
                paragraphs.append(text)

        content_text = "\n\n".join(paragraphs).strip()
        if content_text:
            return content_text
        return normalize_space(article.get_text(" ", strip=True))

    @staticmethod
    def _merge_candidate(
        candidates: dict[str, ListingCandidate],
        candidate: ListingCandidate,
    ) -> None:
        key = candidate.slug or normalize_url(candidate.url)
        if key in candidates:
            candidates[key].merge(candidate)
        else:
            candidates[key] = candidate
