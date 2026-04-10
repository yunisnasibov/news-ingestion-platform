from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from curl_cffi import requests as curl_requests

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_oxu_shortlink_article_id,
    normalize_space,
    normalize_url,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


class OxuClient:
    source_name = "oxu.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://oxu.az"
        self.default_image_url = "https://oxu.az/media/img/og-logo.svg"
        self.min_request_interval_seconds = 0.15
        self.retry_sleep_seconds = 1.0
        self.session = curl_requests.Session(impersonate="chrome136")
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
            for candidate in self._discover_from_news_sitemap(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:news-sitemap: {exc}")

        try:
            for candidate in self._discover_from_homepage_batches(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage-batches: {exc}")

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
        if not self._is_azerbaijani_article_url(canonical_url):
            raise ValueError(f"Unexpected Oxu article url: {canonical_url}")

        short_link = self._extract_short_link(soup)
        source_article_id = extract_oxu_shortlink_article_id(short_link)
        if source_article_id is None:
            raise ValueError(f"Missing Oxu short link id for {candidate.url}")

        schema = self._extract_news_article_schema(soup)
        title_node = soup.select_one(".post-detail-title h1") or soup.select_one("h1")
        title = normalize_space(
            title_node.get_text(" ", strip=True) if title_node else str(schema.get("headline", ""))
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        breadcrumb_category = soup.select_one(".breadcrumb .breadcrumb-item.active a span")
        category = normalize_space(
            breadcrumb_category.get_text(" ", strip=True)
            if breadcrumb_category
            else str(schema.get("articleSection", "") or candidate.category or "uncategorized")
        )
        category = category or "uncategorized"

        published_meta = soup.select_one(".post-detail-meta span")
        published_date_raw = normalize_space(
            published_meta.get_text(" ", strip=True) if published_meta else candidate.list_date_text
        )
        published_at = str(
            schema.get("datePublished")
            or schema.get("dateModified")
            or candidate.published_at
            or published_date_raw
        )

        content_container = soup.select_one(".post-detail-content-inner")
        content_text = self._extract_content_text(content_container)
        teaser = self._extract_meta_content(soup, "description")
        if not content_text:
            content_text = teaser or candidate.teaser or title

        og_image = self._extract_meta_property(soup, "og:image")
        header_image = self._extract_image_src(soup.select_one(".post-detail-img img"))
        inner_images = [
            self._extract_image_src(image)
            for image in soup.select(".post-detail-content img.inner-photo")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, header_image, *inner_images, candidate.list_image_url]
        )
        hero_image_url = (
            gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        )
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(soup)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=self._extract_slug(canonical_url),
            url=canonical_url,
            canonical_url=canonical_url,
            title=title,
            category=category,
            published_date_raw=published_date_raw,
            published_at=published_at,
            list_date_text=candidate.list_date_text,
            teaser=teaser or candidate.teaser,
            content_text=content_text,
            hero_image_url=hero_image_url,
            gallery_image_urls=gallery_image_urls,
            video_embed_url=video_embed_url,
            list_image_url=candidate.list_image_url or hero_image_url,
            discovery_sources=sorted(candidate.discovery_sources),
            content_hash=content_hash,
        )

    def _discover_from_news_sitemap(self, page_count: int) -> list[ListingCandidate]:
        response = self._request(
            f"{self.base_url}/news-sitemap.xml",
            timeout=self.settings.request_timeout_seconds,
        )
        root = ET.fromstring(response.text)
        candidates: list[ListingCandidate] = []
        limit = max(page_count * 20, 60)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue
            article_url = normalize_url(loc_node.text)
            if not self._is_azerbaijani_article_url(article_url):
                continue

            title_node = url_node.find("news:news/news:title", SITEMAP_NS)
            date_node = url_node.find("news:news/news:publication_date", SITEMAP_NS)
            image_node = url_node.find("image:image/image:loc", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=self._extract_slug(article_url),
                    title=normalize_space(title_node.text) if title_node is not None else "",
                    published_at=date_node.text.strip() if date_node is not None and date_node.text else "",
                    list_image_url=image_node.text.strip()
                    if image_node is not None and image_node.text
                    else "",
                    discovery_sources={"news-sitemap"},
                )
            )
            if len(candidates) >= limit:
                break
        return candidates

    def _discover_from_homepage_batches(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        load_container = soup.select_one(".loadContainer")
        if load_container is None:
            raise ValueError("Missing loadContainer on Oxu homepage")

        data_url = load_container.get("data-url", "").strip()
        if not data_url:
            raise ValueError("Missing Oxu homepage infinity endpoint")

        candidates: list[ListingCandidate] = []
        blocks = soup.select(".index-post-block")
        last_timestamp = self._extend_from_blocks(candidates, blocks, "homepage-batch-1")

        if page_count <= 1:
            return candidates

        for batch_number in range(2, page_count + 1):
            if not last_timestamp:
                break
            response = self._request(
                f"{self.base_url}{data_url}",
                params={"date": last_timestamp, "oldest": "1"},
                timeout=self.settings.request_timeout_seconds,
            )
            batch_soup = BeautifulSoup(response.text, "lxml")
            blocks = batch_soup.select(".index-post-block")
            if not blocks:
                break
            last_timestamp = self._extend_from_blocks(
                candidates,
                blocks,
                f"homepage-batch-{batch_number}",
            )

        return candidates

    def _extend_from_blocks(
        self,
        candidates: list[ListingCandidate],
        blocks: list[Tag],
        label: str,
    ) -> str:
        last_timestamp = ""
        for block in blocks:
            candidate = self._parse_listing_block(block, label)
            if candidate is not None:
                candidates.append(candidate)
            last_timestamp = block.get("data-timestamp", "").strip() or last_timestamp
        return last_timestamp

    def _parse_listing_block(self, block: Tag, label: str) -> ListingCandidate | None:
        href = block.select_one(".post-item-title a[href]")
        if href is None:
            href = block.select_one(".post-item-img a[href]")
        if href is None or not href.get("href"):
            return None

        article_url = normalize_url(urljoin(self.base_url, href["href"].strip()))
        if not self._is_azerbaijani_article_url(article_url):
            return None

        title_node = block.select_one(".post-item-title span")
        category_node = block.select_one(".post-item-category")
        meta_node = block.select_one(".post-item-meta span")
        image_node = block.select_one(".post-item-img img")
        return ListingCandidate(
            url=article_url,
            slug=self._extract_slug(article_url),
            title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
            category=normalize_space(category_node.get_text(" ", strip=True))
            if category_node
            else "",
            list_date_text=normalize_space(meta_node.get_text(" ", strip=True))
            if meta_node
            else "",
            list_image_url=self._extract_image_src(image_node),
            discovery_sources={label},
        )

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self._request(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        return BeautifulSoup(response.text, "lxml"), normalize_url(str(response.url))

    def _request(self, url: str, **kwargs):
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = self.session.get(url, **kwargs)
                if response.status_code >= 500:
                    raise ValueError(f"HTTP Error {response.status_code}: {response.reason}")
                time.sleep(self.min_request_interval_seconds)
                return response
            except Exception as exc:
                last_error = exc
                if attempt == 3:
                    break
                time.sleep(self.retry_sleep_seconds * (attempt + 1))
        if last_error is None:
            raise RuntimeError(f"Oxu request failed for {url}")
        raise last_error

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(node["href"].strip())
        return normalize_url(final_url)

    def _extract_short_link(self, soup: BeautifulSoup) -> str:
        node = soup.select_one(".short-url-btn[data-short-link]")
        if node and node.get("data-short-link"):
            return node["data-short-link"].strip()
        return ""

    def _extract_meta_property(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return node["content"].strip()
        return ""

    def _extract_meta_content(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"])
        return ""

    def _extract_image_src(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        src = image_node.get("src", "").strip()
        if not src or src.startswith("data:image"):
            return ""
        return src

    def _extract_content_text(self, container: Tag | None) -> str:
        if container is None:
            return ""
        paragraphs: list[str] = []
        for paragraph in container.find_all("p"):
            text = normalize_space(paragraph.get_text(" ", strip=True))
            if not text:
                continue
            paragraphs.append(text)
        return "\n\n".join(paragraphs).strip()

    def _extract_video_embed_url(self, soup: BeautifulSoup) -> str:
        video_meta = soup.select_one(
            '[itemtype="https://schema.org/VideoObject"] meta[itemprop="url"]'
        )
        if video_meta and video_meta.get("content"):
            return video_meta["content"].strip()

        iframe = soup.select_one("iframe[src]")
        if iframe and iframe.get("src"):
            return iframe["src"].strip()

        trigger = soup.select_one(".player-area a[onclick]")
        if trigger and trigger.get("onclick"):
            match = re.search(r"'([A-Za-z0-9_-]{6,})'", trigger["onclick"])
            if match:
                return f"https://www.youtube.com/embed/{match.group(1)}"
        return ""

    def _extract_news_article_schema(self, soup: BeautifulSoup) -> dict[str, Any]:
        for node in soup.find_all("script", type="application/ld+json"):
            raw_value = node.get_text(strip=True)
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                continue
            articles = self._iter_schema_articles(payload)
            for article in articles:
                if article.get("@type") == "NewsArticle":
                    return article
        return {}

    def _iter_schema_articles(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            items: list[dict[str, Any]] = []
            for item in payload:
                items.extend(self._iter_schema_articles(item))
            return items
        if isinstance(payload, dict):
            if "@graph" in payload:
                return self._iter_schema_articles(payload["@graph"])
            return [payload]
        return []

    def _is_azerbaijani_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in {"oxu.az", "www.oxu.az"}:
            return False
        path = parsed.path.rstrip("/")
        if not path or path in {"", "/"}:
            return False
        return not (path.startswith("/ru/") or path.startswith("/tr/"))

    @staticmethod
    def _extract_slug(url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        return path.rsplit("/", 1)[-1]

    @staticmethod
    def _merge_candidate(
        candidates: dict[str, ListingCandidate],
        candidate: ListingCandidate,
    ) -> None:
        key = normalize_url(candidate.url)
        if key in candidates:
            candidates[key].merge(candidate)
        else:
            candidates[key] = candidate
