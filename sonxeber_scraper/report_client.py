from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_report_shortlink_article_id,
    fix_utf8_mojibake,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


class ReportClient:
    source_name = "report.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://report.az"
        self.default_image_url = "https://report.az/assets/images/thunk_600.webp"
        self.min_request_interval_seconds = 0.15
        self.retry_sleep_seconds = 1.0
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
            for candidate in self._discover_from_news_sitemap(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:news-sitemap: {exc}")

        try:
            for candidate in self._discover_from_latest_batches(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:latest-batches: {exc}")

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
            raise ValueError(f"Unexpected Report article url: {canonical_url}")

        short_link = self._extract_short_link(soup)
        source_article_id = extract_report_shortlink_article_id(short_link)
        if source_article_id is None:
            raise ValueError(f"Missing Report short link id for {candidate.url}")

        schema = self._extract_news_article_schema(soup)
        title_node = soup.select_one("h1.section-title") or soup.select_one("h1")
        title = fix_utf8_mojibake(normalize_space(
            title_node.get_text(" ", strip=True) if title_node else str(schema.get("headline", ""))
        ))
        title = title or fix_utf8_mojibake(candidate.title)
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category_node = soup.select_one(".news-detail__head .news__category")
        category = fix_utf8_mojibake(normalize_space(
            category_node.get_text(" ", strip=True)
            if category_node
            else self._extract_schema_breadcrumb_category(soup)
        ))
        if not category:
            category = (
                fix_utf8_mojibake(self._extract_article_section(soup))
                or fix_utf8_mojibake(candidate.category)
                or "uncategorized"
            )

        date_nodes = soup.select(".news-detail__head .news__date li")
        published_date_raw = fix_utf8_mojibake(normalize_space(
            " ".join(node.get_text(" ", strip=True) for node in date_nodes)
        ))
        published_at = (
            self._extract_meta_property(soup, "article:published_time")
            or str(schema.get("datePublished", "")).strip()
            or candidate.published_at
            or published_date_raw
        )

        content_container = soup.select_one(".news-detail__desc")
        content_text = fix_utf8_mojibake(self._extract_content_text(content_container))
        teaser = fix_utf8_mojibake(
            self._extract_meta_content(soup, "description")
            or str(schema.get("description", "")).strip()
        )
        if not content_text:
            content_text = (
                self._extract_schema_article_body(schema)
                or teaser
                or fix_utf8_mojibake(candidate.teaser)
                or title
            )

        og_image = self._extract_meta_property(soup, "og:image")
        main_image = self._extract_image_src(soup.select_one(".news-detail__main-photo img"))
        inner_images = [
            self._extract_image_src(image)
            for image in soup.select(".news-detail__desc img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *inner_images, candidate.list_image_url]
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
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 25, 100)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue

            article_url = normalize_url(loc_node.text.strip())
            if not self._is_azerbaijani_article_url(article_url):
                continue

            language_node = url_node.find("news:news/news:publication/news:language", SITEMAP_NS)
            language = language_node.text.strip().lower() if language_node is not None and language_node.text else ""
            if language and language != "az":
                continue

            title_node = url_node.find("news:news/news:title", SITEMAP_NS)
            date_node = url_node.find("news:news/news:publication_date", SITEMAP_NS)
            image_node = url_node.find("image:image/image:loc", SITEMAP_NS)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=self._extract_slug(article_url),
                    title=fix_utf8_mojibake(normalize_space(title_node.text))
                    if title_node is not None
                    else "",
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

    def _discover_from_latest_batches(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(f"{self.base_url}/son-xeberler")
        load_container = soup.select_one(".loadContainer")
        candidates: list[ListingCandidate] = []

        blocks = soup.select("div.index-post-block")
        last_timestamp = self._extend_from_blocks(candidates, blocks, "latest-batch-1")
        if page_count <= 1:
            return candidates

        if load_container is None:
            return candidates

        data_url = load_container.get("data-url", "").strip()
        if not data_url:
            return candidates

        for batch_number in range(2, page_count + 1):
            if not last_timestamp:
                break

            response = self._request(
                urljoin(self.base_url, data_url),
                params={"date": last_timestamp, "oldest": "1"},
                timeout=self.settings.request_timeout_seconds,
            )
            batch_soup = self._build_soup(response.content)
            blocks = batch_soup.select("div.index-post-block")
            if not blocks:
                break
            last_timestamp = self._extend_from_blocks(
                candidates,
                blocks,
                f"latest-batch-{batch_number}",
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
        anchor = block.select_one("a.news__item[href]")
        if anchor is None or not anchor.get("href"):
            return None

        article_url = normalize_url(make_absolute_url(self.base_url, anchor["href"].strip()))
        if not self._is_azerbaijani_article_url(article_url):
            return None

        date_nodes = block.select(".news__date li")
        date_texts = [fix_utf8_mojibake(normalize_space(node.get_text(" ", strip=True))) for node in date_nodes]
        list_date_text = fix_utf8_mojibake(normalize_space(" ".join(text for text in date_texts if text)))
        published_at = ""
        if len(date_texts) >= 2:
            published_at = parse_azerbaijani_datetime(
                date_texts[0].replace(",", ""),
                date_texts[1],
            )

        title_node = block.select_one(".news__title")
        category_node = block.select_one(".news__category")
        return ListingCandidate(
            url=article_url,
            slug=self._extract_slug(article_url),
            title=fix_utf8_mojibake(normalize_space(title_node.get_text(" ", strip=True)))
            if title_node
            else "",
            category=fix_utf8_mojibake(normalize_space(category_node.get_text(" ", strip=True)))
            if category_node
            else "",
            published_at=published_at,
            list_date_text=list_date_text,
            list_image_url=self._extract_image_src(block.select_one("img")),
            discovery_sources={label},
        )

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self._request(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        return self._build_soup(response.content), normalize_url(response.url)

    def _request(self, url: str, **kwargs) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
                response.encoding = "utf-8"
                time.sleep(self.min_request_interval_seconds)
                return response
            except Exception as exc:
                last_error = exc
                if attempt == 3:
                    break
                time.sleep(self.retry_sleep_seconds * (attempt + 1))
        if last_error is None:
            raise RuntimeError(f"Report request failed for {url}")
        raise last_error

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(node["href"].strip())
        return normalize_url(final_url)

    def _extract_short_link(self, soup: BeautifulSoup) -> str:
        node = soup.select_one(".copy-url[data-url]")
        if node is None or not node.get("data-url"):
            return ""
        raw_value = node["data-url"].strip()
        if raw_value.startswith("http://") or raw_value.startswith("https://"):
            return raw_value
        if raw_value.startswith("report.az/"):
            return f"https://{raw_value}"
        return make_absolute_url(self.base_url, raw_value)

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

    def _extract_article_section(self, soup: BeautifulSoup) -> str:
        value = self._extract_meta_property(soup, "article:section")
        if not value:
            return ""
        return normalize_space(value.split("|", 1)[0])

    def _extract_image_src(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        src = (
            image_node.get("data-src", "").strip()
            or image_node.get("src", "").strip()
        )
        if not src or src.startswith("data:image"):
            return ""
        return make_absolute_url(self.base_url, src)

    def _extract_video_embed_url(self, soup: BeautifulSoup) -> str:
        iframe = soup.select_one(".news-detail__desc iframe[src], iframe[src]")
        if iframe and iframe.get("src"):
            return make_absolute_url(self.base_url, iframe["src"].strip())
        return ""

    def _extract_content_text(self, container: Tag | None) -> str:
        if container is None:
            return ""

        content_soup = BeautifulSoup(str(container), "lxml")
        content = content_soup.select_one(".news-detail__desc") or content_soup
        for selector in (
            "script",
            "style",
            "noscript",
            "iframe",
            "blockquote.twitter-tweet",
            ".tags",
        ):
            for node in content.select(selector):
                node.decompose()

        paragraphs: list[str] = []
        for paragraph in content.find_all("p"):
            text = fix_utf8_mojibake(normalize_space(paragraph.get_text(" ", strip=True)))
            if not text:
                continue
            paragraphs.append(text)

        content_text = "\n\n".join(unique_preserving_order(paragraphs)).strip()
        if content_text:
            return content_text

        return fix_utf8_mojibake(normalize_space(content.get_text(" ", strip=True)))

    def _extract_news_article_schema(self, soup: BeautifulSoup) -> dict[str, Any]:
        for node in soup.find_all("script", type="application/ld+json"):
            raw_value = node.get_text(strip=True)
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                continue
            for item in self._iter_schema_items(payload):
                item_type = item.get("@type")
                if item_type == "NewsArticle":
                    return item
        return {}

    def _extract_schema_breadcrumb_category(self, soup: BeautifulSoup) -> str:
        for node in soup.find_all("script", type="application/ld+json"):
            raw_value = node.get_text(strip=True)
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                continue
            for item in self._iter_schema_items(payload):
                if item.get("@type") != "BreadcrumbList":
                    continue
                elements = item.get("itemListElement", [])
                if not isinstance(elements, list) or not elements:
                    continue
                first = elements[0]
                if isinstance(first, dict):
                    return fix_utf8_mojibake(normalize_space(str(first.get("name", ""))))
        return ""

    def _extract_schema_article_body(self, schema: dict[str, Any]) -> str:
        value = fix_utf8_mojibake(normalize_space(str(schema.get("articleBody", ""))))
        return value

    def _build_soup(self, html_bytes: bytes) -> BeautifulSoup:
        return BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")

    def _iter_schema_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            items: list[dict[str, Any]] = []
            for entry in payload:
                items.extend(self._iter_schema_items(entry))
            return items
        if isinstance(payload, dict):
            if "@graph" in payload:
                return self._iter_schema_items(payload["@graph"])
            return [payload]
        return []

    def _is_azerbaijani_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in {"report.az", "www.report.az"}:
            return False
        path = parsed.path.rstrip("/")
        if not path or path in {"", "/"}:
            return False
        return not (path.startswith("/ru") or path.startswith("/en"))

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
