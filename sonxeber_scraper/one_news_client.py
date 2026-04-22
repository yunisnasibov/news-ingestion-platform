from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_one_news_article_id,
    extract_one_news_slug,
    is_valid_one_news_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_one_news_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


class OneNewsClient:
    source_name = "1news.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://1news.az"
        self.latest_url = f"{self.base_url}/az/lenta/"
        self.sitemap_index_url = f"{self.base_url}/sitemap.xml"
        self.sitemap_az_url = f"{self.base_url}/sitemap_az.xml"
        self.default_image_url = f"{self.base_url}/assets/i/favicons/favicon.ico"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
            errors.append(f"listing:sitemap-az: {exc}")

        for label, url in self._listing_urls(page_count):
            try:
                page_candidates = self._discover_from_latest_page(label, url)
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
        response = self.session.get(
            candidate.url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        if response.status_code in {404, 410}:
            raise ValueError(f"Article unavailable ({response.status_code}) for {candidate.url}")
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")
        final_url = normalize_url(response.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_one_news_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_one_news_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing 1news article id for {candidate.url}")

        article = soup.select_one("article.mainArticle")
        if article is None:
            raise ValueError(f"Missing mainArticle container for {candidate.url}")

        title = (
            self._extract_news_article_schema(soup).get("headline", "")
            or self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(article)
            or candidate.title
        )
        title = normalize_space(str(title))
        if not title or title == "404":
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_visible_category(article)
            or self._extract_schema_breadcrumb_category(soup)
            or candidate.category
            or "Xəbərlər"
        )
        category = normalize_space(category)

        schema = self._extract_news_article_schema(soup)
        published_date_raw = (
            str(schema.get("datePublished", "")).strip()
            or candidate.list_date_text
            or candidate.published_at
        )
        published_at = parse_one_news_datetime(published_date_raw) or candidate.published_at or published_date_raw
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        content_container = article.select_one("div.content")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        teaser = (
            self._extract_meta_name(soup, "Description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(content_container)
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        main_image = self._extract_image_url(content_container.select_one("div.thumb img"))
        body_images = [
            self._extract_image_url(node)
            for node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(content_container)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_one_news_slug(canonical_url),
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

    def _discover_from_news_sitemap(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            self.sitemap_az_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 75, 150)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue

            article_url = normalize_url(loc_node.text.strip())
            if not is_valid_one_news_article_url(article_url):
                continue

            title_node = url_node.find("news:news/news:title", SITEMAP_NS)
            date_node = url_node.find("news:news/news:publication_date", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_one_news_slug(article_url),
                    source_article_id=extract_one_news_article_id(article_url),
                    title=normalize_space(title_node.text) if title_node is not None and title_node.text else "",
                    published_at=normalize_space(date_node.text) if date_node is not None and date_node.text else "",
                    list_date_text=normalize_space(date_node.text) if date_node is not None and date_node.text else "",
                    discovery_sources={"sitemap-az"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("lenta-page-1", self.latest_url)]
        for page_number in range(2, page_count + 1):
            urls.append((f"lenta-page-{page_number}", f"{self.latest_url}?page={page_number}"))
        return urls

    def _discover_from_latest_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for block in soup.select("div.newsList a[href]"):
            article_url = normalize_url(
                make_absolute_url(self.base_url, block.get("href", "").strip())
            )
            if not is_valid_one_news_article_url(article_url):
                continue

            time_node = block.select_one("time.date")
            image_node = block.select_one("figure img")
            title_node = block.select_one("span.title")
            datetime_value = normalize_space(time_node.get("datetime", "")) if time_node else ""
            date_text = normalize_space(time_node.get_text(" ", strip=True)) if time_node else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_one_news_slug(article_url),
                    source_article_id=extract_one_news_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    published_at=parse_one_news_datetime(datetime_value) or datetime_value,
                    list_date_text=date_text or datetime_value,
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
        return BeautifulSoup(response.content, "lxml"), normalize_url(response.url)

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(make_absolute_url(self.base_url, node["href"].strip()))
        og_url = self._extract_meta_property_raw(soup, "og:url")
        return normalize_url(make_absolute_url(self.base_url, og_url or final_url))

    def _extract_title(self, article: Tag) -> str:
        node = article.select_one("h1.title")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, article: Tag) -> str:
        node = article.select_one("a.sectionTitle")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_meta_name(self, soup: BeautifulSoup, name: str) -> str:
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
            value = normalize_space(image_node.get(attribute, ""))
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_video_embed_url(self, content_container: Tag) -> str:
        node = content_container.select_one("iframe[src], video[src], video source[src]")
        if node and node.get("src"):
            return make_absolute_url(self.base_url, node["src"].strip())
        return ""

    def _extract_content_text(self, content_container: Tag) -> str:
        paragraphs: list[str] = []
        for node in content_container.select("p, li, h2, h3, h4, blockquote"):
            classes = " ".join(node.get("class", []))
            if "sectionTitle" in classes:
                continue
            if node.find_parent(class_="thumb") is not None:
                continue
            if node.find_parent(class_="articleBottom") is not None:
                continue
            if node.find_parent(class_="latestNews") is not None:
                continue
            if node.find_parent(class_="fourNews") is not None:
                continue
            if node.find_parent(class_="fiveNews") is not None:
                continue
            if node.find_parent(class_="AdviadNativeVideo") is not None:
                continue
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
        fallback = normalize_space(content_container.get_text(" ", strip=True).replace("\xa0", " "))
        return fallback

    def _extract_news_article_schema(self, soup: BeautifulSoup) -> dict[str, Any]:
        for node in soup.find_all("script", type="application/ld+json"):
            raw_value = node.get_text(strip=True)
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
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

    def _extract_schema_breadcrumb_category(self, soup: BeautifulSoup) -> str:
        for node in soup.find_all("script", type="application/ld+json"):
            raw_value = node.get_text(strip=True)
            if not raw_value:
                continue
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                continue
            category = self._find_breadcrumb_category(payload)
            if category:
                return category
        return ""

    def _find_breadcrumb_category(self, payload: Any) -> str:
        if isinstance(payload, dict):
            if str(payload.get("@type", "")).strip() == "BreadcrumbList":
                elements = payload.get("itemListElement", [])
                if isinstance(elements, list):
                    names = [
                        normalize_space(str(item.get("name", "")))
                        for item in elements
                        if isinstance(item, dict) and normalize_space(str(item.get("name", "")))
                    ]
                    if len(names) >= 2:
                        return names[-2]
            for value in payload.values():
                category = self._find_breadcrumb_category(value)
                if category:
                    return category
        if isinstance(payload, list):
            for item in payload:
                category = self._find_breadcrumb_category(item)
                if category:
                    return category
        return ""

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
