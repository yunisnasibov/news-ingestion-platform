from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_teleqraf_article_id,
    extract_teleqraf_category_slug,
    is_valid_teleqraf_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class TeleqrafClient:
    source_name = "teleqraf.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://teleqraf.az"
        self.default_image_url = "https://teleqraf.az/assets/img/logo.svg"
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
            for candidate in self._discover_from_latest_sitemap(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:latest-sitemap: {exc}")

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
        if not canonical_url or not is_valid_teleqraf_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            extract_teleqraf_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing Teleqraf article id for {candidate.url}")

        content_title = soup.select_one("h1.content-title")
        if content_title is None:
            raise ValueError(f"Missing title for {candidate.url}")
        title = normalize_space(content_title.get_text(" ", strip=True)) or candidate.title
        if not title:
            raise ValueError(f"Empty title for {candidate.url}")

        category = (
            self._extract_meta_property_raw(soup, "article:section")
            or self._extract_breadcrumb_category(soup)
            or candidate.category
            or extract_teleqraf_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_at = (
            self._extract_meta_property_raw(soup, "article:published_time")
            or self._extract_meta_name(soup, "pubdate")
            or candidate.published_at
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        published_date_raw = published_at

        main_content = soup.select_one("div.main-content")
        if main_content is None:
            raise ValueError(f"Missing main content for {candidate.url}")

        content_text = self._extract_content_text(main_content)
        teaser = (
            self._extract_meta_property_raw(soup, "og:description")
            or self._extract_meta_name(soup, "description")
            or candidate.teaser
        )
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        main_image = self._extract_image_url(soup.select_one("div.image-main-content img"))
        body_images = [
            self._extract_image_url(node) for node in soup.select("div.main-content img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *body_images, candidate.list_image_url]
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
            slug="",
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

    def _discover_from_latest_sitemap(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            f"{self.base_url}/sitemap_latest.php",
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 25, 100)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue
            article_url = normalize_url(loc_node.text.strip())
            if not is_valid_teleqraf_article_url(article_url):
                continue

            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_teleqraf_article_id(article_url),
                    published_at=lastmod_node.text.strip()
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={"latest-sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("latest-page-1", f"{self.base_url}/latest/")]
        for page_number in range(2, page_count + 1):
            urls.append((f"latest-page-{page_number}", f"{self.base_url}/latest/page{page_number}/"))
        return urls

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for anchor in soup.select("div.news-boxes-4 a.box[href]"):
            article_url = normalize_url(anchor.get("href", "").strip())
            if not is_valid_teleqraf_article_url(article_url):
                continue

            title_node = anchor.select_one("div.title")
            time_node = anchor.select_one("div.time")
            image_node = anchor.select_one("div.image img")
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_teleqraf_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True))
                    if title_node
                    else "",
                    list_date_text=normalize_space(time_node.get_text(" ", strip=True))
                    if time_node
                    else "",
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
            return normalize_url(node["href"].strip())
        return normalize_url(final_url)

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

    def _extract_meta_name(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_breadcrumb_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.breadcrumbs ul li:last-child a span")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            value = image_node.get(attribute, "").strip()
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_video_embed_url(self, soup: BeautifulSoup) -> str:
        iframe = soup.select_one("iframe[src]")
        if iframe and iframe.get("src"):
            return make_absolute_url(self.base_url, iframe["src"].strip())
        return ""

    def _extract_content_text(self, main_content: Tag) -> str:
        paragraphs: list[str] = []
        for child in main_content.children:
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
        return normalize_space(main_content.get_text(" ", strip=True))

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
