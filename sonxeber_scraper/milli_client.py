from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_milli_article_id,
    extract_milli_category_slug,
    is_valid_milli_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_date,
    parse_azerbaijani_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class MilliClient:
    source_name = "milli.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://news.milli.az"
        self.sitemap_url = "http://news.milli.az/sitemap_latest.php"
        self.default_image_url = "https://www.milli.az/favicon.ico"
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

        try:
            for candidate in self._discover_from_news_lenti(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:xeber-lenti: {exc}")

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
        if not canonical_url or not is_valid_milli_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_milli_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing Milli article id for {candidate.url}")

        content_container = soup.select_one("div.article_text")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        title = (
            self._extract_meta_itemprop(soup, "headline")
            or self._extract_title(soup)
            or candidate.title
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_meta_property_raw(soup, "article:section")
            or self._extract_visible_category(soup)
            or candidate.category
            or extract_milli_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(soup)
        published_at = (
            self._extract_meta_itemprop(soup, "datePublished")
            or self._extract_meta_property_raw(soup, "article:published_time")
            or self._extract_meta_name(soup, "pubdate")
            or self._parse_visible_datetime(published_date_raw)
            or candidate.published_at
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_property_raw(soup, "og:description")
            or self._extract_meta_name(soup, "description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(content_container)
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        main_image = self._extract_image_url(soup.select_one("img.content-img"))
        article_images = [
            self._extract_image_url(node)
            for node in content_container.select("img")
        ]
        gallery_images = [
            make_absolute_url(self.base_url, node.get("data-src", "").strip())
            for node in soup.select("div.article-gallery li[data-src]")
            if node.get("data-src", "").strip()
        ]
        thumb_images = [
            self._extract_image_url(node)
            for node in soup.select("div.article-gallery img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *article_images, *gallery_images, *thumb_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
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
            self.sitemap_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 75, 150)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue

            article_url = normalize_url(loc_node.text.strip())
            if not is_valid_milli_article_url(article_url):
                continue

            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_milli_article_id(article_url),
                    category=extract_milli_category_slug(article_url),
                    published_at=lastmod_node.text.strip()
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={"latest-sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_news_lenti(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        container = soup.select_one("div.ajax-data-block")
        if container is None:
            return []

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 25, 50)
        for item in container.select("ul.post-list2 > li"):
            title_anchor = item.select_one("strong.title a[href]") or item.select_one("a[href]")
            if title_anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, title_anchor.get("href", "").strip())
            )
            if not is_valid_milli_article_url(article_url):
                continue

            time_node = item.select_one("div.info-block span.time")
            image_node = item.select_one("img.alignleft, img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_milli_article_id(article_url),
                    title=normalize_space(title_anchor.get_text(" ", strip=True)),
                    category=extract_milli_category_slug(article_url),
                    list_date_text=normalize_space(time_node.get_text(" ", strip=True))
                    if time_node
                    else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={"xeber-lenti"},
                )
            )
            if len(candidates) >= limit:
                break

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
        return normalize_url(final_url)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("h1")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("span.category")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.date-info")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _parse_visible_datetime(self, raw_value: str) -> str:
        cleaned = normalize_space(raw_value)
        if not cleaned:
            return ""
        date_part, _, time_part = cleaned.rpartition(" ")
        if not date_part or ":" not in time_part:
            return parse_azerbaijani_date(cleaned) or cleaned
        return parse_azerbaijani_datetime(date_part, time_part) or cleaned

    def _extract_meta_itemprop(self, soup: BeautifulSoup, itemprop: str) -> str:
        node = soup.select_one(f'meta[itemprop="{itemprop}"]')
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

    def _extract_meta_name(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
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

    def _extract_content_text(self, content_container: Tag) -> str:
        paragraphs: list[str] = []
        for node in content_container.find_all("p"):
            text = normalize_space(node.get_text(" ", strip=True))
            if not text:
                continue
            if text == "Milli.Az":
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(paragraphs).strip()
        return normalize_space(content_container.get_text(" ", strip=True))

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
