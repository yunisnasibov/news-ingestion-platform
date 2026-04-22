from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_iqtisadiyyat_article_id,
    extract_iqtisadiyyat_slug,
    fix_utf8_mojibake,
    is_valid_iqtisadiyyat_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_iqtisadiyyat_datetime,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)

RAW_CONTENT_PATTERN = re.compile(r'const rawContent = "(?P<value>(?:\\.|[^"\\])*)";', re.S)


class IqtisadiyyatClient:
    source_name = "iqtisadiyyat.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://iqtisadiyyat.az"
        self.latest_url = f"{self.base_url}/az/"
        self.rss_url = f"{self.base_url}/rss.xml"
        self.sitemap_index_url = f"{self.base_url}/sitemap.xml"
        self.default_image_url = f"{self.base_url}/storage/posts/1766988043802-gmez7amthi.png?v=1"
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
            for candidate in self._discover_from_rss(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:rss: {exc}")

        try:
            for candidate in self._discover_from_homepage():
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage: {exc}")

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

        html = response.text
        soup = BeautifulSoup(html, "lxml")
        final_url = normalize_url(response.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_iqtisadiyyat_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_iqtisadiyyat_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing iqtisadiyyat article id for {candidate.url}")

        article = soup.select_one("article")
        schema = self._extract_news_article_schema(soup)

        title = normalize_space(
            fix_utf8_mojibake(
                str(schema.get("headline", ""))
                or self._extract_meta_property(soup, "og:title")
                or self._extract_heading_text(article, "h1")
                or candidate.title
            )
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = normalize_space(
            fix_utf8_mojibake(
                self._extract_visible_category(article)
                or str(schema.get("articleSection", ""))
                or candidate.category
                or "Xəbərlər"
            )
        )

        teaser = normalize_space(
            fix_utf8_mojibake(
                self._extract_meta_name(soup, "description")
                or self._extract_meta_property(soup, "og:description")
                or str(schema.get("description", ""))
                or self._extract_heading_text(article, "h2")
                or candidate.teaser
            )
        )

        published_date_raw = normalize_space(
            self._extract_meta_property(soup, "article:published_time")
            or str(schema.get("datePublished", "")).strip()
            or candidate.list_date_text
            or candidate.published_at
        )
        published_at = (
            parse_iqtisadiyyat_datetime(published_date_raw)
            or parse_rfc2822_datetime(candidate.published_at)
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        content_html = self._extract_raw_content_html(html)
        content_text = self._extract_content_text(content_html)
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property(soup, "og:image")
        main_image = self._extract_image_url(article.select_one("img") if article else None)
        body_image_urls = self._extract_body_image_urls(content_html)
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *body_image_urls, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(content_html, article)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_iqtisadiyyat_slug(canonical_url),
            url=canonical_url,
            canonical_url=canonical_url,
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
            self.rss_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        items = root.findall("./channel/item")
        limit = max(page_count * 25, 50)
        candidates: list[ListingCandidate] = []
        for item in items[:limit]:
            link = normalize_space(item.findtext("link", default=""))
            article_url = normalize_url(link)
            if not article_url or not is_valid_iqtisadiyyat_article_url(article_url):
                continue

            title = normalize_space(fix_utf8_mojibake(item.findtext("title", default="")))
            pub_date = normalize_space(item.findtext("pubDate", default=""))
            description = normalize_space(fix_utf8_mojibake(item.findtext("description", default="")))
            encoded = item.findtext("{http://purl.org/rss/1.0/modules/content/}encoded", default="")
            teaser = self._extract_content_text(encoded) or description

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_iqtisadiyyat_slug(article_url),
                    source_article_id=extract_iqtisadiyyat_article_id(article_url),
                    title=title,
                    published_at=parse_rfc2822_datetime(pub_date) or pub_date,
                    list_date_text=pub_date,
                    teaser=teaser,
                    discovery_sources={"rss"},
                )
            )

        return candidates

    def _discover_from_homepage(self) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.latest_url)
        candidates: list[ListingCandidate] = []

        for block in soup.select("a.news-item[href]"):
            article_url = normalize_url(
                make_absolute_url(self.base_url, block.get("href", "").strip())
            )
            if not is_valid_iqtisadiyyat_article_url(article_url):
                continue

            title_node = block.select_one("h4") or block.select_one("span.font-semibold")
            time_node = block.select_one("time")
            image_node = block.select_one("img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_iqtisadiyyat_slug(article_url),
                    source_article_id=extract_iqtisadiyyat_article_id(article_url),
                    title=normalize_space(
                        fix_utf8_mojibake(title_node.get_text(" ", strip=True))
                    )
                    if title_node
                    else "",
                    list_date_text=normalize_space(time_node.get_text(" ", strip=True)) if time_node else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={"homepage"},
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

    def _merge_candidate(
        self,
        candidates: dict[str, ListingCandidate],
        candidate: ListingCandidate,
    ) -> None:
        key = normalize_url(candidate.url)
        existing = candidates.get(key)
        if existing is None:
            candidates[key] = candidate
            return
        existing.merge(candidate)

    def _is_article_url(self, url: str) -> bool:
        return is_valid_iqtisadiyyat_article_url(url)

    def _extract_article_id(self, url: str) -> int | None:
        return extract_iqtisadiyyat_article_id(url)

    def _extract_slug(self, url: str) -> str:
        return extract_iqtisadiyyat_slug(url)

    def _extract_canonical_url(self, soup: BeautifulSoup, fallback_url: str) -> str:
        canonical = soup.find("link", rel="canonical")
        href = canonical.get("href", "").strip() if canonical else ""
        return normalize_url(href or fallback_url)

    def _extract_news_article_schema(self, soup: BeautifulSoup) -> dict[str, Any]:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(" ", strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            nodes = payload if isinstance(payload, list) else [payload]
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                if node.get("@type") == "NewsArticle":
                    return node
        return {}

    def _extract_meta_property(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[property="{name}"]')
        if node is None:
            return ""
        return normalize_space(node.get("content", ""))

    def _extract_meta_name(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node is None:
            return ""
        return normalize_space(node.get("content", ""))

    def _extract_heading_text(self, article: Tag | None, selector: str) -> str:
        if article is None:
            return ""
        node = article.select_one(selector)
        if node is None:
            return ""
        return normalize_space(fix_utf8_mojibake(node.get_text(" ", strip=True)))

    def _extract_visible_category(self, article: Tag | None) -> str:
        if article is None:
            return ""
        for node in article.select('a[href^="/az/category/"]'):
            text = normalize_space(node.get_text(" ", strip=True))
            if text:
                return fix_utf8_mojibake(text)
        return ""

    def _extract_raw_content_html(self, html: str) -> str:
        match = RAW_CONTENT_PATTERN.search(html)
        if not match:
            return ""
        raw_value = match.group("value")
        try:
            decoded = json.loads(f'"{raw_value}"')
        except json.JSONDecodeError:
            return ""
        return decoded.replace("\xa0", " ").strip()

    def _extract_content_text(self, html_fragment: str) -> str:
        if not html_fragment:
            return ""
        fragment_soup = BeautifulSoup(html_fragment, "lxml")
        for tag in fragment_soup(["script", "style", "noscript"]):
            tag.decompose()
        return normalize_space(fix_utf8_mojibake(fragment_soup.get_text(" ", strip=True)))

    def _extract_body_image_urls(self, html_fragment: str) -> list[str]:
        if not html_fragment:
            return []
        fragment_soup = BeautifulSoup(html_fragment, "lxml")
        return unique_preserving_order(
            [
                self._extract_image_url(node)
                for node in fragment_soup.select("img")
            ]
        )

    def _extract_video_embed_url(self, html_fragment: str, article: Tag | None) -> str:
        if html_fragment:
            fragment_soup = BeautifulSoup(html_fragment, "lxml")
            iframe_url = self._extract_iframe_url(fragment_soup.select_one("iframe"))
            if iframe_url:
                return iframe_url
        if article is None:
            return ""
        return self._extract_iframe_url(article.select_one("iframe"))

    def _extract_image_url(self, node: Tag | None) -> str:
        if node is None:
            return ""
        for attr in ("src", "data-src", "data-lazy-src"):
            value = normalize_space(node.get(attr, ""))
            if value:
                return normalize_url(make_absolute_url(self.base_url, value))
        return ""

    def _extract_iframe_url(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return normalize_url(make_absolute_url(self.base_url, node.get("src", "").strip()))
