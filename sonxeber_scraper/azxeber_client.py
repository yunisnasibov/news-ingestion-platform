from __future__ import annotations

import re
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_azxeber_category_slug,
    extract_azxeber_slug,
    is_valid_azxeber_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
XID_PATTERN = re.compile(r"'xid':\s*'(?P<xid>\d+)'")
CONTROLLER_PATTERN = re.compile(r"'sController':\s*'(?P<controller>[^']+)'")


class AzxeberClient:
    source_name = "azxeber.com"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://azxeber.com"
        self.news_home_url = f"{self.base_url}/az/"
        self.news_listing_url = f"{self.base_url}/az/xeberler/"
        self.default_image_url = "https://azxeber.com/favicon.ico"
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
            for candidate in self._discover_from_sitemap(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:sitemap: {exc}")

        for label, url in self._listing_urls(page_count):
            try:
                page_candidates = self._fetch_listing_candidates(label, url)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")
                continue
            for candidate in page_candidates:
                self._merge_candidate(candidates, candidate)

        try:
            for candidate in self._discover_from_homepage_latest():
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage-latest: {exc}")

        return candidates, errors

    def discover_probe_candidates(
        self,
        max_article_id: int,
        window: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        return {}, []

    def fetch_article(self, candidate: ListingCandidate) -> ArticleRecord:
        soup, final_url, response_text = self._get_page(candidate.url)
        controller = self._extract_controller(response_text)
        source_article_id = self._extract_xid(response_text)
        if controller != "xeber.full-story" or source_article_id <= 0:
            raise ValueError(f"Invalid article page for {candidate.url}")

        canonical_url = (
            self._extract_meta_property_raw(soup, "og:url")
            or self._extract_meta_itemprop(soup, "mainEntityOfPage")
            or final_url
        )
        canonical_url = normalize_url(canonical_url)
        if not is_valid_azxeber_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        article = soup.select_one("article.article-full-story")
        if article is None:
            raise ValueError(f"Missing article body for {candidate.url}")

        title = (
            self._extract_meta_itemprop(soup, "headline")
            or self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(soup)
            or candidate.title
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_visible_category(soup)
            or candidate.category
            or extract_azxeber_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(soup)
        published_at = (
            self._extract_meta_itemprop(soup, "datePublished")
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_name(soup, "description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(article)
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_raw(soup, "og:image")
        hero_image = self._extract_image_url(soup.select_one("div.full-post-image img"))
        body_images = [
            self._extract_image_url(node) for node in article.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, hero_image, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(article)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_azxeber_slug(canonical_url),
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

    def _discover_from_sitemap(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            f"{self.base_url}/sitemap.xml",
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
            if not is_valid_azxeber_article_url(article_url):
                continue

            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_azxeber_slug(article_url),
                    category=extract_azxeber_category_slug(article_url),
                    published_at=normalize_space(lastmod_node.text)
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={"sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("xeberler-page-1", self.news_listing_url)]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"xeberler-page-{page_number}",
                    f"{self.news_listing_url}?page={page_number}",
                )
            )
        return urls

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _, _ = self._get_page(url)
        return self._extract_cards_from_posts_wrap(soup, label)

    def _discover_from_homepage_latest(self) -> list[ListingCandidate]:
        soup, _, _ = self._get_page(self.news_home_url)
        candidates = self._extract_cards_from_posts_wrap(soup, "homepage")

        for item in soup.select("div.post-list-wrap div.post-list-item"):
            anchor = item.select_one("a[href]")
            if anchor is None:
                continue

            article_url = normalize_url(make_absolute_url(self.base_url, anchor.get("href", "").strip()))
            if not is_valid_azxeber_article_url(article_url):
                continue

            title_node = item.select_one("div.post-list-title")
            date_node = item.select_one("div.post-list-date")
            image_node = item.select_one("div.post-list-image img")
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_azxeber_slug(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=extract_azxeber_category_slug(article_url),
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True)) if date_node else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={"homepage-latest"},
                )
            )

        return candidates

    def _extract_cards_from_posts_wrap(
        self,
        soup: BeautifulSoup,
        label: str,
    ) -> list[ListingCandidate]:
        container = soup.select_one("section.posts div.posts-wrap")
        if container is None:
            return []

        candidates: list[ListingCandidate] = []
        for item in container.select("div.post-item"):
            item_classes = set(item.get("class", []))
            if item_classes.intersection({"a-banner", "a-archive"}):
                continue

            anchor = item.select_one("a[href]")
            if anchor is None:
                continue

            article_url = normalize_url(make_absolute_url(self.base_url, anchor.get("href", "").strip()))
            if not is_valid_azxeber_article_url(article_url):
                continue

            title_node = item.select_one(".post-title")
            category_node = item.select_one(".post-category")
            date_node = item.select_one(".post-date")
            image_node = item.select_one(".post-image img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_azxeber_slug(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=normalize_space(category_node.get_text(" ", strip=True))
                    if category_node
                    else extract_azxeber_category_slug(article_url),
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True)) if date_node else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _get_page(self, url: str) -> tuple[BeautifulSoup, str, str]:
        response = self.session.get(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        html = response.text
        return BeautifulSoup(html, "lxml"), normalize_url(response.url), html

    def _extract_controller(self, response_text: str) -> str:
        match = CONTROLLER_PATTERN.search(response_text)
        if not match:
            return ""
        return normalize_space(match.group("controller"))

    def _extract_xid(self, response_text: str) -> int:
        match = XID_PATTERN.search(response_text)
        if not match:
            return 0
        return int(match.group("xid"))

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("h1.full-post-title")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.cat-info div.cat-name")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.cat-info div.c-date")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_meta_itemprop(self, soup: BeautifulSoup, itemprop: str) -> str:
        node = soup.select_one(f'meta[itemprop="{itemprop}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
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
        video = article.select_one("video[src], video source[src]")
        if video and video.get("src"):
            return make_absolute_url(self.base_url, video["src"].strip())
        return ""

    def _extract_content_text(self, article: Tag) -> str:
        paragraphs: list[str] = []
        for child in article.children:
            if not isinstance(child, Tag):
                continue
            if child.name in {"meta", "script", "style", "ins"}:
                continue
            if child.get("itemprop") == "publisher":
                continue
            if child.select_one("iframe[src]"):
                continue

            text = normalize_space(child.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(paragraphs).strip()
        return normalize_space(article.get_text(" ", strip=True))

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
