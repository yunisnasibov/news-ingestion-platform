from __future__ import annotations

import re
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_xeberler_article_id,
    extract_xeberler_slug,
    is_valid_xeberler_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_xeberler_datetime,
    sha256_text,
    unique_preserving_order,
)


BACKGROUND_URL_PATTERN = re.compile(r"url\(['\"]?(?P<url>[^'\")]+)['\"]?\)")


class XeberlerClient:
    source_name = "xeberler.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://xeberler.az"
        self.home_url = f"{self.base_url}/new/"
        self.content_url = f"{self.base_url}/new/content/"
        self.rss_url = f"{self.base_url}/new/rss.php"
        self.default_image_url = f"{self.base_url}/new/favicon.ico"
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

        for label, url in self._content_urls(page_count):
            try:
                page_candidates = self._discover_from_content_page(label, url)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")
                continue
            for candidate in page_candidates:
                self._merge_candidate(candidates, candidate)

        try:
            for candidate in self._discover_from_homepage_latest(page_count):
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
        soup, final_url = self._get_soup(candidate.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_xeberler_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            extract_xeberler_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing xeberler article id for {candidate.url}")

        content_container = soup.select_one("div.news-details-all")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        info_container = content_container.find_parent("div", class_="sec-info")
        if info_container is None:
            raise ValueError(f"Missing detail info container for {candidate.url}")

        title = (
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(info_container)
            or candidate.title
        )
        title = normalize_space(title)
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_visible_category(soup)
            or candidate.category
            or "Xəbərlər"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(info_container)
        published_at = (
            parse_xeberler_datetime(published_date_raw)
            or candidate.published_at
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_name(soup, "description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(content_container)
        if not content_text:
            raise ValueError(f"Missing content text for {candidate.url}")

        og_image = self._extract_meta_property_raw(soup, "og:image")
        detail_image = self._extract_background_image_url(soup.select_one("div.detail_img"))
        inline_images = [
            self._extract_image_url(node) for node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, detail_image, *inline_images, candidate.list_image_url]
        )
        hero_image_url = (
            gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        )
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(content_container)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_xeberler_slug(canonical_url) or candidate.slug,
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
            self.rss_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 75, 150)
        for item in root.findall("./channel/item"):
            link = normalize_space(item.findtext("link", default=""))
            article_url = normalize_url(link)
            if not is_valid_xeberler_article_url(article_url):
                continue

            image_url = ""
            enclosure = item.find("enclosure")
            if enclosure is not None and enclosure.attrib.get("url"):
                image_url = normalize_space(enclosure.attrib["url"])

            raw_pub_date = normalize_space(item.findtext("pubDate", default=""))
            title = normalize_space(item.findtext("title", default=""))
            teaser = normalize_space(item.findtext("description", default=""))

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_xeberler_slug(article_url),
                    source_article_id=extract_xeberler_article_id(article_url),
                    title=title,
                    published_at=parse_xeberler_datetime(raw_pub_date) or raw_pub_date,
                    list_date_text=raw_pub_date,
                    teaser=teaser,
                    list_image_url=image_url,
                    discovery_sources={"rss"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _content_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("content-page-1", self.content_url)]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"content-page-{page_number}",
                    f"{self.content_url}all/{page_number}",
                )
            )
        return urls

    def _discover_from_content_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for card in soup.select("div.sec-topic"):
            anchor = card.select_one("a[href]")
            if anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, anchor.get("href", "").strip())
            )
            if not is_valid_xeberler_article_url(article_url):
                continue

            title_node = card.select_one("div.sec-info h3")
            time_node = card.select_one("div.text-danger div.time")
            image_node = card.select_one("img.cat-big-img, img")
            raw_date = self._extract_time_text(time_node)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_xeberler_slug(article_url),
                    source_article_id=extract_xeberler_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    published_at=parse_xeberler_datetime(raw_date) or raw_date,
                    list_date_text=raw_date,
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _discover_from_homepage_latest(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.home_url)
        candidates: list[ListingCandidate] = []
        limit = max(page_count * 15, 30)

        for anchor in soup.select("ul#js-news li.news-item a[href]"):
            article_url = normalize_url(
                make_absolute_url(self.base_url, anchor.get("href", "").strip())
            )
            if not is_valid_xeberler_article_url(article_url):
                continue

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_xeberler_slug(article_url),
                    source_article_id=extract_xeberler_article_id(article_url),
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    discovery_sources={"homepage-latest"},
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
        og_url = self._extract_meta_property_raw(soup, "og:url")
        return normalize_url(make_absolute_url(self.base_url, og_url or final_url))

    def _extract_title(self, info_container: Tag) -> str:
        node = info_container.find("h3", recursive=False)
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.page-header h1")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        breadcrumb = soup.select_one("ol.breadcrumb li.active")
        if breadcrumb:
            return normalize_space(breadcrumb.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, info_container: Tag) -> str:
        node = info_container.select_one("div.text-danger.sub-info-bordered div.time")
        return self._extract_time_text(node)

    def _extract_time_text(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

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

    def _extract_background_image_url(self, node: Tag | None) -> str:
        if node is None:
            return ""
        style = normalize_space(node.get("style", ""))
        match = BACKGROUND_URL_PATTERN.search(style)
        if not match:
            return ""
        return make_absolute_url(self.base_url, match.group("url").strip())

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "data-original", "src"):
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
        for node in content_container.select("p, li, blockquote, h4"):
            if node.select_one("script, style, iframe[src], video, ins"):
                continue
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
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
