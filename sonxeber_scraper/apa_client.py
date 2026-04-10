from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_apa_article_id,
    extract_apa_category_slug,
    extract_apa_slug,
    is_valid_apa_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_apa_datetime,
    parse_azerbaijani_datetime,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


class ApaClient:
    source_name = "apa.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://apa.az"
        self.rss_url = f"{self.base_url}/rss"
        self.all_news_url = f"{self.base_url}/all-news"
        self.default_image_url = f"{self.base_url}/site/assets/images/favicon-16x16.png"
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

        for label, url in self._all_news_urls(page_count):
            try:
                page_candidates = self._discover_from_all_news_page(label, url)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")
                continue
            for candidate in page_candidates:
                self._merge_candidate(candidates, candidate)

        try:
            for candidate in self._discover_from_homepage_news_lenti(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage-news-lenti: {exc}")

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
        if not canonical_url or not is_valid_apa_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_apa_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing APA article id for {candidate.url}")

        content_container = soup.select_one("div.news_content div.texts")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        title = (
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(soup)
            or candidate.title
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_visible_category(soup)
            or candidate.category
            or extract_apa_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(soup)
        published_at = (
            parse_apa_datetime(published_date_raw)
            or candidate.published_at
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
            content_text = teaser or title

        og_image = self._extract_meta_property_raw(soup, "og:image")
        main_image = self._extract_image_url(soup.select_one("div.content_main div.main_img img"))
        body_images = [
            self._extract_image_url(node) for node in content_container.select("img")
        ]
        gallery_images = [
            make_absolute_url(self.base_url, node.get("href", "").strip())
            for node in soup.select("div.news_in_gallery a[href]")
            if node.get("href", "").strip()
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *gallery_images, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(soup)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_apa_slug(canonical_url),
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
            if not is_valid_apa_article_url(article_url):
                continue

            image_url = ""
            enclosure = item.find("enclosure")
            if enclosure is not None and enclosure.attrib.get("url"):
                image_url = normalize_space(enclosure.attrib["url"])

            pub_date = normalize_space(item.findtext("pubDate", default=""))
            category = normalize_space(item.findtext("category", default=""))
            title = normalize_space(item.findtext("title", default=""))
            teaser = normalize_space(item.findtext("description", default=""))

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_apa_slug(article_url),
                    source_article_id=extract_apa_article_id(article_url),
                    title=title,
                    category=category,
                    published_at=parse_rfc2822_datetime(pub_date) or pub_date,
                    list_date_text=pub_date,
                    teaser=teaser,
                    list_image_url=image_url,
                    discovery_sources={"rss"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _all_news_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("all-news-page-1", self.all_news_url)]
        for page_number in range(2, page_count + 1):
            urls.append((f"all-news-page-{page_number}", f"{self.all_news_url}?page={page_number}"))
        return urls

    def _discover_from_all_news_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for anchor in soup.select("div.four_columns_block a.item.news-item[data-news-id]"):
            href = normalize_space(anchor.get("href", ""))
            article_url = normalize_url(make_absolute_url(self.base_url, href))
            if not is_valid_apa_article_url(article_url):
                continue

            source_article_id = self._safe_int(anchor.get("data-news-id", ""))
            image_node = anchor.select_one("div.img img")
            title_node = anchor.select_one("div.content h2.title")
            date_node = anchor.select_one("div.content div.date")
            list_date_text, published_at = self._extract_card_date_values(date_node)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_apa_slug(article_url),
                    source_article_id=source_article_id or extract_apa_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=extract_apa_category_slug(article_url),
                    published_at=published_at,
                    list_date_text=list_date_text,
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _discover_from_homepage_news_lenti(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        candidates: list[ListingCandidate] = []
        limit = max(page_count * 20, 40)

        for anchor in soup.select("div.main_index div.sidebar div.news_block div.news > a.item"):
            href = normalize_space(anchor.get("href", ""))
            article_url = normalize_url(make_absolute_url(self.base_url, href))
            if not is_valid_apa_article_url(article_url):
                continue

            title_node = anchor.select_one("p.lent-title")
            date_node = anchor.select_one("div.date")
            list_date_text, published_at = self._extract_card_date_values(date_node)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_apa_slug(article_url),
                    source_article_id=extract_apa_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=extract_apa_category_slug(article_url),
                    published_at=published_at,
                    list_date_text=list_date_text,
                    discovery_sources={"homepage-news-lenti"},
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
            return normalize_url(node["href"].strip())
        og_url = self._extract_meta_property_raw(soup, "og:url")
        return normalize_url(og_url or final_url)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.content_main h2.title_news")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.breadcrumb_row h1")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.date_news span.date")
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

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            value = normalize_space(image_node.get(attribute, ""))
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_card_date_values(self, date_node: Tag | None) -> tuple[str, str]:
        if date_node is None:
            return "", ""
        spans = [normalize_space(node.get_text(" ", strip=True)) for node in date_node.select("span")]
        if len(spans) >= 2:
            raw_value = f"{spans[1]} {spans[0]}"
            return raw_value, parse_azerbaijani_datetime(spans[1], spans[0])
        if spans:
            raw_value = spans[0]
            return raw_value, parse_apa_datetime(raw_value)
        return "", ""

    def _extract_video_embed_url(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.news_content iframe[src], div.news_content video[src], div.news_content video source[src]")
        if node and node.get("src"):
            return make_absolute_url(self.base_url, node["src"].strip())
        return ""

    def _extract_content_text(self, content_container: Tag) -> str:
        paragraphs: list[str] = []
        for node in content_container.select("p, div, li, h3, h4"):
            if node.get("class") and "links_block" in node.get("class", []):
                continue
            if node.select_one("script, style, ins, iframe[src]"):
                continue

            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
        return normalize_space(content_container.get_text(" ", strip=True))

    @staticmethod
    def _safe_int(value: str) -> int | None:
        try:
            return int(value.strip())
        except (AttributeError, ValueError):
            return None

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
