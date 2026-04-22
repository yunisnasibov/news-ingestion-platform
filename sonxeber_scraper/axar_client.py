from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_axar_article_id,
    extract_axar_category_slug,
    is_valid_axar_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_axar_datetime,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class AxarClient:
    source_name = "axar.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://axar.az"
        self.default_image_url = "https://axar.az/assets/images/rss_logo.png"
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
            for candidate in self._discover_from_feed():
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:feed: {exc}")

        try:
            for candidate in self._discover_from_homepage_news_line(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:xeber-xetti: {exc}")

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
        if not canonical_url or not is_valid_axar_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_axar_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing Axar article id for {candidate.url}")

        title = self._extract_title(soup) or candidate.title
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_meta_property_raw(soup, "article:section")
            or self._extract_breadcrumb_category(soup)
            or candidate.category
            or extract_axar_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(soup)
        published_at = (
            self._extract_meta_itemprop(soup, "datePublished")
            or self._extract_meta_property_raw(soup, "article:published_time")
            or parse_axar_datetime(published_date_raw)
            or candidate.published_at
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        content_container = soup.select_one("span#font_size.article_body")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        content_text = self._extract_content_text(content_container)
        teaser = self._extract_meta_name(soup, "description") or candidate.teaser
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        hero_image = self._extract_image_url(soup.select_one("img.newsImage"))
        body_images = [
            self._extract_image_url(image_node)
            for image_node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, hero_image, *body_images, candidate.list_image_url]
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
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 60, 120)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue

            article_url = normalize_url(loc_node.text.strip())
            if not is_valid_axar_article_url(article_url):
                continue

            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_axar_article_id(article_url),
                    category=extract_axar_category_slug(article_url),
                    published_at=lastmod_node.text.strip()
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={"latest-sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_feed(self) -> list[ListingCandidate]:
        response = self.session.get(
            f"{self.base_url}/feed.php",
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        channel = root.find("channel")
        if channel is None:
            return candidates

        for item in channel.findall("item"):
            link_node = item.find("link")
            if link_node is None or not link_node.text:
                continue

            article_url = normalize_url(link_node.text.strip())
            if not is_valid_axar_article_url(article_url):
                continue

            title_node = item.find("title")
            description_node = item.find("description")
            category_node = item.find("category")
            pub_date_node = item.find("pubDate")
            enclosure_node = item.find("enclosure")

            list_image_url = ""
            if enclosure_node is not None and enclosure_node.get("url"):
                list_image_url = normalize_url(enclosure_node.get("url", "").strip())

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_axar_article_id(article_url),
                    title=normalize_space(title_node.text) if title_node is not None and title_node.text else "",
                    category=normalize_space(category_node.text)
                    if category_node is not None and category_node.text
                    else extract_axar_category_slug(article_url),
                    published_at=parse_rfc2822_datetime(pub_date_node.text)
                    if pub_date_node is not None and pub_date_node.text
                    else "",
                    teaser=normalize_space(description_node.text)
                    if description_node is not None and description_node.text
                    else "",
                    list_image_url=list_image_url,
                    discovery_sources={"feed"},
                )
            )

        return candidates

    def _discover_from_homepage_news_line(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        slider = self._find_news_line_slider(soup)
        if slider is None:
            return []

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 60, 120)
        for article in slider.select("div.content article.pad10"):
            anchor = article.select_one("a.hover[href]")
            if anchor is None:
                continue

            article_url = normalize_url(anchor.get("href", "").strip())
            if not is_valid_axar_article_url(article_url):
                continue

            title_node = anchor.select_one("div.txt h3")
            time_node = anchor.select_one("div.txt div")
            image_node = anchor.select_one("span.img img")
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_axar_article_id(article_url),
                    title=normalize_space(title_node.get_text(" ", strip=True))
                    if title_node
                    else "",
                    category=extract_axar_category_slug(article_url),
                    list_date_text=normalize_space(time_node.get_text(" ", strip=True))
                    if time_node
                    else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={"xeber-xetti"},
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
        return normalize_url(final_url)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("div.newsPlace > h1")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
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

    def _extract_meta_itemprop(self, soup: BeautifulSoup, itemprop: str) -> str:
        node = soup.select_one(f'meta[itemprop="{itemprop}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_breadcrumb_category(self, soup: BeautifulSoup) -> str:
        for anchor in soup.select("div.newsPlace table a[href]"):
            href = anchor.get("href", "").strip()
            if "/news/" not in href:
                continue
            text = normalize_space(anchor.get_text(" ", strip=True))
            if text:
                return text
        return ""

    def _extract_visible_date(self, soup: BeautifulSoup) -> str:
        for cell in soup.select("div.newsPlace table td"):
            parts = [
                normalize_space(node.get_text(" ", strip=True))
                for node in cell.select("div")
            ]
            if len(parts) < 2:
                continue
            if parts[0].lower() == "tarix":
                return parts[1]
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
        paragraphs = [
            normalize_space(node.get_text(" ", strip=True))
            for node in content_container.find_all("p", recursive=False)
            if normalize_space(node.get_text(" ", strip=True))
        ]
        if not paragraphs:
            paragraphs = [
                normalize_space(node.get_text(" ", strip=True))
                for node in content_container.find_all("p")
                if normalize_space(node.get_text(" ", strip=True))
            ]

        if paragraphs:
            return "\n\n".join(paragraphs).strip()
        return normalize_space(content_container.get_text(" ", strip=True))

    def _find_news_line_slider(self, soup: BeautifulSoup) -> Tag | None:
        for slider in soup.select("div.contentSlider.posV"):
            title_node = slider.select_one("div.title h3")
            if title_node and normalize_space(title_node.get_text(" ", strip=True)) == "Xəbər xətti":
                return slider
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
