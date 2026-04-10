from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_sia_article_id,
    extract_sia_category_slug,
    is_valid_sia_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class SiaClient:
    source_name = "sia.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://sia.az"
        self.latest_url = f"{self.base_url}/az/latest/"
        self.sitemap_latest_url = f"{self.base_url}/sitemap_latest.php"
        self.feed_url = f"{self.base_url}/feed.php"
        self.default_image_url = f"{self.base_url}/assets/favicon/favicon.ico"
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
            for candidate in self._discover_from_feed(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:feed: {exc}")

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

        soup = BeautifulSoup(response.text, "lxml")
        final_url = normalize_url(response.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_sia_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_sia_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing SIA article id for {candidate.url}")

        article = soup.select_one("div.single-post")
        if article is None:
            raise ValueError(f"Missing single-post container for {candidate.url}")

        title = (
            self._extract_meta_itemprop(soup, "headline")
            or self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(article)
            or candidate.title
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_meta_property_raw(soup, "article:section")
            or self._extract_visible_category(article)
            or candidate.category
            or extract_sia_category_slug(canonical_url)
            or "uncategorized"
        )
        category = normalize_space(category)

        published_at = (
            self._extract_meta_itemprop(soup, "datePublished")
            or self._extract_meta_property_raw(soup, "article:published_time")
            or candidate.published_at
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        published_date_raw = published_at

        content_container = self._extract_primary_content(article)
        if content_container is None:
            raise ValueError(f"Missing article body for {candidate.url}")

        teaser = (
            self._extract_meta_name(soup, "description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )
        content_text = self._extract_content_text(content_container)
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property_url(soup, "og:image")
        main_image = self._extract_image_url(article.select_one("div.post-media img"))
        body_images = [
            self._extract_image_url(node) for node in content_container.select("img")
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
            self.sitemap_latest_url,
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
            if not is_valid_sia_article_url(article_url):
                continue

            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_sia_article_id(article_url),
                    category=extract_sia_category_slug(article_url),
                    published_at=normalize_space(lastmod_node.text)
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={"latest-sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_feed(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            self.feed_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 40, 80)
        for item in root.findall("./channel/item"):
            link = normalize_space(item.findtext("link", default=""))
            article_url = normalize_url(link)
            if not is_valid_sia_article_url(article_url):
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
                    slug="",
                    source_article_id=extract_sia_article_id(article_url),
                    title=title,
                    category=category,
                    published_at=parse_rfc2822_datetime(pub_date) or pub_date,
                    list_date_text=pub_date,
                    teaser=teaser,
                    list_image_url=image_url,
                    discovery_sources={"feed"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("latest-page-1", self.latest_url)]
        for page_number in range(2, page_count + 1):
            urls.append((f"latest-page-{page_number}", f"{self.latest_url}page{page_number}/"))
        return urls

    def _discover_from_latest_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for block in soup.select("div.col-md-9.col-sm-12.col-xs-12.m22 div.large-widget.m30"):
            title_anchor = block.select_one("div.title-area h3 a[href]")
            image_node = block.select_one("div.post-media img")
            category_anchor = block.select_one("div.colorfulcats a[href]")
            time_anchor = block.select_one("div.large-post-meta span a")
            if title_anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, title_anchor.get("href", "").strip())
            )
            if not is_valid_sia_article_url(article_url):
                continue

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_sia_article_id(article_url),
                    title=normalize_space(title_anchor.get_text(" ", strip=True)),
                    category=normalize_space(category_anchor.get_text(" ", strip=True))
                    if category_anchor
                    else extract_sia_category_slug(article_url),
                    list_date_text=normalize_space(time_anchor.get_text(" ", strip=True))
                    if time_anchor
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
        return BeautifulSoup(response.text, "lxml"), normalize_url(response.url)

    def _extract_primary_content(self, article: Tag) -> Tag | None:
        best_container: Tag | None = None
        best_length = -1

        for container in article.select("div.post-desc"):
            text_length = len(normalize_space(container.get_text(" ", strip=True)))
            if text_length > best_length:
                best_length = text_length
                best_container = container

        return best_container

    def _extract_content_text(self, content_container: Tag) -> str:
        container = BeautifulSoup(str(content_container), "lxml")
        root = container.select_one("div.post-desc") or container

        for node in root.select("script, style, iframe, noscript, .post-sharing, .gallery, .news_list"):
            node.decompose()
        for node in root.select('div[id^="adfox_"]'):
            node.decompose()

        paragraphs = [
            normalize_space(node.get_text(" ", strip=True))
            for node in root.select("p, blockquote")
            if normalize_space(node.get_text(" ", strip=True))
        ]
        if paragraphs:
            return "\n".join(paragraphs)
        return normalize_space(root.get_text(" ", strip=True))

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(node["href"].strip())
        return normalize_url(final_url)

    def _extract_meta_itemprop(self, soup: BeautifulSoup, itemprop_name: str) -> str:
        node = soup.select_one(f'meta[itemprop="{itemprop_name}"]')
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

    def _extract_meta_property_url(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return make_absolute_url(self.base_url, node["content"].strip())
        return ""

    def _extract_title(self, article: Tag) -> str:
        node = article.select_one("h1")
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

    def _extract_visible_category(self, article: Tag) -> str:
        node = article.select_one("div.colorfulcats span.label")
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

    def _extract_image_url(self, node: Tag | None) -> str:
        if node is None:
            return ""
        for attribute in ("src", "data-src", "data-lazy-src"):
            value = node.get(attribute, "").strip()
            if value:
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_video_embed_url(self, root: Tag) -> str:
        iframe = root.select_one("iframe[src]")
        if iframe is None:
            return ""
        return make_absolute_url(self.base_url, iframe.get("src", "").strip())

    def _merge_candidate(
        self,
        candidates: dict[str, ListingCandidate],
        candidate: ListingCandidate,
    ) -> None:
        existing = candidates.get(candidate.url)
        if existing is None:
            candidates[candidate.url] = candidate
            return
        existing.merge(candidate)
