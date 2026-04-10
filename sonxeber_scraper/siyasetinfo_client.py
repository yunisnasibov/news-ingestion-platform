from __future__ import annotations

from datetime import datetime
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_siyasetinfo_article_id,
    is_valid_siyasetinfo_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    AZERBAIJAN_TZ,
    parse_azerbaijani_date,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


NAMESPACES = {
    "content": "http://purl.org/rss/1.0/modules/content/",
}


class SiyasetinfoClient:
    source_name = "siyasetinfo.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://siyasetinfo.az"
        self.feed_url = f"{self.base_url}/feed"
        self.default_image_url = (
            f"{self.base_url}/wp-content/uploads/2022/11/s-150x150.png"
        )
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
            for candidate in self._discover_from_feed(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:feed: {exc}")

        for label, url in self._listing_urls(page_count):
            try:
                page_candidates = self._discover_from_listing_page(label, url)
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
        if "error404" in self._body_classes(soup):
            raise ValueError(f"404 page for {candidate.url}")

        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_siyasetinfo_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            extract_siyasetinfo_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing siyasetinfo article id for {candidate.url}")

        article = soup.select_one("main.site-main article.af-single-article")
        if article is None:
            raise ValueError(f"Missing article container for {candidate.url}")

        title = (
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(article)
            or candidate.title
        )
        title = normalize_space(title)
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = (
            self._extract_visible_category(article)
            or candidate.category
            or "uncategorized"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(article)
        published_at = (
            candidate.published_at
            or parse_azerbaijani_date(published_date_raw)
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )

        content_container = article.select_one("div.entry-content.read-details")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        content_text = self._extract_content_text(content_container)
        if not content_text:
            raise ValueError(f"Missing content text for {candidate.url}")

        og_image = self._extract_meta_property_raw(soup, "og:image")
        featured_image = self._extract_image_url(article.select_one("div.post-thumbnail img"))
        body_images = [
            self._extract_image_url(node) for node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, featured_image, *body_images, candidate.list_image_url]
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

    def _discover_from_feed(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            self.feed_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 20, 40)
        for item in root.findall("./channel/item"):
            link = normalize_space(item.findtext("link", default=""))
            article_url = normalize_url(link)
            if not is_valid_siyasetinfo_article_url(article_url):
                continue

            content_encoded = item.findtext("content:encoded", default="", namespaces=NAMESPACES)
            image_url = self._extract_first_image_from_html(content_encoded)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_siyasetinfo_article_id(article_url),
                    title=normalize_space(item.findtext("title", default="")),
                    category=normalize_space(item.findtext("category", default="")),
                    published_at=self._normalize_feed_datetime(
                        parse_rfc2822_datetime(
                            normalize_space(item.findtext("pubDate", default=""))
                        )
                    ),
                    list_date_text=normalize_space(item.findtext("pubDate", default="")),
                    teaser=normalize_space(item.findtext("description", default="")),
                    list_image_url=image_url,
                    discovery_sources={"feed"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("homepage", self.base_url)]
        for page_number in range(2, page_count + 1):
            urls.append((f"page-{page_number}", f"{self.base_url}/page/{page_number}/"))
        return urls

    def _discover_from_listing_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for article in soup.select("main.site-main article.latest-posts-list"):
            if "type-post" not in article.get("class", []):
                continue

            title_link = article.select_one("div.read-title h4 a[href]")
            image_link = article.select_one("a.aft-post-image-link[href]")
            anchor = title_link or image_link
            if anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, anchor.get("href", "").strip())
            )
            if not is_valid_siyasetinfo_article_url(article_url):
                continue

            date_node = article.select_one("span.item-metadata.posts-date")
            category_node = article.select_one("ul.cat-links li.meta-category a")
            teaser_node = article.select_one("div.post-description")
            image_node = article.select_one("img.wp-post-image")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_siyasetinfo_article_id(article_url),
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    category=normalize_space(category_node.get_text(" ", strip=True))
                    if category_node
                    else "",
                    published_at=parse_azerbaijani_date(
                        normalize_space(date_node.get_text(" ", strip=True))
                    ),
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True))
                    if date_node
                    else "",
                    teaser=self._clean_teaser(
                        teaser_node.get_text(" ", strip=True) if teaser_node else ""
                    ),
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

    def _extract_meta_property_raw(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_title(self, article: Tag) -> str:
        node = article.select_one("header.entry-header h1.entry-title")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_category(self, article: Tag) -> str:
        node = article.select_one("ul.cat-links li.meta-category a")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, article: Tag) -> str:
        node = article.select_one("span.item-metadata.posts-date")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "data-lazy-src", "src"):
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
        for node in content_container.select("p, li, h3, h4, blockquote"):
            classes = set(node.get("class", []))
            if classes & {"addtoany_share_save_container", "post-views", "post-item-metadata"}:
                continue
            if node.find_parent("nav", class_="post-navigation") is not None:
                continue
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            if text.startswith("Post Views:"):
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
        return normalize_space(content_container.get_text(" ", strip=True))

    def _extract_first_image_from_html(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        return self._extract_image_url(soup.select_one("img"))

    def _body_classes(self, soup: BeautifulSoup) -> set[str]:
        body = soup.body
        if body is None:
            return set()
        return set(body.get("class", []))

    def _clean_teaser(self, value: str) -> str:
        cleaned = normalize_space(value)
        return cleaned.replace("davamı", "").strip(" .")

    def _normalize_feed_datetime(self, value: str) -> str:
        if not value:
            return ""
        try:
            return datetime.fromisoformat(value).astimezone(AZERBAIJAN_TZ).isoformat()
        except ValueError:
            return value

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
