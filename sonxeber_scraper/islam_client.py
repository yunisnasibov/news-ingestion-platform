from __future__ import annotations

from datetime import datetime
import json
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    AZERBAIJAN_TZ,
    extract_islam_article_id,
    extract_islam_slug,
    is_valid_islam_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


NAMESPACES = {
    "content": "http://purl.org/rss/1.0/modules/content/",
}

NOISE_CATEGORIES = {
    "xəbərlər",
    "xeberler",
    "ana səhifə",
    "ana sehife",
    "home",
    "главный",
    "esas",
}


class IslamClient:
    source_name = "islam.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://islam.az"
        self.category_url = f"{self.base_url}/cat/xeberler/"
        self.feed_url = f"{self.category_url}feed/"
        self.ajax_url = f"{self.base_url}/wp-admin/admin-ajax.php"
        self.default_image_url = f"{self.base_url}/wp-content/uploads/2021/04/logo_dark.png"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "az,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.category_url,
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

        try:
            for candidate in self._discover_from_category_widget(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:widget: {exc}")

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
        if not canonical_url or not is_valid_islam_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_islam_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing islam article id for {candidate.url}")

        content_container = soup.select_one("div.entry-content.entry.clearfix, div.entry-content")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        title = normalize_space(
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(soup)
            or candidate.title
        )
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        all_categories = self._extract_category_candidates(soup)
        if not self._belongs_to_news_category(all_categories):
            raise ValueError(f"Article is outside xeberler category: {candidate.url}")

        category = self._choose_primary_category(all_categories, candidate.category)
        category = normalize_space(category or "Xəbərlər")

        published_date_raw = (
            self._extract_meta_property_raw(soup, "article:published_time")
            or self._extract_visible_datetime(soup)
            or candidate.list_date_text
        )
        published_at = (
            self._normalize_datetime(published_date_raw)
            or self._normalize_datetime(candidate.published_at)
            or candidate.published_at
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
            or self._extract_excerpt_from_html(str(content_container))
        )
        teaser = normalize_space(teaser)

        content_text = self._extract_content_text(content_container)
        if not content_text:
            raise ValueError(f"Missing content text for {candidate.url}")

        og_image = self._extract_meta_property_raw(soup, "og:image")
        featured_image = self._extract_featured_image(soup)
        body_images = [
            self._extract_image_url(node)
            for node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, featured_image, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(content_container)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_islam_slug(canonical_url),
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
            if not is_valid_islam_article_url(article_url):
                continue

            categories = [
                normalize_space(node.text or "")
                for node in item.findall("category")
                if normalize_space(node.text or "")
            ]
            raw_pub_date = normalize_space(item.findtext("pubDate", default=""))
            feed_html = item.findtext("content:encoded", default="", namespaces=NAMESPACES)
            description_html = item.findtext("description", default="")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islam_slug(article_url),
                    source_article_id=extract_islam_article_id(article_url),
                    title=normalize_space(item.findtext("title", default="")),
                    category=self._choose_primary_category(categories, ""),
                    published_at=self._normalize_datetime(parse_rfc2822_datetime(raw_pub_date)),
                    list_date_text=raw_pub_date,
                    teaser=self._extract_excerpt_from_html(description_html),
                    list_image_url=self._extract_first_image_from_html(feed_html),
                    discovery_sources={"category-feed"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_category_widget(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.category_url)
        widget = self._find_news_widget(soup)
        if widget is None:
            raise ValueError("Missing xeberler widget container")

        query = normalize_space(widget.get("data-query", ""))
        style = normalize_space(widget.get("data-style", ""))
        if not query or not style:
            raise ValueError("Missing widget load-more params")

        candidates = self._parse_widget_candidates(widget, "category-page-1")
        if page_count <= 1:
            return candidates

        for page_number in range(2, page_count + 1):
            payload = self._load_widget_page(query, style, page_number)
            html = payload.get("code", "")
            if html:
                candidates.extend(
                    self._parse_widget_candidates_html(html, f"category-page-{page_number}")
                )
            if payload.get("hide_next"):
                break

        return candidates

    def _load_widget_page(self, query: str, style: str, page_number: int) -> dict[str, object]:
        response = self.session.post(
            self.ajax_url,
            timeout=self.settings.request_timeout_seconds,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": self.base_url,
                "Referer": self.category_url,
            },
            data={
                "action": "tie_widgets_load_more",
                "query": query,
                "style": style,
                "page": str(page_number),
            },
        )
        response.raise_for_status()
        payload = self._decode_ajax_payload(response.text)
        if not isinstance(payload.get("code", ""), str):
            raise ValueError(f"Unexpected widget payload on page {page_number}")
        return payload

    def _decode_ajax_payload(self, raw_value: str) -> dict[str, object]:
        payload: object = raw_value.strip()
        for _ in range(3):
            if isinstance(payload, str):
                payload = json.loads(payload)
                continue
            break
        if not isinstance(payload, dict):
            raise ValueError("Invalid ajax payload")
        return payload

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self.session.get(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        return BeautifulSoup(response.content, "lxml"), normalize_url(response.url)

    def _find_news_widget(self, soup: BeautifulSoup) -> Tag | None:
        for container in soup.select("div.widget-posts-list-container[data-query][data-style]"):
            query = normalize_space(container.get("data-query", ""))
            if "'32'" in query or '"32"' in query:
                return container
        return None

    def _parse_widget_candidates(self, widget: Tag, label: str) -> list[ListingCandidate]:
        items = widget.select("li.widget-single-post-item.widget-post-list")
        return self._build_widget_candidates(items, label)

    def _parse_widget_candidates_html(self, html: str, label: str) -> list[ListingCandidate]:
        soup = BeautifulSoup(f"<ul>{html}</ul>", "lxml")
        items = soup.select("li.widget-single-post-item.widget-post-list")
        return self._build_widget_candidates(items, label)

    def _build_widget_candidates(self, items: list[Tag], label: str) -> list[ListingCandidate]:
        candidates: list[ListingCandidate] = []
        for item in items:
            title_link = item.select_one("a.post-title.the-subtitle[href]")
            thumb_link = item.select_one("a.post-thumb[href]")
            anchor = title_link or thumb_link
            if anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, anchor.get("href", "").strip())
            )
            if not is_valid_islam_article_url(article_url):
                continue

            category_node = item.select_one("span.post-cat")
            date_node = item.select_one("span.date.meta-item")
            image_node = item.select_one("img")
            title = normalize_space(
                (title_link.get_text(" ", strip=True) if title_link else "")
                or thumb_link.get("aria-label", "")
            )
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islam_slug(article_url),
                    source_article_id=extract_islam_article_id(article_url),
                    title=title,
                    category=self._choose_primary_category(
                        [normalize_space(category_node.get_text(" ", strip=True))]
                        if category_node
                        else [],
                        "",
                    ),
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True))
                    if date_node
                    else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )
        return candidates

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

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("h1.entry-title")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_datetime(self, soup: BeautifulSoup) -> str:
        node = soup.select_one("time.entry-date[datetime], span.date.meta-item[datetime]")
        if node and node.get("datetime"):
            return normalize_space(node["datetime"].strip())
        time_node = soup.select_one("time.entry-date, span.date.meta-item.tie-icon")
        if time_node:
            return normalize_space(time_node.get_text(" ", strip=True))
        return ""

    def _extract_category_candidates(self, soup: BeautifulSoup) -> list[str]:
        values = [
            normalize_space(node.get_text(" ", strip=True))
            for node in soup.select("span.post-cat, ul.cat-links a, #breadcrumb a, .breadcrumbs a")
        ]
        return unique_preserving_order([value for value in values if value])

    def _belongs_to_news_category(self, categories: list[str]) -> bool:
        lowered = {normalize_space(value).casefold() for value in categories}
        return "xəbərlər" in lowered or "xeberler" in lowered

    def _choose_primary_category(self, categories: list[str], fallback: str) -> str:
        ordered = unique_preserving_order([*categories, normalize_space(fallback)])
        for value in ordered:
            lowered = value.casefold()
            if lowered in NOISE_CATEGORIES:
                continue
            return value
        for value in ordered:
            lowered = value.casefold()
            if lowered in {"xəbərlər", "xeberler"}:
                return "Xəbərlər"
        for value in ordered:
            if value and value.casefold() not in {"ana səhifə", "ana sehife", "home"}:
                return value
        return ""

    def _extract_featured_image(self, soup: BeautifulSoup) -> str:
        for selector in (
            "figure.post-thumbnail img",
            "div.single-featured-image img",
            "img.wp-post-image",
        ):
            url = self._extract_image_url(soup.select_one(selector))
            if url:
                return url
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "data-lazy-src", "src"):
            value = normalize_space(image_node.get(attribute, ""))
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        srcset = normalize_space(image_node.get("srcset", ""))
        if srcset:
            first_candidate = normalize_space(srcset.split(",", 1)[0].split(" ", 1)[0])
            if first_candidate:
                return make_absolute_url(self.base_url, first_candidate)
        return ""

    def _extract_video_embed_url(self, content_container: Tag) -> str:
        node = content_container.select_one("iframe[src], video[src], video source[src]")
        if node and node.get("src"):
            return make_absolute_url(self.base_url, node["src"].strip())
        return ""

    def _extract_content_text(self, content_container: Tag) -> str:
        paragraphs: list[str] = []
        for node in content_container.select("p, li, h2, h3, h4, blockquote"):
            if node.find_parent(["aside", "nav"]) is not None:
                continue
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            if text.startswith("The post "):
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
        fallback = normalize_space(content_container.get_text(" ", strip=True))
        if fallback.startswith("The post "):
            return ""
        return fallback

    def _extract_first_image_from_html(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        return self._extract_image_url(soup.select_one("img"))

    def _extract_excerpt_from_html(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        for node in soup.select("p, li"):
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            if text.startswith("The post "):
                continue
            return text
        return ""

    def _normalize_datetime(self, value: str) -> str:
        cleaned = normalize_space(value.replace("Z", "+00:00"))
        if not cleaned:
            return ""
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError:
            return ""
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=AZERBAIJAN_TZ)
        return parsed.astimezone(AZERBAIJAN_TZ).isoformat()

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
