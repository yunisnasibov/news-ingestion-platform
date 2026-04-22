from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_metbuat_article_id,
    extract_metbuat_slug,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_datetime,
    parse_rfc2822_datetime,
    sha256_text,
    unique_preserving_order,
)


MEDIA_NS = {"media": "http://search.yahoo.com/mrss/"}


class MetbuatClient:
    source_name = "metbuat.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://metbuat.az"
        self.default_image_url = "https://metbuat.az/images/icons/logo.png"
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
            for candidate in self._discover_from_rss():
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:rss: {exc}")

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
        if self._is_not_found_page(soup):
            raise ValueError(f"Not found page returned for {candidate.url}")

        canonical_url = self._extract_canonical_url(soup, final_url)
        source_article_id = (
            extract_metbuat_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing Metbuat article id for {candidate.url}")

        title_node = soup.select_one("h1.news_in_ttl") or soup.select_one(".news_in_ttl")
        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        title = title or candidate.title
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category_node = soup.select_one(".news_in_catg a[href*='/category/']")
        category = (
            normalize_space(category_node.get_text(" ", strip=True))
            if category_node
            else candidate.category
        )
        category = category or "uncategorized"

        date_node = soup.select_one(".news_in_date")
        time_node = soup.select_one(".news_in_time")
        published_date_text = self._clean_detail_text(date_node)
        published_time_text = self._clean_detail_text(time_node)
        published_date_raw = normalize_space(
            f"{published_date_text} {published_time_text}".strip()
        )
        published_at = (
            parse_azerbaijani_datetime(published_date_text, published_time_text)
            or candidate.published_at
            or published_date_raw
        )

        article_body = (
            soup.select_one("article#maincontent[itemprop='articleBody']")
            or soup.select_one("article#maincontent")
        )
        if article_body is None:
            raise ValueError(f"Missing article body for {candidate.url}")

        content_text = self._extract_content_text(article_body)
        teaser = self._extract_meta_content(soup, "description") or candidate.teaser
        if not content_text:
            content_text = teaser or title

        og_image = self._extract_meta_property(soup, "og:image")
        main_image = self._extract_image_url(soup.select_one(".news_in_img img"))
        gallery_images = [
            self._extract_anchor_href(anchor)
            for anchor in soup.select(".news_in_other_images a.fancybox[href]")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *gallery_images, candidate.list_image_url]
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
            slug=extract_metbuat_slug(canonical_url) or candidate.slug,
            url=normalize_url(candidate.url),
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

    def _discover_from_rss(self) -> list[ListingCandidate]:
        response = self.session.get(
            f"{self.base_url}/rss.xml",
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)

        candidates: list[ListingCandidate] = []
        for item in root.findall("./channel/item"):
            link_node = item.find("link")
            if link_node is None or link_node.text is None:
                continue

            article_url = normalize_url(link_node.text.strip())
            source_article_id = extract_metbuat_article_id(article_url)
            if source_article_id is None:
                continue

            title_node = item.find("title")
            description_node = item.find("description")
            pub_date_node = item.find("pubDate")
            media_node = item.find("media:content", MEDIA_NS)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_metbuat_slug(article_url),
                    source_article_id=source_article_id,
                    title=normalize_space(title_node.text) if title_node is not None else "",
                    published_at=parse_rfc2822_datetime(pub_date_node.text or "")
                    if pub_date_node is not None
                    else "",
                    teaser=normalize_space(description_node.text)
                    if description_node is not None and description_node.text
                    else "",
                    list_image_url=media_node.get("url", "").strip()
                    if media_node is not None
                    else "",
                    discovery_sources={"rss"},
                )
            )
        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("latest-page-1", f"{self.base_url}/olke-metbuati.html")]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"latest-page-{page_number}",
                    f"{self.base_url}/olke-metbuati.html?page={page_number}&per-page=30",
                )
            )
        return urls

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        if self._is_not_found_page(soup):
            return []

        container = soup.select_one(".col-sm-8.col-md-9.col-lg-9") or soup
        candidates: list[ListingCandidate] = []
        for anchor in container.select("a.news_box[href]"):
            href = anchor.get("href", "").strip()
            article_url = normalize_url(make_absolute_url(self.base_url, href))
            source_article_id = extract_metbuat_article_id(article_url)
            if source_article_id is None:
                continue

            title = normalize_space(anchor.get("title", ""))
            if not title:
                title_node = anchor.select_one("h3, h4, h5, h6")
                title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_metbuat_slug(article_url),
                    source_article_id=source_article_id,
                    title=title,
                    list_image_url=self._extract_image_url(anchor.select_one("img")),
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

    def _is_not_found_page(self, soup: BeautifulSoup) -> bool:
        title_node = soup.select_one("title")
        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        if "Not Found (#404)" in title:
            return True
        error_node = soup.select_one(".site-error h1")
        if error_node and "Not Found" in error_node.get_text(" ", strip=True):
            return True
        return False

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return make_absolute_url(self.base_url, node["href"].strip())
        return final_url

    def _extract_meta_property(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return make_absolute_url(self.base_url, node["content"].strip())
        return ""

    def _extract_meta_content(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"])
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        src = image_node.get("src", "").strip()
        if not src or src.startswith("data:image"):
            return ""
        return make_absolute_url(self.base_url, src)

    def _extract_anchor_href(self, anchor_node: Tag | None) -> str:
        if anchor_node is None:
            return ""
        href = anchor_node.get("href", "").strip()
        if not href:
            return ""
        return make_absolute_url(self.base_url, href)

    def _extract_video_embed_url(self, soup: BeautifulSoup) -> str:
        iframe = soup.select_one("article#maincontent iframe[src], .news_in_content iframe[src]")
        if iframe and iframe.get("src"):
            return make_absolute_url(self.base_url, iframe["src"].strip())
        return ""

    def _extract_content_text(self, article_body: Tag) -> str:
        article_soup = BeautifulSoup(str(article_body), "lxml")
        article = article_soup.select_one("article") or article_soup

        for selector in (
            "script",
            "style",
            "ins",
            "noscript",
            "iframe",
            "#fb-root",
            ".fb-like",
            ".ainsyndication",
        ):
            for node in article.select(selector):
                node.decompose()

        paragraphs: list[str] = []
        for paragraph in article.find_all("p"):
            text = normalize_space(paragraph.get_text(" ", strip=True))
            if not text:
                continue
            paragraphs.append(text)

        content_text = "\n\n".join(unique_preserving_order(paragraphs)).strip()
        if content_text:
            return content_text

        return normalize_space(article.get_text(" ", strip=True))

    def _clean_detail_text(self, node: Tag | None) -> str:
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

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
