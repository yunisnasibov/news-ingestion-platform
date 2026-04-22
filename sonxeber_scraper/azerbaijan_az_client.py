from __future__ import annotations

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_azerbaijan_az_article_id,
    is_valid_azerbaijan_az_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_iso_or_dotted_date,
    sha256_text,
    unique_preserving_order,
)


class AzerbaijanAzClient:
    source_name = "azerbaijan.az"
    supports_forward_probe = True

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://azerbaijan.az"
        self.default_category = "Xəbərlər"
        self.default_image_url = "https://azerbaijan.az/media/images/favicon.ico"
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

        for label, url in self._listing_urls(page_count):
            try:
                page_candidates = self._fetch_listing_candidates(label, url)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")
                continue
            for candidate in page_candidates:
                key = normalize_url(candidate.url)
                if key in candidates:
                    candidates[key].merge(candidate)
                else:
                    candidates[key] = candidate

        return candidates, errors

    def discover_probe_candidates(
        self,
        max_article_id: int,
        window: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        candidates: dict[str, ListingCandidate] = {}
        errors: list[str] = []

        for source_article_id in range(max_article_id + 1, max_article_id + window + 1):
            try:
                candidate = self._probe_article_id(source_article_id)
            except Exception as exc:
                errors.append(f"probe:{source_article_id}: {exc}")
                candidate = None
            if candidate is None:
                continue
            candidates[normalize_url(candidate.url)] = candidate

        return candidates, errors

    def fetch_article(self, candidate: ListingCandidate) -> ArticleRecord:
        soup, final_url = self._get_soup(candidate.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_azerbaijan_az_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_azerbaijan_az_article_id(canonical_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing article id for {candidate.url}")

        title_node = soup.select_one("div.news-view-title p")
        body_node = soup.select_one("div.news-view-body")
        image_node = soup.select_one("div.news-view-image img")
        date_text = self._extract_detail_date_text(soup)

        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        content_text = self._extract_content_text(body_node)
        if not title or not content_text or not date_text:
            raise ValueError(f"Empty Azerbaijan.az detail payload for {candidate.url}")

        published_at = parse_iso_or_dotted_date(date_text) or candidate.published_at or date_text
        hero_image_url = self._extract_image_url(image_node) or candidate.list_image_url or self.default_image_url
        gallery_image_urls = unique_preserving_order([hero_image_url, candidate.list_image_url])
        if not gallery_image_urls:
            gallery_image_urls = [self.default_image_url]

        teaser = title
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug="",
            url=canonical_url,
            canonical_url=canonical_url,
            title=title,
            category=candidate.category or self.default_category,
            published_date_raw=date_text,
            published_at=published_at,
            list_date_text=candidate.list_date_text,
            teaser=teaser,
            content_text=content_text,
            hero_image_url=hero_image_url,
            gallery_image_urls=gallery_image_urls,
            video_embed_url="",
            list_image_url=candidate.list_image_url or hero_image_url,
            discovery_sources=sorted(candidate.discovery_sources),
            content_hash=content_hash,
        )

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        return [
            (f"news-page-{page_number}", f"{self.base_url}/news?page={page_number}")
            for page_number in range(1, page_count + 1)
        ]

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, final_url = self._get_soup(url)
        if normalize_url(final_url) != normalize_url(url):
            return []

        candidates: list[ListingCandidate] = []
        for card in soup.select("div.all-news-container div.other-news-container"):
            anchor = card.select_one("a[href]")
            if anchor is None:
                continue

            article_url = normalize_url(make_absolute_url(self.base_url, anchor.get("href", "").strip()))
            source_article_id = extract_azerbaijan_az_article_id(article_url)
            if source_article_id is None:
                continue

            title_node = card.select_one("div.other-news-title p")
            date_node = card.select_one("div.news-date-index")
            image_node = card.select_one("div.other-news-image img")
            date_text = normalize_space(date_node.get_text(" ", strip=True)) if date_node else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=source_article_id,
                    title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                    category=self.default_category,
                    published_at=parse_iso_or_dotted_date(date_text) if date_text else "",
                    list_date_text=date_text,
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _probe_article_id(self, source_article_id: int) -> ListingCandidate | None:
        probe_url = f"{self.base_url}/news/{source_article_id}"
        soup, final_url = self._get_soup(probe_url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_azerbaijan_az_article_url(canonical_url):
            return None
        if extract_azerbaijan_az_article_id(canonical_url) != source_article_id:
            return None

        title_node = soup.select_one("div.news-view-title p")
        body_node = soup.select_one("div.news-view-body")
        image_node = soup.select_one("div.news-view-image img")
        date_text = self._extract_detail_date_text(soup)
        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        content_text = self._extract_content_text(body_node)
        if not title or not content_text or not date_text:
            return None

        return ListingCandidate(
            url=canonical_url,
            slug="",
            source_article_id=source_article_id,
            title=title,
            category=self.default_category,
            published_at=parse_iso_or_dotted_date(date_text),
            list_date_text=date_text,
            list_image_url=self._extract_image_url(image_node),
            discovery_sources={"id-probe"},
        )

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

    def _extract_detail_date_text(self, soup: BeautifulSoup) -> str:
        for node in soup.select("div.news-view-container-left > div"):
            if node.get("class"):
                continue
            text = normalize_space(node.get_text(" ", strip=True))
            if text and text[0].isdigit():
                return text
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            value = image_node.get(attribute, "").strip()
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_content_text(self, body_node: Tag | None) -> str:
        if body_node is None:
            return ""
        paragraphs: list[str] = []
        for paragraph in body_node.select("p"):
            text = normalize_space(paragraph.get_text(" ", strip=True))
            if text:
                paragraphs.append(text)
        content_text = "\n\n".join(paragraphs).strip()
        if content_text:
            return content_text
        return normalize_space(body_node.get_text(" ", strip=True))
