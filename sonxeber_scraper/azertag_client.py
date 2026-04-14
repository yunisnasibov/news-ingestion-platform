from __future__ import annotations

import concurrent.futures
import time

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_azertag_article_id,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azertag_datetime,
    sha256_text,
    unique_preserving_order,
)


class AzertagClient:
    source_name = "azertag.az"
    supports_forward_probe = True

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://special.azertag.az"
        self.default_image_url = "https://azertag.az/resources/images/logo.svg"
        self.min_request_interval_seconds = 0.15
        self.retry_sleep_seconds = 1.0
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
                for candidate in self._fetch_listing_candidates(label, url):
                    self._merge_candidate(candidates, candidate)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")

        return candidates, errors

    def discover_probe_candidates(
        self,
        max_article_id: int,
        window: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        candidates: dict[str, ListingCandidate] = {}
        errors: list[str] = []


        def _probe(article_id: int) -> ListingCandidate | Exception | None:
            article_url = f"{self.base_url}/az/xeber/{article_id}"
            try:
                response = self._request(
                    article_url,
                    timeout=self.settings.request_timeout_seconds,
                    allow_redirects=True,
                    allow_server_errors=True,
                )
                if response.status_code >= 500:
                    return None
                soup = BeautifulSoup(response.content, "lxml", from_encoding="utf-8")
                final_url = normalize_url(str(response.url))
                if self._is_error_page(soup):
                    return None
                return self._build_candidate_from_detail(
                    soup,
                    final_url,
                    f"id-probe-{article_id}",
                )
            except Exception as exc:
                return exc

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(window, 20)) as executor:
            futures = {executor.submit(_probe, i): i for i in range(max_article_id + 1, max_article_id + window + 1)}
            for future in concurrent.futures.as_completed(futures):
                article_id = futures[future]
                res = future.result()
                if isinstance(res, Exception):
                    errors.append(f"probe:{article_id}: {res}")
                elif res is not None:
                    self._merge_candidate(candidates, res)

        return candidates, errors

    def discover_backward_probe_articles(
        self,
        min_article_id: int,
        window: int,
    ) -> tuple[dict[str, "ArticleRecord"], list[str]]:
        """Fetch articles by decrementing IDs backward. Returns full ArticleRecord
        objects in a single HTTP pass — no second round-trip needed in backfill."""
        articles: dict[str, ArticleRecord] = {}
        errors: list[str] = []

        def _fetch(article_id: int) -> ArticleRecord | Exception | None:
            article_url = f"{self.base_url}/az/xeber/{article_id}"
            try:
                response = self._request(
                    article_url,
                    timeout=self.settings.request_timeout_seconds,
                    allow_redirects=True,
                    allow_server_errors=True,
                )
                if response.status_code >= 500:
                    return None
                soup = BeautifulSoup(response.content, "lxml", from_encoding="utf-8")
                final_url = normalize_url(str(response.url))
                if self._is_error_page(soup):
                    return None

                source_article_id = extract_azertag_article_id(final_url)
                if source_article_id is None:
                    return None

                title = self._extract_title(soup)
                if not title:
                    return None

                category = self._extract_category(soup) or "uncategorized"
                published_date_raw = self._extract_date_text(soup)
                published_at = parse_azertag_datetime(published_date_raw) or published_date_raw

                content_container = soup.select_one(".news-view-body")
                content_text = self._extract_content_text(content_container)
                teaser = self._extract_meta_property(soup, "og:description") or title
                if not content_text:
                    content_text = teaser

                main_image = self._extract_image_url(soup.select_one(".preview-news-view img"))
                gallery_image_urls = unique_preserving_order(
                    [main_image, self.default_image_url]
                )
                hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
                video_embed_url = self._extract_video_url(soup)
                content_hash = sha256_text(content_text)

                return ArticleRecord(
                    source_name=self.source_name,
                    source_article_id=source_article_id,
                    slug=str(source_article_id),
                    url=normalize_url(final_url),
                    canonical_url=normalize_url(final_url),
                    title=title,
                    category=category,
                    published_date_raw=published_date_raw,
                    published_at=published_at,
                    list_date_text=published_date_raw,
                    teaser=teaser,
                    content_text=content_text,
                    hero_image_url=hero_image_url,
                    gallery_image_urls=gallery_image_urls,
                    video_embed_url=video_embed_url,
                    list_image_url=hero_image_url,
                    discovery_sources=[f"id-backward-probe-{article_id}"],
                    content_hash=content_hash,
                )
            except Exception as exc:
                return exc

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(window, 20)) as executor:
            futures = {executor.submit(_fetch, i): i for i in range(min_article_id - 1, min_article_id - window - 1, -1)}
            for future in concurrent.futures.as_completed(futures):
                article_id = futures[future]
                res = future.result()
                if isinstance(res, Exception):
                    errors.append(f"backward-probe:{article_id}: {res}")
                elif res is not None:
                    articles[res.url] = res

        return articles, errors

    def fetch_article(self, candidate: ListingCandidate) -> ArticleRecord:
        soup, final_url = self._get_soup(candidate.url)
        if self._is_error_page(soup):
            raise ValueError(f"Error page returned for {candidate.url}")

        source_article_id = extract_azertag_article_id(final_url) or candidate.source_article_id
        if source_article_id is None:
            raise ValueError(f"Missing AZERTAG article id for {candidate.url}")

        title = self._extract_title(soup) or candidate.title
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        category = self._extract_category(soup) or candidate.category or "uncategorized"
        published_date_raw = self._extract_date_text(soup) or candidate.list_date_text
        published_at = parse_azertag_datetime(published_date_raw) or candidate.published_at or published_date_raw

        content_container = soup.select_one(".news-view-body")
        content_text = self._extract_content_text(content_container)
        teaser = self._extract_meta_property(soup, "og:description") or candidate.teaser or title
        if not content_text:
            content_text = teaser

        main_image = self._extract_image_url(soup.select_one(".preview-news-view img"))
        gallery_image_urls = unique_preserving_order(
            [main_image, candidate.list_image_url, self.default_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        video_embed_url = self._extract_video_url(soup)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=str(source_article_id),
            url=normalize_url(final_url),
            canonical_url=normalize_url(final_url),
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

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("archive-page-1", f"{self.base_url}/az")]
        for page_number in range(2, page_count + 1):
            urls.append((f"archive-page-{page_number}", f"{self.base_url}/az/arxiv/{page_number}"))
        return urls

    def archive_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return f"{self.base_url}/az"
        return f"{self.base_url}/az/arxiv/{page_number}"

    def discover_archive_page(self, page_number: int) -> list[ListingCandidate]:
        label = "archive-page-1" if page_number <= 1 else f"archive-page-{page_number}"
        return self._fetch_listing_candidates(label, self.archive_page_url(page_number))

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        if self._is_error_page(soup):
            return []

        candidates: list[ListingCandidate] = []
        for item in soup.select(".news-item"):
            anchor = item.select_one(".news-title a[href]")
            if anchor is None or not anchor.get("href"):
                continue
            article_url = normalize_url(make_absolute_url(self.base_url, anchor["href"].strip()))
            source_article_id = extract_azertag_article_id(article_url)
            if source_article_id is None:
                continue

            category_node = item.select_one(".news-category a")
            date_node = item.select_one(".news-date")
            image_node = item.select_one(".news-img img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=str(source_article_id),
                    source_article_id=source_article_id,
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    category=normalize_space(category_node.get_text(" ", strip=True))
                    if category_node
                    else "",
                    published_at=parse_azertag_datetime(date_node.get_text(" ", strip=True))
                    if date_node
                    else "",
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True))
                    if date_node
                    else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _build_candidate_from_detail(
        self,
        soup: BeautifulSoup,
        final_url: str,
        label: str,
    ) -> ListingCandidate:
        source_article_id = extract_azertag_article_id(final_url)
        if source_article_id is None:
            raise ValueError(f"Missing AZERTAG article id for {final_url}")

        date_text = self._extract_date_text(soup)
        return ListingCandidate(
            url=normalize_url(final_url),
            slug=str(source_article_id),
            source_article_id=source_article_id,
            title=self._extract_title(soup),
            category=self._extract_category(soup),
            published_at=parse_azertag_datetime(date_text) if date_text else "",
            list_date_text=date_text,
            list_image_url=self._extract_image_url(soup.select_one(".preview-news-view img")),
            discovery_sources={label},
        )

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self._request(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        return (
            BeautifulSoup(response.content, "lxml", from_encoding="utf-8"),
            normalize_url(response.url),
        )

    def _request(self, url: str, *, allow_server_errors: bool = False, **kwargs) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = self.session.get(url, **kwargs)
                response.encoding = "utf-8"
                if response.status_code >= 500 and not allow_server_errors:
                    raise requests.HTTPError(
                        f"{response.status_code} Server Error: {response.reason} for url: {response.url}",
                        response=response,
                    )
                if response.status_code >= 500 and allow_server_errors:
                    return response
                response.raise_for_status()
                time.sleep(self.min_request_interval_seconds)
                return response
            except Exception as exc:
                last_error = exc
                if attempt == 3:
                    break
                time.sleep(self.retry_sleep_seconds * (attempt + 1))
        if last_error is None:
            raise RuntimeError(f"AZERTAG request failed for {url}")
        raise last_error

    def _is_error_page(self, soup: BeautifulSoup) -> bool:
        title_node = soup.select_one("title")
        title = normalize_space(title_node.get_text(" ", strip=True)) if title_node else ""
        if "Xəta" in title or "Not Found" in title:
            return True
        error_node = soup.select_one(".site-error h1")
        if error_node:
            return True
        return False

    def _extract_title(self, soup: BeautifulSoup) -> str:
        node = soup.select_one(".news-view-title p")
        if node is not None:
            return normalize_space(node.get_text(" ", strip=True))
        return self._extract_meta_property(soup, "og:title")

    def _extract_category(self, soup: BeautifulSoup) -> str:
        node = soup.select_one(".news-view-category a")
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

    def _extract_date_text(self, soup: BeautifulSoup) -> str:
        node = soup.select_one(".news-view-date")
        if node is None:
            return ""
        return normalize_space(node.get_text(" ", strip=True))

    def _extract_content_text(self, container: Tag | None) -> str:
        if container is None:
            return ""

        content_soup = BeautifulSoup(str(container), "lxml")
        body = content_soup.select_one(".news-view-body") or content_soup
        for selector in ("script", "style", "noscript", "iframe"):
            for node in body.select(selector):
                node.decompose()

        paragraphs: list[str] = []
        for paragraph in body.find_all("p"):
            if paragraph.find("p") is not None:
                continue
            text = normalize_space(paragraph.get_text(" ", strip=True))
            if not text:
                continue
            paragraphs.append(text)

        content_text = "\n\n".join(unique_preserving_order(paragraphs)).strip()
        if content_text:
            return content_text
        return normalize_space(body.get_text(" ", strip=True))

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        src = image_node.get("src", "").strip()
        if not src or src.startswith("data:image"):
            return ""
        return make_absolute_url(self.base_url, src)

    def _extract_video_url(self, soup: BeautifulSoup) -> str:
        node = soup.select_one('a[href*="video.azertag.az/video"]')
        if node and node.get("href"):
            return node["href"].strip()
        return ""

    def _extract_meta_property(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"])
        return ""

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
