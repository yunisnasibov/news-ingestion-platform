from __future__ import annotations

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_published_date_raw,
    extract_slug,
    extract_source_article_id,
    is_valid_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_date,
    sha256_text,
    unique_preserving_order,
)


class SonxeberClient:
    source_name = "sonxeber.az"
    supports_forward_probe = True

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://sonxeber.az"
        self.default_image_url = "https://sonxeber.az/images/fbcover.jpg"
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
        if not canonical_url or not is_valid_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = extract_source_article_id(canonical_url)
        if source_article_id is None:
            raise ValueError(f"Missing source article id for {candidate.url}")

        article = soup.select_one("article")
        if article is None:
            raise ValueError(f"Missing article container for {candidate.url}")

        title_node = article.select_one("h1")
        if title_node is None:
            raise ValueError(f"Missing title for {candidate.url}")

        datespan = article.select_one("div.datespan")
        if datespan is None:
            raise ValueError(f"Missing datespan for {candidate.url}")

        category_node = datespan.select_one("span.right a")
        right_text_node = datespan.select_one("span.right")
        category = (
            normalize_space(category_node.get_text(" ", strip=True))
            if category_node
            else candidate.category or "uncategorized"
        )
        published_date_raw = (
            extract_published_date_raw(right_text_node.get_text(" ", strip=True))
            if right_text_node
            else candidate.list_date_text
        )
        published_at = parse_azerbaijani_date(published_date_raw)

        content_text = self._extract_content_text(article)
        image_urls = self._extract_article_images(article)
        og_image = self._extract_og_image(soup)
        gallery_image_urls = unique_preserving_order(
            [og_image, *image_urls, candidate.list_image_url]
        )
        hero_image_url = (
            gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        )
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]
        video_embed_url = self._extract_video_embed_url(article)
        title = normalize_space(title_node.get_text(" ", strip=True))
        content_text = content_text or candidate.teaser or title
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_slug(canonical_url),
            url=normalize_url(canonical_url),
            canonical_url=normalize_url(canonical_url),
            title=title or candidate.title,
            category=category or "uncategorized",
            published_date_raw=published_date_raw,
            published_at=published_at,
            list_date_text=candidate.list_date_text,
            teaser=candidate.teaser,
            content_text=content_text,
            hero_image_url=hero_image_url,
            gallery_image_urls=gallery_image_urls,
            video_embed_url=video_embed_url,
            list_image_url=candidate.list_image_url or hero_image_url,
            discovery_sources=sorted(candidate.discovery_sources),
            content_hash=content_hash,
        )

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [
            ("homepage", self.base_url),
            ("son-xeberler", f"{self.base_url}/son-xeberler"),
            ("xeberler-page-1", f"{self.base_url}/xeberler/"),
        ]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"xeberler-page-{page_number}",
                    f"{self.base_url}/xeberler/?start={page_number}",
                )
            )
        return urls

    def _fetch_listing_candidates(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        container = soup.select_one("div.centerblok > div.newslister#prodwrap")
        if container is None:
            container = soup.select_one("div.newslister#prodwrap")
        if container is None:
            return []

        candidates: list[ListingCandidate] = []
        for card in container.find_all("div", class_=self._has_nart_class, recursive=False):
            anchor = card.select_one("a.thumb_zoom")
            if anchor is None:
                continue

            href = anchor.get("href", "").strip()
            article_url = normalize_url(make_absolute_url(self.base_url, href))
            source_article_id = extract_source_article_id(article_url)
            if source_article_id is None:
                continue

            title = normalize_space(
                (anchor.select_one("h3") or anchor).get_text(" ", strip=True)
            )
            teaser_node = card.select_one("p.artful")
            date_node = card.select_one("span.dttime")
            image_node = card.select_one("div.imgholder img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_slug(article_url),
                    source_article_id=source_article_id,
                    title=title,
                    list_date_text=normalize_space(date_node.get_text(" ", strip=True))
                    if date_node
                    else "",
                    teaser=normalize_space(teaser_node.get_text(" ", strip=True))
                    if teaser_node
                    else "",
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )
        return candidates

    def _probe_article_id(self, source_article_id: int) -> ListingCandidate | None:
        probe_url = f"{self.base_url}/{source_article_id}"
        soup, final_url = self._get_soup(probe_url)

        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_article_url(canonical_url):
            return None

        canonical_url = normalize_url(canonical_url)
        canonical_id = extract_source_article_id(canonical_url)
        if canonical_id != source_article_id:
            return None

        article = soup.select_one("article")
        title_node = article.select_one("h1") if article else None
        datespan = article.select_one("div.datespan") if article else None
        if article is None or title_node is None or datespan is None:
            return None

        image_url = self._extract_og_image(soup)
        return ListingCandidate(
            url=canonical_url,
            slug=extract_slug(canonical_url),
            source_article_id=source_article_id,
            title=normalize_space(title_node.get_text(" ", strip=True)),
            list_image_url=image_url,
            discovery_sources={"id-probe"},
        )

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self.session.get(url, timeout=self.settings.request_timeout_seconds)
        response.raise_for_status()
        return BeautifulSoup(response.content, "lxml"), response.url

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return make_absolute_url(self.base_url, node["href"].strip())
        return final_url

    def _extract_og_image(self, soup: BeautifulSoup) -> str:
        node = soup.select_one('meta[property="og:image"]')
        if node and node.get("content"):
            return make_absolute_url(self.base_url, node["content"].strip())
        return ""

    def _extract_video_embed_url(self, article: Tag) -> str:
        iframe = article.select_one("div.embed-responsive iframe")
        if iframe and iframe.get("src"):
            return make_absolute_url(self.base_url, iframe["src"].strip())
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            candidate = image_node.get(attribute, "").strip()
            if candidate and not candidate.startswith("data:image"):
                return make_absolute_url(self.base_url, candidate)
        return ""

    def _extract_article_images(self, article: Tag) -> list[str]:
        images: list[str] = []
        for image_node in article.select("img.imgbcode"):
            image_url = self._extract_image_url(image_node)
            if image_url:
                images.append(image_url)
        return unique_preserving_order(images)

    def _extract_content_text(self, article: Tag) -> str:
        paragraphs: list[str] = []
        for child in article.children:
            if not isinstance(child, Tag):
                continue
            classes = set(child.get("class", []))
            if "datespan" in classes:
                break
            if child.name != "p":
                continue
            text = normalize_space(child.get_text(" ", strip=True))
            if not text:
                continue
            paragraphs.append(text)

        content_text = "\n\n".join(paragraphs).strip()
        if content_text:
            return content_text

        return normalize_space(article.get_text(" ", strip=True))

    @staticmethod
    def _has_nart_class(value: str | list[str] | None) -> bool:
        if value is None:
            return False
        classes = value.split() if isinstance(value, str) else value
        return "nart" in classes
