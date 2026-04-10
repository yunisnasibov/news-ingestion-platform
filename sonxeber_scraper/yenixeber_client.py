from __future__ import annotations

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_published_date_raw,
    extract_yenixeber_article_id,
    extract_yenixeber_slug,
    is_valid_yenixeber_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_azerbaijani_date,
    sha256_text,
    unique_preserving_order,
)


class YenixeberClient:
    source_name = "yenixeber.az"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://yenixeber.az"
        self.default_image_url = "https://yenixeber.az/images/fbcover.jpg"
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

        for label, url, default_category in self._listing_urls(page_count):
            try:
                page_candidates = self._fetch_listing_candidates(label, url, default_category)
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
        return {}, []

    def fetch_article(self, candidate: ListingCandidate) -> ArticleRecord:
        soup, final_url = self._get_soup(candidate.url)
        canonical_url = self._extract_canonical_url(soup, final_url)
        if not canonical_url or not is_valid_yenixeber_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            extract_yenixeber_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing Yenixeber article id for {candidate.url}")

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
        breadcrumb_category = article.select_one(".breadcrumb_news a:last-child")
        category = (
            normalize_space(category_node.get_text(" ", strip=True))
            if category_node
            else normalize_space(breadcrumb_category.get_text(" ", strip=True))
            if breadcrumb_category
            else candidate.category
        )
        category = category or "uncategorized"

        right_text_node = datespan.select_one("span.right")
        published_date_raw = (
            extract_published_date_raw(right_text_node.get_text(" ", strip=True))
            if right_text_node
            else candidate.list_date_text
        )
        published_at = parse_azerbaijani_date(published_date_raw) or published_date_raw

        title = normalize_space(title_node.get_text(" ", strip=True)) or candidate.title
        if not title:
            raise ValueError(f"Empty title for {candidate.url}")

        teaser = self._extract_meta_content(soup, "description") or candidate.teaser
        content_text = self._extract_content_text(article) or teaser or title

        og_image = self._extract_meta_property(soup, "og:image")
        image_urls = self._extract_article_images(article)
        gallery_image_urls = unique_preserving_order(
            [og_image, *image_urls, candidate.list_image_url]
        )
        hero_image_url = (
            gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        )
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        video_embed_url = self._extract_video_embed_url(article)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_yenixeber_slug(canonical_url) or candidate.slug,
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

    def _listing_urls(self, page_count: int) -> list[tuple[str, str, str]]:
        urls: list[tuple[str, str, str]] = [
            ("homepage", self.base_url, ""),
            ("son-xeberler", f"{self.base_url}/son-xeberler", ""),
            ("xeberler-page-1", f"{self.base_url}/xeberler/", ""),
        ]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"xeberler-page-{page_number}",
                    f"{self.base_url}/xeberler/?start={page_number}",
                    "",
                )
            )

        for slug, category in self._category_pages().items():
            urls.append((f"category-{slug}", f"{self.base_url}/{slug}/", category))

        return urls

    def _fetch_listing_candidates(
        self,
        label: str,
        url: str,
        default_category: str,
    ) -> list[ListingCandidate]:
        soup, final_url = self._get_soup(url)
        if normalize_url(final_url) == normalize_url(self.base_url) and normalize_url(url) != normalize_url(self.base_url):
            return []

        container = soup.select_one("div.centerblok > div.newslister#prodwrap")
        if container is None:
            container = soup.select_one("div.newslister#prodwrap")
        if container is None:
            return []

        cards_root = container.select_one("div.newslister") or container
        candidates: list[ListingCandidate] = []
        for card in cards_root.find_all("div", class_=self._has_yxart_class, recursive=False):
            anchor = card.select_one("a.thumb_zoom[href]")
            if anchor is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, anchor.get("href", "").strip())
            )
            source_article_id = extract_yenixeber_article_id(article_url)
            if source_article_id is None:
                continue

            title_node = anchor.select_one("h3")
            title = normalize_space(
                title_node.get_text(" ", strip=True) if title_node else anchor.get("title", "")
            )
            teaser_node = card.select_one("p.artful")
            image_node = card.select_one("div.imgholder img")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_yenixeber_slug(article_url),
                    source_article_id=source_article_id,
                    title=title,
                    category=default_category,
                    teaser=normalize_space(teaser_node.get_text(" ", strip=True))
                    if teaser_node
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

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return normalize_url(make_absolute_url(self.base_url, node["href"].strip()))
        return normalize_url(final_url)

    def _extract_meta_property(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return make_absolute_url(self.base_url, node["content"].strip())
        return ""

    def _extract_meta_content(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_video_embed_url(self, article: Tag) -> str:
        iframe = article.select_one("iframe[src]")
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
        for image_node in article.select("img"):
            image_url = self._extract_image_url(image_node)
            if image_url:
                images.append(image_url)
        return unique_preserving_order(images)

    def _extract_content_text(self, article: Tag) -> str:
        paragraphs: list[str] = []
        for child in article.children:
            if not isinstance(child, Tag):
                continue
            if "datespan" in set(child.get("class", [])):
                break
            if child.name != "p":
                continue
            text = normalize_space(child.get_text(" ", strip=True))
            if text:
                paragraphs.append(text)

        content_text = "\n\n".join(paragraphs).strip()
        if content_text:
            return content_text
        return normalize_space(article.get_text(" ", strip=True))

    @staticmethod
    def _category_pages() -> dict[str, str]:
        return {
            "hadise-xeberleri": "Hadisə",
            "dunya-xeberleri": "Dünya",
            "siyasi-xeberler": "Siyasi",
            "sosial-xeberler": "Sosial",
            "sou-biznes-xeberleri": "Şou Biznes",
            "maraqli-xeberler": "Maraqlı",
            "iqtisadiyyat-xeberleri": "İqtisadiyyat",
            "idman-xeberleri": "İdman",
            "saglamliq-xeberleri": "Sağlamlıq",
            "turizm-xeberleri": "Turizm",
        }

    @staticmethod
    def _has_yxart_class(value: str | list[str] | None) -> bool:
        if value is None:
            return False
        classes = value.split() if isinstance(value, str) else value
        return "yxart" in classes
