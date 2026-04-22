from __future__ import annotations

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_islamazeri_image_article_id,
    extract_islamazeri_slug,
    is_valid_islamazeri_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_islamazeri_datetime,
    sha256_text,
    stable_bigint_from_text,
    unique_preserving_order,
)


class IslamAzeriClient:
    source_name = "islamazeri.com"
    supports_forward_probe = False
    category_pages = (
        ("olke-siyaset", "/siyast/", "ÖLKƏ / SİYASƏT"),
        ("olke-munasibet", "/munasibet/", "ÖLKƏ / MÜNASİBƏT"),
        ("olke-shiyye", "/shiyye/", "ÖLKƏ / ŞİYYƏ"),
        ("olke-harbi", "/harbi/", "ÖLKƏ / HƏRBİ"),
        ("olke-tehsil", "/thsil/", "ÖLKƏ / TƏHSİL"),
        ("olke-hadise", "/hadis/", "ÖLKƏ / HADİSƏ"),
        ("olke-iqtisadiyyat", "/iqtisadiyyat/", "ÖLKƏ / İQTİSADİYYAT"),
        ("olke-muxtelif", "/muxtlif/", "ÖLKƏ / MÜXTƏLİF"),
        ("olke-din", "/din/", "ÖLKƏ / DİN"),
        ("region-gurcustan", "/gurcustan/", "REGİON / GÜRCÜSTAN"),
        ("region-rusiya", "/rusiya/", "REGİON / RUSİYA"),
        ("region-iran", "/iran/", "REGİON / İRAN"),
        ("region-turkiye", "/turkiye/", "REGİON / TÜRKİYƏ"),
        ("region-ermenistan", "/ermnistan/", "REGİON / ERMƏNİSTAN"),
        ("dunya-idman", "/idman/", "DÜNYA / İDMAN"),
        ("dunya-islam", "/islam-dunyasi/", "DÜNYA / İSLAM DÜNYASI"),
        ("dunya-texnologiya", "/texnologiya/", "DÜNYA / TEXNOLOGİYA"),
        ("dunya-maraqli", "/maraqli/", "DÜNYA / MARAQLI"),
        ("dunya-herbi-munaqiseler", "/hrbi-munaqislr/", "DÜNYA / HƏRBİ MÜNAQİŞƏLƏR"),
        ("dunya-medeniyyet", "/medeniyyet/", "DÜNYA / MƏDƏNİYYƏT"),
        ("redaktordan", "/redaktordan/", "REDAKTORDAN"),
    )

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://www.islamazeri.com"
        self.latest_url = f"{self.base_url}/x%C9%99b%C9%99rl%C9%99r/"
        self.default_image_url = f"{self.base_url}/3goz/images/logo/logo.png"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "az,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.latest_url,
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
                for candidate in self._discover_from_latest_page(label, url):
                    self._merge_candidate(candidates, candidate)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")

        try:
            for candidate in self._discover_from_homepage_breaking(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage-breaking: {exc}")

        try:
            for candidate in self._discover_from_homepage_featured(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage-featured: {exc}")

        for label, category_url, category_label in self.category_pages:
            try:
                for candidate in self._discover_from_category_page(label, category_url, category_label):
                    self._merge_candidate(candidates, candidate)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")

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
        if not canonical_url or not is_valid_islamazeri_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        detail = soup.select_one("section.news-details-area div.blog-details-desc")
        if detail is None:
            raise ValueError(f"Missing detail container for {candidate.url}")

        content_container = detail.select_one("div.article-content")
        if content_container is None:
            raise ValueError(f"Missing article body for {candidate.url}")

        title = (
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(detail)
            or candidate.title
        )
        title = normalize_space(title)
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")

        published_date_raw = self._extract_visible_datetime(content_container) or candidate.list_date_text
        published_at = (
            parse_islamazeri_datetime(published_date_raw)
            or parse_islamazeri_datetime(candidate.published_at)
            or candidate.published_at
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
            or self._extract_first_paragraph(content_container)
        )
        teaser = normalize_space(teaser)

        content_text = self._extract_content_text(content_container)
        if not content_text:
            raise ValueError(f"Missing content text for {candidate.url}")

        og_image = self._extract_meta_property_raw(soup, "og:image")
        article_image = self._extract_image_url(detail.select_one("div.article-image img"))
        body_images = [self._extract_image_url(node) for node in content_container.select("img")]
        gallery_image_urls = unique_preserving_order(
            [og_image, article_image, *body_images, candidate.list_image_url]
        )
        hero_image_url = gallery_image_urls[0] if gallery_image_urls else self.default_image_url
        if not gallery_image_urls:
            gallery_image_urls = [hero_image_url]

        source_article_id = (
            candidate.source_article_id
            or extract_islamazeri_image_article_id(hero_image_url)
            or stable_bigint_from_text(canonical_url)
        )

        category = (
            self._extract_detail_tag_category(soup, canonical_url)
            or candidate.category
            or "Xəbərlər"
        )
        category = normalize_space(category)

        video_embed_url = self._extract_video_embed_url(detail)
        content_hash = sha256_text(content_text)

        return ArticleRecord(
            source_name=self.source_name,
            source_article_id=source_article_id,
            slug=extract_islamazeri_slug(canonical_url) or candidate.slug,
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

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("latest-page-1", self.latest_url)]
        for page_number in range(2, page_count + 1):
            urls.append((f"latest-page-{page_number}", f"{self.latest_url}?pages={page_number}"))
        return urls

    def _discover_from_latest_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for card in soup.select("div.single-news-item"):
            link = card.select_one("div.news-content h3 a[href]") or card.select_one("a[href]")
            if link is None:
                continue

            article_url = normalize_url(make_absolute_url(self.base_url, link.get("href", "").strip()))
            if not is_valid_islamazeri_article_url(article_url):
                continue

            image_url = self._extract_image_url(card.select_one("div.news-image img, img"))
            paragraphs = card.select("div.news-content p")
            teaser = normalize_space(paragraphs[0].get_text(" ", strip=True)) if paragraphs else ""
            raw_date = normalize_space(paragraphs[1].get_text(" ", strip=True)) if len(paragraphs) > 1 else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islamazeri_slug(article_url),
                    source_article_id=extract_islamazeri_image_article_id(image_url),
                    title=normalize_space(link.get_text(" ", strip=True)),
                    published_at=parse_islamazeri_datetime(raw_date) or raw_date,
                    list_date_text=raw_date,
                    teaser=teaser,
                    list_image_url=image_url,
                    discovery_sources={label},
                )
            )

        return candidates

    def _discover_from_homepage_breaking(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        candidates: list[ListingCandidate] = []
        limit = max(page_count * 15, 30)

        for anchor in soup.select("div.single-breaking-news a[href]"):
            article_url = normalize_url(make_absolute_url(self.base_url, anchor.get("href", "").strip()))
            if not is_valid_islamazeri_article_url(article_url):
                continue

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islamazeri_slug(article_url),
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    discovery_sources={"homepage-breaking"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_homepage_featured(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        candidates: list[ListingCandidate] = []
        limit = max(page_count * 20, 40)

        for card in soup.select("div.single-main-default-news, div.single-main-default-news-inner"):
            title_link = card.select_one("div.news-content h3 a[href]") or card.select_one("a[href]")
            if title_link is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, title_link.get("href", "").strip())
            )
            if not is_valid_islamazeri_article_url(article_url):
                continue

            image_url = self._extract_image_url(card.select_one("img"))
            category_node = card.select_one("div.news-content div.tag")

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islamazeri_slug(article_url),
                    source_article_id=extract_islamazeri_image_article_id(image_url),
                    title=normalize_space(title_link.get_text(" ", strip=True)),
                    category=normalize_space(category_node.get_text(" ", strip=True))
                    if category_node
                    else "",
                    list_image_url=image_url,
                    discovery_sources={"homepage-featured"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_category_page(
        self,
        label: str,
        category_path: str,
        category_label: str,
    ) -> list[ListingCandidate]:
        soup, _ = self._get_soup(make_absolute_url(self.base_url, category_path))
        candidates: list[ListingCandidate] = []

        for card in soup.select("div.single-news-item"):
            link = card.select_one("div.news-content h3 a[href]") or card.select_one("a[href]")
            if link is None:
                continue

            article_url = normalize_url(
                make_absolute_url(self.base_url, link.get("href", "").strip())
            )
            if not is_valid_islamazeri_article_url(article_url):
                continue

            image_url = self._extract_image_url(card.select_one("div.news-image img, img"))
            paragraphs = card.select("div.news-content p")
            teaser = normalize_space(paragraphs[0].get_text(" ", strip=True)) if paragraphs else ""
            raw_date = normalize_space(paragraphs[1].get_text(" ", strip=True)) if len(paragraphs) > 1 else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=extract_islamazeri_slug(article_url),
                    source_article_id=extract_islamazeri_image_article_id(image_url),
                    title=normalize_space(link.get_text(" ", strip=True)),
                    category=category_label,
                    published_at=parse_islamazeri_datetime(raw_date) or raw_date,
                    list_date_text=raw_date,
                    teaser=teaser,
                    list_image_url=image_url,
                    discovery_sources={f"category-{label}"},
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

    def _extract_title(self, detail: Tag) -> str:
        node = detail.select_one("h1")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_datetime(self, content_container: Tag) -> str:
        node = content_container.select_one("span")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_first_paragraph(self, content_container: Tag) -> str:
        node = content_container.select_one("p")
        if node:
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if text and not text.startswith("Yayınlanma tarixi:"):
                return text
        return ""

    def _extract_detail_tag_category(self, soup: BeautifulSoup, canonical_url: str) -> str:
        target_url = normalize_url(canonical_url)
        for card in soup.select("div.single-main-default-news, div.single-main-default-news-inner"):
            link = card.select_one("div.news-content h3 a[href], a[href]")
            if link is None:
                continue
            article_url = normalize_url(
                make_absolute_url(self.base_url, link.get("href", "").strip())
            )
            if article_url != target_url:
                continue
            tag = card.select_one("div.news-content div.tag")
            if tag:
                return normalize_space(tag.get_text(" ", strip=True))
        return ""

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "data-lazy-src", "src"):
            value = normalize_space(image_node.get(attribute, ""))
            if value and not value.startswith("data:image"):
                return make_absolute_url(self.base_url, value)
        return ""

    def _extract_video_embed_url(self, detail: Tag) -> str:
        node = detail.select_one("iframe[src], video[src], video source[src]")
        if node and node.get("src"):
            return make_absolute_url(self.base_url, node["src"].strip())
        return ""

    def _extract_content_text(self, content_container: Tag) -> str:
        paragraphs: list[str] = []
        for node in content_container.select("p, li, blockquote, h2, h3, h4"):
            if node.find_parent(["aside", "nav"]) is not None:
                continue
            text = normalize_space(node.get_text(" ", strip=True).replace("\xa0", " "))
            if not text:
                continue
            if text.startswith("Yayınlanma tarixi:"):
                continue
            paragraphs.append(text)

        if paragraphs:
            return "\n\n".join(unique_preserving_order(paragraphs)).strip()
        fallback = normalize_space(content_container.get_text(" ", strip=True).replace("\xa0", " "))
        if fallback.startswith("Yayınlanma tarixi:"):
            return ""
        return fallback

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
