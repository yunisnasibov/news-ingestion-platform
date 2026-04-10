from __future__ import annotations

import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup, Tag

from .config import Settings
from .models import ArticleRecord, ListingCandidate
from .utils import (
    extract_yeniazerbaycan_article_id,
    extract_yeniazerbaycan_category_slug,
    is_valid_yeniazerbaycan_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
    parse_yeniazerbaycan_datetime,
    sha256_text,
    unique_preserving_order,
)


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class YeniAzerbaycanClient:
    source_name = "yeniazerbaycan.com"
    supports_forward_probe = False

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = "https://www.yeniazerbaycan.com"
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        self.latest_url = f"{self.base_url}/SonXeber_az.html"
        self.rss_url = "http://www.yeniazerbaycan.com/rss.xml"
        self.default_image_url = (
            f"{self.base_url}/front/assets/images/yeni-azerbaycan-logo-2021-11.png"
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "az,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.base_url,
            }
        )

    def discover_listing_candidates(
        self,
        page_count: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]:
        candidates: dict[str, ListingCandidate] = {}
        errors: list[str] = []

        try:
            for candidate in self._discover_from_sitemap(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:sitemap: {exc}")

        try:
            for candidate in self._discover_from_rss(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:rss: {exc}")

        for label, url in self._listing_urls(page_count):
            try:
                for candidate in self._discover_from_listing_page(label, url):
                    self._merge_candidate(candidates, candidate)
            except Exception as exc:
                errors.append(f"listing:{label}: {exc}")

        try:
            for candidate in self._discover_from_homepage(page_count):
                self._merge_candidate(candidates, candidate)
        except Exception as exc:
            errors.append(f"listing:homepage: {exc}")

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
        if not canonical_url or not is_valid_yeniazerbaycan_article_url(canonical_url):
            raise ValueError(f"Invalid canonical URL for article: {candidate.url}")

        source_article_id = (
            extract_yeniazerbaycan_article_id(canonical_url) or candidate.source_article_id
        )
        if source_article_id is None:
            raise ValueError(f"Missing yeniazerbaycan article id for {candidate.url}")

        print_container = soup.select_one("div#print")
        if print_container is None:
            raise ValueError(f"Missing print container for {candidate.url}")

        content_container = print_container.select_one("div.brd-b.pb-10.mt-20.ln-25")
        if content_container is None:
            raise ValueError(f"Missing content container for {candidate.url}")

        category = (
            self._extract_breadcrumb_category(soup)
            or candidate.category
            or extract_yeniazerbaycan_category_slug(canonical_url)
            or "Xəbər lenti"
        )
        category = normalize_space(category)

        published_date_raw = self._extract_visible_date(print_container) or candidate.list_date_text
        published_at = (
            parse_yeniazerbaycan_datetime(published_date_raw)
            or candidate.published_at
            or published_date_raw
        )
        if not published_at:
            raise ValueError(f"Missing published time for {candidate.url}")

        teaser = (
            self._extract_meta_name(soup, "description")
            or self._extract_meta_property_raw(soup, "og:description")
            or candidate.teaser
        )

        content_text = self._extract_content_text(content_container)
        title = normalize_space(
            self._extract_meta_property_raw(soup, "og:title")
            or self._extract_title(print_container)
            or candidate.title
        )
        if not title:
            title = self._derive_title_from_content(content_text)

        if not title and not content_text and not teaser:
            self._raise_soft_not_found(candidate.url)
        if not title:
            raise ValueError(f"Missing title for {candidate.url}")
        if not content_text:
            self._raise_soft_not_found(candidate.url)

        og_image = self._extract_meta_property_url(soup, "og:image")
        main_image = self._extract_image_url(content_container.select_one("img"))
        body_images = [
            self._extract_image_url(node) for node in content_container.select("img")
        ]
        gallery_image_urls = unique_preserving_order(
            [og_image, main_image, *body_images, candidate.list_image_url]
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

    def _discover_from_sitemap(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            self.sitemap_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 75, 150)
        for url_node in root.findall("sm:url", SITEMAP_NS):
            loc_node = url_node.find("sm:loc", SITEMAP_NS)
            if loc_node is None or not loc_node.text:
                continue

            article_url = self._normalize_article_url(loc_node.text.strip())
            if not is_valid_yeniazerbaycan_article_url(article_url):
                continue

            lastmod_text = ""
            lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
            if lastmod_node is not None and lastmod_node.text:
                lastmod_text = normalize_space(lastmod_node.text)

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_yeniazerbaycan_article_id(article_url),
                    category=extract_yeniazerbaycan_category_slug(article_url),
                    published_at=parse_yeniazerbaycan_datetime(lastmod_text) or lastmod_text,
                    list_date_text=lastmod_text,
                    discovery_sources={"sitemap"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _discover_from_rss(self, page_count: int) -> list[ListingCandidate]:
        response = self.session.get(
            self.rss_url,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)

        candidates: list[ListingCandidate] = []
        limit = max(page_count * 20, 40)
        for item in root.findall("./channel/item"):
            link = normalize_space(item.findtext("link", default=""))
            article_url = self._normalize_article_url(link)
            if not is_valid_yeniazerbaycan_article_url(article_url):
                continue

            raw_pub_date = normalize_space(item.findtext("pubDate", default=""))
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_yeniazerbaycan_article_id(article_url),
                    title=normalize_space(item.findtext("title", default="")),
                    category=extract_yeniazerbaycan_category_slug(article_url),
                    published_at=parse_yeniazerbaycan_datetime(raw_pub_date) or raw_pub_date,
                    list_date_text=raw_pub_date,
                    teaser=normalize_space(item.findtext("description", default="")),
                    discovery_sources={"rss"},
                )
            )
            if len(candidates) >= limit:
                break

        return candidates

    def _listing_urls(self, page_count: int) -> list[tuple[str, str]]:
        urls: list[tuple[str, str]] = [("son-xeber-page-1", self.latest_url)]
        for page_number in range(2, page_count + 1):
            urls.append(
                (
                    f"son-xeber-page-{page_number}",
                    f"{self.base_url}/SonXeber_{page_number}_az.html",
                )
            )
        return urls

    def _discover_from_listing_page(self, label: str, url: str) -> list[ListingCandidate]:
        soup, _ = self._get_soup(url)
        candidates: list[ListingCandidate] = []

        for card in soup.select("div.listing-news"):
            title_link = card.select_one("a.listing-title[href]")
            if title_link is None:
                continue

            article_url = self._normalize_article_url(title_link.get("href", "").strip())
            if not is_valid_yeniazerbaycan_article_url(article_url):
                continue

            category_node = card.select_one("div.listing-news-img p")
            date_node = card.select_one("p.fz14.fwl")
            image_node = card.select_one("div.listing-news-img img")
            raw_date = normalize_space(date_node.get_text(" ", strip=True)) if date_node else ""

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_yeniazerbaycan_article_id(article_url),
                    title=normalize_space(title_link.get_text(" ", strip=True)),
                    category=normalize_space(category_node.get_text(" ", strip=True))
                    if category_node
                    else extract_yeniazerbaycan_category_slug(article_url),
                    published_at=parse_yeniazerbaycan_datetime(raw_date) or raw_date,
                    list_date_text=raw_date,
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={label},
                )
            )

        return candidates

    def _discover_from_homepage(self, page_count: int) -> list[ListingCandidate]:
        soup, _ = self._get_soup(self.base_url)
        candidates: list[ListingCandidate] = []

        ticker_limit = max(page_count * 20, 40)
        for anchor in soup.select("ul.my-news-ticker li a[href]"):
            article_url = self._normalize_article_url(anchor.get("href", "").strip())
            if not is_valid_yeniazerbaycan_article_url(article_url):
                continue

            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_yeniazerbaycan_article_id(article_url),
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    discovery_sources={"homepage-ticker"},
                )
            )
            if len(candidates) >= ticker_limit:
                break

        side_limit = max(page_count * 25, 50)
        side_count = 0
        for card in soup.select("div.side-news"):
            anchor = card.select_one("a.title-side-black[href]")
            if anchor is None:
                continue

            article_url = self._normalize_article_url(anchor.get("href", "").strip())
            if not is_valid_yeniazerbaycan_article_url(article_url):
                continue

            image_node = card.select_one("div.side-news-img img")
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug="",
                    source_article_id=extract_yeniazerbaycan_article_id(article_url),
                    title=normalize_space(anchor.get_text(" ", strip=True)),
                    list_image_url=self._extract_image_url(image_node),
                    discovery_sources={"homepage-side-news"},
                )
            )
            side_count += 1
            if side_count >= side_limit:
                break

        return candidates

    def _get_soup(self, url: str) -> tuple[BeautifulSoup, str]:
        response = self.session.get(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        return BeautifulSoup(response.content, "lxml"), self._normalize_article_url(response.url)

    def _normalize_article_url(self, url: str) -> str:
        absolute_url = normalize_url(make_absolute_url(f"{self.base_url}/", url))
        prefixes = (
            "http://yeniazerbaycan.com",
            "https://yeniazerbaycan.com",
            "http://www.yeniazerbaycan.com",
            "https://www.yeniazerbaycan.com",
        )
        for prefix in prefixes:
            if absolute_url.startswith(prefix):
                return f"{self.base_url}{absolute_url[len(prefix):]}"
        return absolute_url

    def _extract_canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and node.get("href"):
            return self._normalize_article_url(node["href"].strip())
        return self._normalize_article_url(final_url)

    def _extract_title(self, print_container: Tag) -> str:
        node = print_container.select_one("h3.fz20.fwb")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_visible_date(self, print_container: Tag) -> str:
        node = print_container.select_one("p.fz14.fwl.c-g")
        if node:
            return normalize_space(node.get_text(" ", strip=True))
        return ""

    def _extract_breadcrumb_category(self, soup: BeautifulSoup) -> str:
        for anchor in soup.select('section a[href$="_az.html"]'):
            href = normalize_space(anchor.get("href", ""))
            if not href or href == "az.html":
                continue
            value = normalize_space(anchor.get_text(" ", strip=True))
            if value:
                return value
        return ""

    def _extract_meta_property_raw(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _extract_meta_property_url(self, soup: BeautifulSoup, property_name: str) -> str:
        node = soup.select_one(f'meta[property="{property_name}"]')
        if node and node.get("content"):
            return self._normalize_media_url(node["content"].strip())
        return ""

    def _extract_meta_name(self, soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_space(node["content"].strip())
        return ""

    def _normalize_media_url(self, value: str) -> str:
        if not value:
            return ""
        absolute_url = make_absolute_url(f"{self.base_url}/", value.strip())
        if absolute_url.startswith("http://www.yeniazerbaycan.com"):
            return "https://www.yeniazerbaycan.com" + absolute_url.removeprefix(
                "http://www.yeniazerbaycan.com"
            )
        if absolute_url.startswith("http://yeniazerbaycan.com"):
            return "https://www.yeniazerbaycan.com" + absolute_url.removeprefix(
                "http://yeniazerbaycan.com"
            )
        return absolute_url

    def _extract_image_url(self, image_node: Tag | None) -> str:
        if image_node is None:
            return ""
        for attribute in ("data-src", "src"):
            value = image_node.get(attribute, "").strip()
            if value and not value.startswith("data:image"):
                return self._normalize_media_url(value)
        return ""

    def _extract_content_text(self, container: Tag) -> str:
        for unwanted in container.select("script, style, noscript, iframe"):
            unwanted.decompose()

        paragraphs = [
            normalize_space(node.get_text(" ", strip=True))
            for node in container.select("p")
            if normalize_space(node.get_text(" ", strip=True))
        ]
        if paragraphs:
            return "\n\n".join(paragraphs)

        return normalize_space(container.get_text(" ", strip=True))

    def _derive_title_from_content(self, content_text: str) -> str:
        if not content_text:
            return ""
        first_line = normalize_space(content_text.splitlines()[0])
        if not first_line:
            return ""
        words = first_line.split()
        if len(words) <= 18:
            return first_line
        return " ".join(words[:18]).strip()

    def _extract_video_embed_url(self, container: Tag) -> str:
        iframe = container.select_one("iframe[src]")
        if iframe and iframe.get("src"):
            return self._normalize_media_url(iframe["src"].strip())
        return ""

    def _raise_soft_not_found(self, url: str) -> None:
        response = requests.Response()
        response.status_code = 404
        response.url = url
        raise requests.HTTPError(f"Soft 404 for {url}", response=response)

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
