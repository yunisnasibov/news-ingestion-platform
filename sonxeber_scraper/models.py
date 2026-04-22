from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ListingCandidate:
    url: str
    slug: str
    source_article_id: int | None = None
    title: str = ""
    category: str = ""
    published_at: str = ""
    list_date_text: str = ""
    teaser: str = ""
    list_image_url: str = ""
    discovery_sources: set[str] = field(default_factory=set)

    def merge(self, other: "ListingCandidate") -> None:
        if self.source_article_id is None and other.source_article_id is not None:
            self.source_article_id = other.source_article_id
        if not self.title and other.title:
            self.title = other.title
        if not self.category and other.category:
            self.category = other.category
        if not self.published_at and other.published_at:
            self.published_at = other.published_at
        if not self.list_date_text and other.list_date_text:
            self.list_date_text = other.list_date_text
        if not self.teaser and other.teaser:
            self.teaser = other.teaser
        if not self.list_image_url and other.list_image_url:
            self.list_image_url = other.list_image_url
        if not self.slug and other.slug:
            self.slug = other.slug
        if other.url:
            self.url = other.url
        self.discovery_sources.update(other.discovery_sources)


@dataclass(slots=True)
class ArticleRecord:
    source_name: str
    source_article_id: int
    slug: str
    url: str
    canonical_url: str
    title: str
    category: str
    published_date_raw: str
    published_at: str
    list_date_text: str
    teaser: str
    content_text: str
    hero_image_url: str
    gallery_image_urls: list[str]
    video_embed_url: str
    list_image_url: str
    discovery_sources: list[str]
    content_hash: str


@dataclass(slots=True)
class SyncSummary:
    listing_candidates: int = 0
    probed_candidates: int = 0
    new_articles: int = 0
    updated_articles: int = 0
    skipped_existing_articles: int = 0
    skipped_due_to_backfill: bool = False
    errors: list[str] = field(default_factory=list)
