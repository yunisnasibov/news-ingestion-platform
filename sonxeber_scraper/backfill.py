from __future__ import annotations

from dataclasses import dataclass, field

from .azertag_client import AzertagClient
from .config import Settings
from .db import Database
from .source_locks import SourceLockManager


@dataclass(slots=True)
class BackfillSummary:
    source_name: str
    pages_scanned: int = 0
    listing_candidates: int = 0
    inserted_articles: int = 0
    updated_articles: int = 0
    skipped_existing_articles: int = 0
    errors: list[str] = field(default_factory=list)
    last_page_scanned: int = 0
    stopped_reason: str = ""
    archive_errors: int = 0


class AzertagBackfillService:
    def __init__(self, settings: Settings, database: Database, client: AzertagClient) -> None:
        self.settings = settings
        self.database = database
        self.client = client
        self.locks = SourceLockManager(settings)

    def run(
        self,
        *,
        max_pages: int = 0,
        stop_after_empty_pages: int = 3,
        wait_for_live_seconds: int = 120,
    ) -> BackfillSummary:
        summary = BackfillSummary(source_name=self.client.source_name)

        min_id = self.database.get_min_source_article_id(self.client.source_name)
        if min_id is None:
            max_id = self.database.get_max_source_article_id(self.client.source_name)
            if max_id is not None:
                min_id = max_id
            else:
                # No data at all — discover the latest ID from Azertag listing
                try:
                    candidates, _ = self.client.discover_listing_candidates(page_count=1)
                    if candidates:
                        ids = [c.source_article_id for c in candidates.values() if c.source_article_id]
                        min_id = max(ids) if ids else 4500000
                    else:
                        min_id = 4500000
                except Exception:
                    min_id = 4500000

        current_id = min_id
        consecutive_empty_batches = 0
        batch_size = 50
        batches_scanned = 0

        with self.locks.backfill_lock(
            self.client.source_name,
            wait_for_live_seconds=wait_for_live_seconds,
        ):
            while True:
                if max_pages > 0 and batches_scanned >= max_pages:
                    summary.stopped_reason = "max_batches_reached"
                    break

                if current_id <= 0:
                    summary.stopped_reason = "reached_id_zero"
                    break

                articles, errors = self.client.discover_backward_probe_articles(current_id, batch_size)
                if errors:
                    summary.errors.extend(errors)

                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = current_id
                summary.listing_candidates += len(articles)

                if not articles:
                    consecutive_empty_batches += 1
                    print(
                        "backfill_progress"
                        f" source={self.client.source_name}"
                        f" current_id={current_id}"
                        " candidates=0"
                        f" empty_streak={consecutive_empty_batches}",
                        flush=True,
                    )
                    if consecutive_empty_batches >= stop_after_empty_pages * 5:
                        summary.stopped_reason = "archive_tail_reached_too_many_empty"
                        break
                    current_id -= batch_size
                    continue

                consecutive_empty_batches = 0

                page_urls = [a.url for a in articles.values()]
                existing_urls = self.database.get_existing_article_urls(self.client.source_name, page_urls)
                summary.skipped_existing_articles += len(existing_urls)

                inserted_before = summary.inserted_articles
                updated_before = summary.updated_articles
                for record in articles.values():
                    if record.url in existing_urls:
                        continue
                    try:
                        status = self.database.upsert_article(record)
                        if status == "inserted":
                            summary.inserted_articles += 1
                        else:
                            summary.updated_articles += 1
                    except Exception as exc:
                        summary.errors.append(f"id={record.source_article_id}: {exc}")

                print(
                    "backfill_progress"
                    f" source={self.client.source_name}"
                    f" current_id={current_id}"
                    f" candidates={len(articles)}"
                    f" inserted={summary.inserted_articles - inserted_before}"
                    f" updated={summary.updated_articles - updated_before}"
                    f" skipped_existing={len(existing_urls)}",
                    flush=True,
                )
                current_id -= batch_size

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary



class DummyBackfillService:
    def __init__(self, source_name: str):
        self.source_name = source_name
    def run(self, **kwargs) -> BackfillSummary:
        return BackfillSummary(
            source_name=self.source_name,
            stopped_reason="historical_backfill_not_supported_for_source"
        )

def build_backfill_service(
    settings: Settings,
    database: Database,
    source_name: str,
):
    if source_name != AzertagClient.source_name:
        return DummyBackfillService(source_name)
    return AzertagBackfillService(settings, database, AzertagClient(settings))
