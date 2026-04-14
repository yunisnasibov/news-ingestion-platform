from __future__ import annotations

import time
from typing import Protocol

from .config import Settings
from .db import Database
from .models import ListingCandidate, SyncSummary
from .source_locks import SourceLockError, SourceLockManager
from .utils import normalize_url, utc_now_iso


class SyncClient(Protocol):
    source_name: str
    supports_forward_probe: bool

    def discover_listing_candidates(
        self,
        page_count: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]: ...

    def discover_probe_candidates(
        self,
        max_article_id: int,
        window: int,
    ) -> tuple[dict[str, ListingCandidate], list[str]]: ...

    def fetch_article(self, candidate: ListingCandidate): ...


class SiteSyncService:
    def __init__(self, settings: Settings, database: Database, client: SyncClient) -> None:
        self.settings = settings
        self.database = database
        self.client = client
        self.locks = SourceLockManager(settings)

    def sync_once(self, page_count: int | None = None) -> SyncSummary:
        started_at = utc_now_iso()
        summary = SyncSummary()
        effective_page_count = page_count or self.settings.listing_page_count

        if self.locks.has_backfill_lock(self.client.source_name):
            summary.skipped_due_to_backfill = True
            self.database.record_sync_run(self.client.source_name, summary, started_at)
            return summary

        try:
            with self.locks.live_lock(self.client.source_name):
                return self._run_sync_once(summary, effective_page_count, started_at)
        except SourceLockError as exc:
            if "backfill_active" in str(exc):
                summary.skipped_due_to_backfill = True
            else:
                summary.errors.append(str(exc))
            self.database.record_sync_run(self.client.source_name, summary, started_at)
            return summary

    def _run_sync_once(
        self,
        summary: SyncSummary,
        effective_page_count: int,
        started_at: str,
    ) -> SyncSummary:

        candidates, listing_errors = self.client.discover_listing_candidates(effective_page_count)
        summary.errors.extend(listing_errors)
        summary.listing_candidates = len(candidates)

        if self.client.supports_forward_probe:
            max_article_id = self.database.get_max_source_article_id(self.client.source_name)
            if max_article_id is not None:
                probed_candidates, probe_errors = self.client.discover_probe_candidates(
                    max_article_id,
                    self.settings.forward_probe_window,
                )
                summary.errors.extend(probe_errors)
                summary.probed_candidates = len(probed_candidates)
                for url, candidate in probed_candidates.items():
                    normalized_url = normalize_url(url)
                    if normalized_url in candidates:
                        candidates[normalized_url].merge(candidate)
                    else:
                        candidates[normalized_url] = candidate

        existing_urls = self.database.get_existing_article_urls(
            self.client.source_name,
            list(candidates.keys()),
        )
        new_candidates = {
            url: candidate
            for url, candidate in candidates.items()
            if url not in existing_urls
        }
        summary.skipped_existing_articles = len(existing_urls)

        for candidate in new_candidates.values():
            try:
                record = self.client.fetch_article(candidate)
                status = self.database.upsert_article(record)
                if status == "inserted":
                    summary.new_articles += 1
                else:
                    summary.updated_articles += 1
            except Exception as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                if status_code in {404, 410}:
                    continue
                label = candidate.source_article_id if candidate.source_article_id is not None else candidate.url
                summary.errors.append(f"{label}: {exc}")

        self.database.record_sync_run(self.client.source_name, summary, started_at)
        return summary

    def poll_forever(self) -> None:
        cycle = 0
        while True:
            cycle += 1
            page_count = self.settings.listing_page_count
            if cycle % self.settings.reconcile_every_cycles == 0:
                page_count = self.settings.reconcile_page_count
            summary = self.sync_once(page_count=page_count)
            print(self._format_summary(summary, page_count), flush=True)
            time.sleep(self.settings.poll_interval_seconds)

    def _format_summary(self, summary: SyncSummary, page_count: int) -> str:
        if summary.skipped_due_to_backfill:
            return (
                "sync_skipped"
                f" source={self.client.source_name}"
                " reason=backfill_lock_active"
            )
        error_suffix = f", errors={len(summary.errors)}" if summary.errors else ""
        return (
            "sync_complete"
            f" source={self.client.source_name}"
            f" pages={page_count}"
            f" listing_candidates={summary.listing_candidates}"
            f" probed_candidates={summary.probed_candidates}"
            f" new_articles={summary.new_articles}"
            f" skipped_existing={summary.skipped_existing_articles}"
            f"{error_suffix}"
        )
