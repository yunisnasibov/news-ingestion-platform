from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .apa_client import ApaClient
from .axar_client import AxarClient
from .azertag_client import AzertagClient
from .azerbaijan_az_client import AzerbaijanAzClient
from .azxeber_client import AzxeberClient
from .client import SonxeberClient
from .ikisahil_client import IkiSahilClient
from .islam_client import IslamClient
from .islamazeri_client import IslamAzeriClient
from .iqtisadiyyat_client import IqtisadiyyatClient
from .metbuat_client import MetbuatClient
from .milli_client import MilliClient
from .one_news_client import OneNewsClient
from .oxu_client import OxuClient
from .report_client import ReportClient
from .config import Settings
from .db import Database
from .models import ArticleRecord, ListingCandidate
from .source_locks import SourceLockManager
from .utils import (
    extract_apa_article_id,
    extract_apa_category_slug,
    extract_apa_slug,
    is_valid_apa_article_url,
    make_absolute_url,
    normalize_space,
    normalize_url,
)
from .sia_client import SiaClient
from .siyasetinfo_client import SiyasetinfoClient
from .teleqraf_client import TeleqrafClient
from .xeberler_client import XeberlerClient
from .yenixeber_client import YenixeberClient


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


class HistoricalStateStore:
    def __init__(self, settings: Settings, source_name: str) -> None:
        self.path = settings.project_root / "data" / "backfill_state" / f"{self._safe_name(source_name)}.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, object]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def save(self, state: dict[str, object]) -> None:
        self.path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    @staticmethod
    def _safe_name(source_name: str) -> str:
        return source_name.replace("/", "_").replace(":", "_")


class HistoricalBackfillService:
    finished_reasons = {
        "completed",
        "reached_id_zero",
        "probe_tail_reached",
        "archive_tail_reached",
        "archive_signature_repeated",
        "listing_tail_reached",
        "corrupt_listing_tail_reached",
        "listing_404_tail_reached",
    }

    def __init__(self, settings: Settings, database: Database, client: object) -> None:
        self.settings = settings
        self.database = database
        self.client = client
        self.locks = SourceLockManager(settings)
        self.state_store = HistoricalStateStore(settings, getattr(client, "source_name"))

    def run(
        self,
        *,
        max_pages: int = 0,
        stop_after_empty_pages: int = 3,
        wait_for_live_seconds: int = 120,
    ) -> BackfillSummary:
        summary = BackfillSummary(source_name=self.client.source_name)
        with self.locks.backfill_lock(
            self.client.source_name,
            wait_for_live_seconds=wait_for_live_seconds,
        ):
            self._run(summary, max_pages=max_pages, stop_after_empty_pages=stop_after_empty_pages)
        if summary.stopped_reason in self.finished_reasons and not summary.errors:
            self.state_store.clear()
        return summary

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        raise NotImplementedError

    def _upsert_records(self, records: list[ArticleRecord], summary: BackfillSummary) -> None:
        if not records:
            return
        existing_urls = self.database.get_existing_article_urls(
            self.client.source_name,
            [record.url for record in records],
        )
        summary.skipped_existing_articles += len(existing_urls)
        for record in records:
            if record.url in existing_urls:
                continue
            try:
                status = self.database.upsert_article(record)
                if status == "inserted":
                    summary.inserted_articles += 1
                else:
                    summary.updated_articles += 1
            except Exception as exc:  # pragma: no cover - defensive on DB write path
                summary.errors.append(f"record[{record.source_article_id}]: {exc}")

    def _upsert_candidates(self, candidates: list[ListingCandidate], summary: BackfillSummary) -> None:
        if not candidates:
            return
        deduped: dict[str, ListingCandidate] = {}
        for candidate in candidates:
            key = normalize_url(candidate.url)
            if key in deduped:
                deduped[key].merge(candidate)
            else:
                deduped[key] = candidate
        ordered = list(deduped.values())
        summary.listing_candidates += len(ordered)
        existing_urls = self.database.get_existing_article_urls(
            self.client.source_name,
            [candidate.url for candidate in ordered],
        )
        summary.skipped_existing_articles += len(existing_urls)
        for candidate in ordered:
            if candidate.url in existing_urls:
                continue
            try:
                record = self.client.fetch_article(candidate)
            except Exception as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                if status_code in {404, 410}:
                    continue
                label = candidate.source_article_id if candidate.source_article_id is not None else candidate.url
                summary.errors.append(f"candidate[{label}]: {exc}")
                continue
            try:
                status = self.database.upsert_article(record)
                if status == "inserted":
                    summary.inserted_articles += 1
                else:
                    summary.updated_articles += 1
            except Exception as exc:  # pragma: no cover - defensive on DB write path
                summary.errors.append(f"record[{record.source_article_id}]: {exc}")


class AzertagHistoricalBackfillService(HistoricalBackfillService):
    batch_size = 50

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        current_id = int(state.get("current_id") or self._discover_start_id())
        consecutive_empty_batches = int(state.get("empty_batches") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break
            if current_id <= 0:
                summary.stopped_reason = "reached_id_zero"
                break

            articles, errors = self.client.discover_backward_probe_articles(current_id, self.batch_size)
            summary.errors.extend(errors)
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = current_id
            summary.listing_candidates += len(articles)

            if not articles:
                consecutive_empty_batches += 1
                if consecutive_empty_batches >= stop_after_empty_pages * 5:
                    summary.stopped_reason = "archive_tail_reached"
                    break
            else:
                consecutive_empty_batches = 0
                self._upsert_records(list(articles.values()), summary)

            current_id -= self.batch_size
            self.state_store.save(
                {
                    "batches_scanned": batches_scanned,
                    "current_id": current_id,
                    "empty_batches": consecutive_empty_batches,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"

    def _discover_start_id(self) -> int:
        min_id = self.database.get_min_source_article_id(self.client.source_name)
        if min_id is not None:
            return min_id
        max_id = self.database.get_max_source_article_id(self.client.source_name)
        if max_id is not None:
            return max_id
        try:
            candidates, _ = self.client.discover_listing_candidates(page_count=1)
        except Exception:
            return 4500000
        ids = [candidate.source_article_id for candidate in candidates.values() if candidate.source_article_id]
        return max(ids) if ids else 4500000


class SonxeberHistoricalBackfillService(HistoricalBackfillService):
    probe_batch_size = 50

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "listing")
        listing_start = int(state.get("listing_start") or 1)
        listing_empty_streak = int(state.get("listing_empty_streak") or 0)
        probe_current_id = state.get("probe_current_id")
        probe_empty_batches = int(state.get("probe_empty_batches") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "listing":
                url = f"{self.client.base_url}/xeberler/" if listing_start <= 1 else f"{self.client.base_url}/xeberler/?start={listing_start}"
                candidates = self.client._fetch_listing_candidates(f"history-start-{listing_start}", url)
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = listing_start

                if not candidates:
                    listing_empty_streak += 1
                    if listing_empty_streak >= stop_after_empty_pages:
                        phase = "probe"
                        probe_current_id = probe_current_id or self.database.get_min_source_article_id(self.client.source_name)
                        if probe_current_id is None:
                            probe_current_id = self.database.get_max_source_article_id(self.client.source_name)
                        if probe_current_id is None:
                            summary.stopped_reason = "listing_tail_reached"
                            break
                    else:
                        listing_start += 1
                    self.state_store.save(
                        {
                            "phase": phase,
                            "listing_start": listing_start,
                            "listing_empty_streak": listing_empty_streak,
                            "probe_current_id": probe_current_id,
                            "probe_empty_batches": probe_empty_batches,
                            "batches_scanned": batches_scanned,
                        }
                    )
                    continue

                listing_empty_streak = 0
                self._upsert_candidates(candidates, summary)
                listing_start += 1
                self.state_store.save(
                    {
                        "phase": phase,
                        "listing_start": listing_start,
                        "listing_empty_streak": listing_empty_streak,
                        "probe_current_id": probe_current_id,
                        "probe_empty_batches": probe_empty_batches,
                        "batches_scanned": batches_scanned,
                    }
                )
                continue

            current_id = int(probe_current_id or 0)
            if current_id <= 0:
                summary.stopped_reason = "reached_id_zero"
                break

            candidates: list[ListingCandidate] = []
            for source_article_id in range(current_id - 1, max(current_id - self.probe_batch_size - 1, 0), -1):
                try:
                    candidate = self.client._probe_article_id(source_article_id)
                except Exception as exc:
                    summary.errors.append(f"probe[{source_article_id}]: {exc}")
                    continue
                if candidate is not None:
                    candidates.append(candidate)

            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = current_id

            if not candidates:
                probe_empty_batches += 1
                if probe_empty_batches >= stop_after_empty_pages * 20:
                    summary.stopped_reason = "probe_tail_reached"
                    break
            else:
                probe_empty_batches = 0
                self._upsert_candidates(candidates, summary)

            probe_current_id = current_id - self.probe_batch_size
            self.state_store.save(
                {
                    "phase": "probe",
                    "listing_start": listing_start,
                    "listing_empty_streak": listing_empty_streak,
                    "probe_current_id": probe_current_id,
                    "probe_empty_batches": probe_empty_batches,
                    "batches_scanned": batches_scanned,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class AzerbaijanAzHistoricalBackfillService(HistoricalBackfillService):
    probe_batch_size = 50

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "listing")
        listing_page = int(state.get("listing_page") or 1)
        listing_empty_streak = int(state.get("listing_empty_streak") or 0)
        probe_current_id = state.get("probe_current_id")
        probe_empty_batches = int(state.get("probe_empty_batches") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "listing":
                url = f"{self.client.base_url}/news?page={listing_page}"
                candidates = self.client._fetch_listing_candidates(f"history-page-{listing_page}", url)
                signature = "|".join(candidate.url for candidate in candidates[:5])
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = listing_page

                if not candidates:
                    listing_empty_streak += 1
                    if listing_empty_streak >= stop_after_empty_pages:
                        phase = "probe"
                        probe_current_id = probe_current_id or self.database.get_min_source_article_id(self.client.source_name)
                        if probe_current_id is None:
                            probe_current_id = self.database.get_max_source_article_id(self.client.source_name)
                        if probe_current_id is None:
                            summary.stopped_reason = "listing_tail_reached"
                            break
                    else:
                        listing_page += 1
                    self.state_store.save({"phase": phase, "listing_page": listing_page, "listing_empty_streak": listing_empty_streak, "probe_current_id": probe_current_id, "probe_empty_batches": probe_empty_batches, "batches_scanned": batches_scanned})
                    continue

                if signature and state.get("last_signature") == signature:
                    summary.stopped_reason = "archive_signature_repeated"
                    break

                listing_empty_streak = 0
                self._upsert_candidates(candidates, summary)
                listing_page += 1
                self.state_store.save({"phase": phase, "listing_page": listing_page, "listing_empty_streak": listing_empty_streak, "probe_current_id": probe_current_id, "probe_empty_batches": probe_empty_batches, "batches_scanned": batches_scanned, "last_signature": signature})
                continue

            current_id = int(probe_current_id or 0)
            if current_id <= 0:
                summary.stopped_reason = "reached_id_zero"
                break

            candidates: list[ListingCandidate] = []
            for source_article_id in range(current_id - 1, max(current_id - self.probe_batch_size - 1, 0), -1):
                try:
                    candidate = self.client._probe_article_id(source_article_id)
                except Exception as exc:
                    summary.errors.append(f"probe[{source_article_id}]: {exc}")
                    continue
                if candidate is not None:
                    candidates.append(candidate)

            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = current_id

            if not candidates:
                probe_empty_batches += 1
                if probe_empty_batches >= stop_after_empty_pages * 20:
                    summary.stopped_reason = "probe_tail_reached"
                    break
            else:
                probe_empty_batches = 0
                self._upsert_candidates(candidates, summary)

            probe_current_id = current_id - self.probe_batch_size
            self.state_store.save({"phase": "probe", "listing_page": listing_page, "listing_empty_streak": listing_empty_streak, "probe_current_id": probe_current_id, "probe_empty_batches": probe_empty_batches, "batches_scanned": batches_scanned})

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class IkiSahilHistoricalBackfillService(HistoricalBackfillService):
    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "seed")
        listing_page = int(state.get("listing_page") or 1)
        empty_streak = int(state.get("empty_streak") or 0)
        last_signature = str(state.get("last_signature") or "")
        repeated_signature_count = int(state.get("repeated_signature_count") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "seed":
                try:
                    candidates = self.client._discover_from_rss(1)
                except Exception as exc:
                    summary.errors.append(f"seed[rss]: {exc}")
                    candidates = []
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = 0
                self._upsert_candidates(candidates, summary)
                phase = "listing"
                self.state_store.save({"phase": phase, "listing_page": listing_page, "empty_streak": empty_streak, "last_signature": last_signature, "repeated_signature_count": repeated_signature_count, "batches_scanned": batches_scanned})
                continue

            url = f"{self.client.base_url}/lent" if listing_page <= 1 else f"{self.client.base_url}/lent/p-{listing_page}"
            candidates = self.client._fetch_listing_candidates(f"lent-page-{listing_page}", url)
            signature = "|".join(candidate.url for candidate in candidates[:5])
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = listing_page

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "listing_tail_reached"
                    break
            else:
                empty_streak = 0
                if signature and signature == last_signature:
                    repeated_signature_count += 1
                    if repeated_signature_count >= 1:
                        summary.stopped_reason = "archive_signature_repeated"
                        break
                else:
                    repeated_signature_count = 0
                last_signature = signature
                self._upsert_candidates(candidates, summary)

            listing_page += 1
            self.state_store.save({"phase": phase, "listing_page": listing_page, "empty_streak": empty_streak, "last_signature": last_signature, "repeated_signature_count": repeated_signature_count, "batches_scanned": batches_scanned})

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class YenixeberHistoricalBackfillService(HistoricalBackfillService):
    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "seed")
        listing_page = int(state.get("listing_page") or 2)
        empty_streak = int(state.get("empty_streak") or 0)
        last_signature = str(state.get("last_signature") or "")
        repeated_signature_count = int(state.get("repeated_signature_count") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "seed":
                seed_candidates: list[ListingCandidate] = []
                for label, url, default_category in self.client._listing_urls(1):
                    try:
                        seed_candidates.extend(self.client._fetch_listing_candidates(label, url, default_category))
                    except Exception as exc:
                        summary.errors.append(f"seed[{label}]: {exc}")
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = 1
                self._upsert_candidates(seed_candidates, summary)
                phase = "listing"
                self.state_store.save({"phase": phase, "listing_page": listing_page, "empty_streak": empty_streak, "last_signature": last_signature, "repeated_signature_count": repeated_signature_count, "batches_scanned": batches_scanned})
                continue

            url = f"{self.client.base_url}/xeberler/?start={listing_page}"
            candidates = self.client._fetch_listing_candidates(f"xeberler-page-{listing_page}", url, "")
            signature = "|".join(candidate.url for candidate in candidates[:5])
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = listing_page

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "listing_tail_reached"
                    break
            else:
                empty_streak = 0
                if signature and signature == last_signature:
                    repeated_signature_count += 1
                    if repeated_signature_count >= 1:
                        summary.stopped_reason = "archive_signature_repeated"
                        break
                else:
                    repeated_signature_count = 0
                last_signature = signature
                self._upsert_candidates(candidates, summary)

            listing_page += 1
            self.state_store.save({"phase": phase, "listing_page": listing_page, "empty_streak": empty_streak, "last_signature": last_signature, "repeated_signature_count": repeated_signature_count, "batches_scanned": batches_scanned})

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class ApaHistoricalBackfillService(HistoricalBackfillService):
    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        archive_page = int(state.get("archive_page") or 1)
        empty_streak = int(state.get("empty_streak") or 0)
        last_signature = str(state.get("last_signature") or "")
        repeated_signature_count = int(state.get("repeated_signature_count") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            candidates, signature = self._discover_archive_candidates(archive_page)
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = archive_page

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "archive_tail_reached"
                    break
            else:
                empty_streak = 0
                if signature and signature == last_signature:
                    repeated_signature_count += 1
                    if repeated_signature_count >= 1:
                        summary.stopped_reason = "archive_signature_repeated"
                        break
                else:
                    repeated_signature_count = 0
                last_signature = signature
                self._upsert_candidates(candidates, summary)

            archive_page += 1
            self.state_store.save(
                {
                    "archive_page": archive_page,
                    "empty_streak": empty_streak,
                    "last_signature": last_signature,
                    "repeated_signature_count": repeated_signature_count,
                    "batches_scanned": batches_scanned,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"

    def _discover_archive_candidates(self, page_number: int) -> tuple[list[ListingCandidate], str]:
        url = f"{self.client.base_url}/archive" if page_number <= 1 else f"{self.client.base_url}/archive?page={page_number}"
        response = self.client.session.get(
            url,
            timeout=self.settings.request_timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "lxml")

        candidates: list[ListingCandidate] = []
        selectors = (
            "div.four_columns_block a.item.news-item[data-news-id]",
            "div.four_columns_block a.item.news-item",
            "a.item.news-item[data-news-id]",
            "a.item.news-item",
        )
        seen_urls: set[str] = set()
        for selector in selectors:
            for anchor in soup.select(selector):
                href = normalize_space(anchor.get("href", ""))
                article_url = normalize_url(make_absolute_url(self.client.base_url, href))
                if not article_url or article_url in seen_urls or not is_valid_apa_article_url(article_url):
                    continue
                seen_urls.add(article_url)
                image_node = anchor.select_one("div.img img")
                title_node = anchor.select_one("div.content h2.title")
                date_node = anchor.select_one("div.content div.date")
                list_date_text, published_at = self.client._extract_card_date_values(date_node)
                source_article_id = self.client._safe_int(anchor.get("data-news-id", ""))
                if source_article_id is None:
                    source_article_id = extract_apa_article_id(article_url)
                candidates.append(
                    ListingCandidate(
                        url=article_url,
                        slug=extract_apa_slug(article_url),
                        source_article_id=source_article_id,
                        title=normalize_space(title_node.get_text(" ", strip=True)) if title_node else "",
                        category=extract_apa_category_slug(article_url),
                        published_at=published_at,
                        list_date_text=list_date_text,
                        list_image_url=self.client._extract_image_url(image_node),
                        discovery_sources={f"archive-page-{page_number}"},
                    )
                )
        signature = "|".join(candidate.url for candidate in candidates[:5])
        return candidates, signature


class YeniAzerbaycanHistoricalBackfillService(HistoricalBackfillService):
    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        listing_page = int(state.get("listing_page") or 1)
        empty_streak = int(state.get("empty_streak") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            url = self.client.latest_url if listing_page <= 1 else f"{self.client.base_url}/SonXeber_{listing_page}_az.html"
            candidates = self.client._discover_from_listing_page(f"son-xeber-page-{listing_page}", url)
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = listing_page

            if any(self._is_corrupt_listing_candidate(candidate) for candidate in candidates):
                summary.stopped_reason = "corrupt_listing_tail_reached"
                break

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "listing_tail_reached"
                    break
            else:
                empty_streak = 0
                self._upsert_candidates(candidates, summary)

            listing_page += 1
            self.state_store.save(
                {
                    "listing_page": listing_page,
                    "empty_streak": empty_streak,
                    "batches_scanned": batches_scanned,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"

    @staticmethod
    def _is_corrupt_listing_candidate(candidate: ListingCandidate) -> bool:
        value = normalize_space(candidate.list_date_text)
        return value.startswith("30.11.-0001")



class ListingPageHistoricalBackfillService(HistoricalBackfillService):
    stop_on_signature_repeat = True
    signature_repeat_limit = 1
    terminal_error_reason = "listing_tail_reached"

    def seed_candidates(self) -> list[ListingCandidate]:
        return []

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        raise NotImplementedError

    def page_signature(self, candidates: list[ListingCandidate]) -> str:
        return "|".join(candidate.url for candidate in candidates[:5])

    def is_terminal_listing_error(self, exc: Exception, page_number: int) -> bool:
        response = getattr(exc, "response", None)
        return getattr(response, "status_code", None) == 404

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "seed")
        listing_page = int(state.get("listing_page") or 1)
        empty_streak = int(state.get("empty_streak") or 0)
        last_signature = str(state.get("last_signature") or "")
        repeated_signature_count = int(state.get("repeated_signature_count") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "seed":
                try:
                    seed_items = self.seed_candidates()
                except Exception as exc:
                    summary.errors.append(f"seed: {exc}")
                    seed_items = []
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = 0
                self._upsert_candidates(seed_items, summary)
                phase = "listing"
                self.state_store.save(
                    {
                        "phase": phase,
                        "listing_page": listing_page,
                        "empty_streak": empty_streak,
                        "last_signature": last_signature,
                        "repeated_signature_count": repeated_signature_count,
                        "batches_scanned": batches_scanned,
                    }
                )
                continue

            try:
                candidates = self.fetch_listing_page(listing_page)
            except Exception as exc:
                if self.is_terminal_listing_error(exc, listing_page):
                    summary.stopped_reason = self.terminal_error_reason
                    break
                summary.errors.append(f"listing[{listing_page}]: {exc}")
                candidates = []

            signature = self.page_signature(candidates)
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = listing_page

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "listing_tail_reached"
                    break
            else:
                empty_streak = 0
                if self.stop_on_signature_repeat and signature and signature == last_signature:
                    repeated_signature_count += 1
                    if repeated_signature_count >= self.signature_repeat_limit:
                        summary.stopped_reason = "archive_signature_repeated"
                        break
                else:
                    repeated_signature_count = 0
                last_signature = signature
                self._upsert_candidates(candidates, summary)

            listing_page += 1
            self.state_store.save(
                {
                    "phase": "listing",
                    "listing_page": listing_page,
                    "empty_streak": empty_streak,
                    "last_signature": last_signature,
                    "repeated_signature_count": repeated_signature_count,
                    "batches_scanned": batches_scanned,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class TeleqrafHistoricalBackfillService(ListingPageHistoricalBackfillService):
    stop_on_signature_repeat = False

    def seed_candidates(self) -> list[ListingCandidate]:
        return self.client._discover_from_latest_sitemap(1)

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = f"{self.client.base_url}/latest/" if page_number <= 1 else f"{self.client.base_url}/latest/page{page_number}/"
        return self.client._fetch_listing_candidates(f"latest-page-{page_number}", url)


class AzxeberHistoricalBackfillService(ListingPageHistoricalBackfillService):
    def seed_candidates(self) -> list[ListingCandidate]:
        items = list(self.client._discover_from_sitemap(1))
        items.extend(self.client._discover_from_homepage_latest())
        return items

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.news_listing_url if page_number <= 1 else f"{self.client.news_listing_url}?page={page_number}"
        return self.client._fetch_listing_candidates(f"xeberler-page-{page_number}", url)


class SiyasetinfoHistoricalBackfillService(ListingPageHistoricalBackfillService):
    stop_on_signature_repeat = False
    terminal_error_reason = "listing_404_tail_reached"

    def seed_candidates(self) -> list[ListingCandidate]:
        return self.client._discover_from_feed(1)

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.base_url if page_number <= 1 else f"{self.client.base_url}/page/{page_number}/"
        return self.client._discover_from_listing_page(f"page-{page_number}", url)


class MetbuatHistoricalBackfillService(ListingPageHistoricalBackfillService):
    def seed_candidates(self) -> list[ListingCandidate]:
        return self.client._discover_from_rss()

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = f"{self.client.base_url}/olke-metbuati.html" if page_number <= 1 else f"{self.client.base_url}/olke-metbuati.html?page={page_number}&per-page=30"
        return self.client._fetch_listing_candidates(f"latest-page-{page_number}", url)


class OneNewsHistoricalBackfillService(ListingPageHistoricalBackfillService):
    def seed_candidates(self) -> list[ListingCandidate]:
        return self.client._discover_from_news_sitemap(1)

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.latest_url if page_number <= 1 else f"{self.client.latest_url}?page={page_number}"
        return self.client._discover_from_latest_page(f"lenta-page-{page_number}", url)


class SiaHistoricalBackfillService(ListingPageHistoricalBackfillService):
    stop_on_signature_repeat = False

    def seed_candidates(self) -> list[ListingCandidate]:
        items = list(self.client._discover_from_latest_sitemap(1))
        items.extend(self.client._discover_from_feed(1))
        return items

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.latest_url if page_number <= 1 else f"{self.client.latest_url}page{page_number}/"
        return self.client._discover_from_latest_page(f"latest-page-{page_number}", url)


class XeberlerHistoricalBackfillService(ListingPageHistoricalBackfillService):
    stop_on_signature_repeat = False

    def seed_candidates(self) -> list[ListingCandidate]:
        items = list(self.client._discover_from_rss(1))
        items.extend(self.client._discover_from_homepage_latest(1))
        return items

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.content_url if page_number <= 1 else f"{self.client.content_url}all/{page_number}"
        return self.client._discover_from_content_page(f"content-page-{page_number}", url)


class IslamAzeriHistoricalBackfillService(ListingPageHistoricalBackfillService):
    def seed_candidates(self) -> list[ListingCandidate]:
        items = list(self.client._discover_from_homepage_breaking(1))
        items.extend(self.client._discover_from_homepage_featured(1))
        for label, category_url, category_label in self.client.category_pages:
            try:
                items.extend(self.client._discover_from_category_page(label, category_url, category_label))
            except Exception:
                continue
        return items

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        url = self.client.latest_url if page_number <= 1 else f"{self.client.latest_url}?pages={page_number}"
        return self.client._discover_from_latest_page(f"latest-page-{page_number}", url)


class IslamHistoricalBackfillService(ListingPageHistoricalBackfillService):
    stop_on_signature_repeat = False

    def seed_candidates(self) -> list[ListingCandidate]:
        return self.client._discover_from_feed(1)

    def fetch_listing_page(self, page_number: int) -> list[ListingCandidate]:
        if page_number <= 1:
            soup, _ = self.client._get_soup(self.client.category_url)
            widget = self.client._find_news_widget(soup)
            if widget is None:
                return []
            return self.client._parse_widget_candidates(widget, "category-page-1")

        soup, _ = self.client._get_soup(self.client.category_url)
        widget = self.client._find_news_widget(soup)
        if widget is None:
            return []
        query = normalize_space(widget.get("data-query", ""))
        style = normalize_space(widget.get("data-style", ""))
        if not query or not style:
            return []
        payload = self.client._load_widget_page(query, style, page_number)
        html = payload.get("code", "")
        if not html:
            return []
        return self.client._parse_widget_candidates_html(html, f"category-page-{page_number}")

    def is_terminal_listing_error(self, exc: Exception, page_number: int) -> bool:
        return False

class BackwardProbeHistoricalBackfillService(HistoricalBackfillService):
    batch_size = 100

    def build_probe_candidate(self, article_id: int) -> ListingCandidate:
        raise NotImplementedError

    def default_start_id(self) -> int:
        return 500000

    def discover_start_id(self) -> int:
        min_id = self.database.get_min_source_article_id(self.client.source_name)
        if min_id is not None:
            return max(int(min_id), 1)
        max_id = self.database.get_max_source_article_id(self.client.source_name)
        if max_id is not None:
            return max(int(max_id), 1)
        try:
            candidates, _ = self.client.discover_listing_candidates(1)
        except Exception:
            return self.default_start_id()
        ids = [candidate.source_article_id for candidate in candidates.values() if candidate.source_article_id]
        return max(ids) if ids else self.default_start_id()

    def should_ignore_probe_error(self, exc: Exception, candidate: ListingCandidate) -> bool:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in {404, 410}:
            return True
        message = normalize_space(str(exc)).lower()
        ignored_fragments = (
            "invalid canonical url",
            "missing content container",
            "missing milli article id",
            "missing axar article id",
            "missing title",
        )
        return any(fragment in message for fragment in ignored_fragments)

    @staticmethod
    def should_accept_probe_record(record: ArticleRecord, candidate: ListingCandidate) -> bool:
        if candidate.source_article_id is None:
            return True
        return record.source_article_id == candidate.source_article_id

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        current_id = int(state.get("current_id") or self.discover_start_id())
        batches_scanned = int(state.get("batches_scanned") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break
            if current_id <= 0:
                summary.stopped_reason = "reached_id_zero"
                break

            batch_start = current_id
            batch_end = max(current_id - self.batch_size + 1, 1)
            records: list[ArticleRecord] = []
            for article_id in range(batch_start, batch_end - 1, -1):
                candidate = self.build_probe_candidate(article_id)
                try:
                    record = self.client.fetch_article(candidate)
                except Exception as exc:
                    if self.should_ignore_probe_error(exc, candidate):
                        continue
                    summary.errors.append(f"probe[{article_id}]: {exc}")
                    continue
                if not self.should_accept_probe_record(record, candidate):
                    continue
                records.append(record)

            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = batch_start
            summary.listing_candidates += len(records)
            self._upsert_records(records, summary)

            current_id = batch_end - 1
            self.state_store.save(
                {
                    "batches_scanned": batches_scanned,
                    "current_id": current_id,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"


class AxarHistoricalBackfillService(BackwardProbeHistoricalBackfillService):
    def build_probe_candidate(self, article_id: int) -> ListingCandidate:
        return ListingCandidate(
            url=f"{self.client.base_url}/news/siyaset/{article_id}.html",
            slug="",
            source_article_id=article_id,
            discovery_sources={"historical-probe"},
        )

    def default_start_id(self) -> int:
        return 1600000


class MilliHistoricalBackfillService(BackwardProbeHistoricalBackfillService):
    def build_probe_candidate(self, article_id: int) -> ListingCandidate:
        return ListingCandidate(
            url=f"{self.client.base_url}/society/{article_id}.html",
            slug="",
            source_article_id=article_id,
            discovery_sources={"historical-probe"},
        )

    def default_start_id(self) -> int:
        return 1800000


class ReportHistoricalBackfillService(HistoricalBackfillService):
    chunk_size = 250
    sitemap_ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        shard_index = int(state.get("shard_index") or 0)
        entry_offset = int(state.get("entry_offset") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)
        archive_sitemaps = self._discover_archive_sitemaps()

        while shard_index < len(archive_sitemaps):
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            shard_url = archive_sitemaps[shard_index]
            try:
                candidates = self._discover_archive_candidates(shard_url)
            except Exception as exc:
                summary.errors.append(f"archive[{shard_index + 1}]: {exc}")
                summary.stopped_reason = "archive_error"
                break

            if entry_offset >= len(candidates):
                shard_index += 1
                entry_offset = 0
                self.state_store.save(
                    {
                        "batches_scanned": batches_scanned,
                        "entry_offset": entry_offset,
                        "shard_index": shard_index,
                    }
                )
                continue

            chunk = candidates[entry_offset : entry_offset + self.chunk_size]
            if not chunk:
                shard_index += 1
                entry_offset = 0
                self.state_store.save(
                    {
                        "batches_scanned": batches_scanned,
                        "entry_offset": entry_offset,
                        "shard_index": shard_index,
                    }
                )
                continue

            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = shard_index + 1
            self._upsert_candidates(chunk, summary)

            entry_offset += len(chunk)
            if entry_offset >= len(candidates):
                shard_index += 1
                entry_offset = 0

            self.state_store.save(
                {
                    "batches_scanned": batches_scanned,
                    "entry_offset": entry_offset,
                    "shard_index": shard_index,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "archive_tail_reached"

    def _discover_archive_sitemaps(self) -> list[str]:
        response = requests.get(
            f"{self.client.base_url}/sitemap-posts-archive.xml",
            timeout=self.settings.request_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        sitemaps: list[str] = []
        for sitemap_node in root.findall("sm:sitemap", self.sitemap_ns):
            loc_node = sitemap_node.find("sm:loc", self.sitemap_ns)
            if loc_node is None or not loc_node.text:
                continue
            sitemaps.append(loc_node.text.strip())
        return sitemaps

    def _discover_archive_candidates(self, shard_url: str) -> list[ListingCandidate]:
        response = requests.get(
            shard_url,
            timeout=self.settings.request_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        candidates: list[ListingCandidate] = []
        label = shard_url.rsplit("/", 1)[-1]
        for url_node in root.findall("sm:url", self.sitemap_ns):
            loc_node = url_node.find("sm:loc", self.sitemap_ns)
            if loc_node is None or not loc_node.text:
                continue
            article_url = normalize_url(loc_node.text.strip())
            if not self.client._is_azerbaijani_article_url(article_url):
                continue
            lastmod_node = url_node.find("sm:lastmod", self.sitemap_ns)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=self.client._extract_slug(article_url),
                    published_at=lastmod_node.text.strip()
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={label},
                )
            )
        return candidates


class IqtisadiyyatHistoricalBackfillService(HistoricalBackfillService):
    chunk_size = 250
    sitemap_ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        shard_index = int(state.get("shard_index") or 0)
        entry_offset = int(state.get("entry_offset") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)
        shard_urls = self._discover_post_sitemaps()

        while shard_index < len(shard_urls):
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            shard_url = shard_urls[shard_index]
            try:
                candidates = self._discover_shard_candidates(shard_url)
            except Exception as exc:
                summary.errors.append(f"sitemap[{shard_index + 1}]: {exc}")
                summary.stopped_reason = "archive_error"
                break

            if entry_offset >= len(candidates):
                shard_index += 1
                entry_offset = 0
                self.state_store.save(
                    {
                        "batches_scanned": batches_scanned,
                        "entry_offset": entry_offset,
                        "shard_index": shard_index,
                    }
                )
                continue

            chunk = candidates[entry_offset : entry_offset + self.chunk_size]
            if not chunk:
                shard_index += 1
                entry_offset = 0
                self.state_store.save(
                    {
                        "batches_scanned": batches_scanned,
                        "entry_offset": entry_offset,
                        "shard_index": shard_index,
                    }
                )
                continue

            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = shard_index + 1
            self._upsert_candidates(chunk, summary)

            entry_offset += len(chunk)
            if entry_offset >= len(candidates):
                shard_index += 1
                entry_offset = 0

            self.state_store.save(
                {
                    "batches_scanned": batches_scanned,
                    "entry_offset": entry_offset,
                    "shard_index": shard_index,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "archive_tail_reached"

    def _discover_post_sitemaps(self) -> list[str]:
        response = requests.get(
            self.client.sitemap_index_url,
            timeout=self.settings.request_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        shard_urls: list[str] = []
        for sitemap_node in root.findall("sm:sitemap", self.sitemap_ns):
            loc_node = sitemap_node.find("sm:loc", self.sitemap_ns)
            if loc_node is None or not loc_node.text:
                continue
            sitemap_url = loc_node.text.strip()
            if re.search(r"/sitemap-posts-\d+\.xml$", sitemap_url):
                shard_urls.append(sitemap_url)
        return shard_urls

    def _discover_shard_candidates(self, shard_url: str) -> list[ListingCandidate]:
        response = requests.get(
            shard_url,
            timeout=self.settings.request_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        candidates: list[ListingCandidate] = []
        label = shard_url.rsplit("/", 1)[-1]
        for url_node in root.findall("sm:url", self.sitemap_ns):
            loc_node = url_node.find("sm:loc", self.sitemap_ns)
            if loc_node is None or not loc_node.text:
                continue
            article_url = normalize_url(loc_node.text.strip())
            if not self.client._is_article_url(article_url):
                continue
            lastmod_node = url_node.find("sm:lastmod", self.sitemap_ns)
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=self.client._extract_slug(article_url),
                    source_article_id=self.client._extract_article_id(article_url),
                    published_at=lastmod_node.text.strip()
                    if lastmod_node is not None and lastmod_node.text
                    else "",
                    discovery_sources={label},
                )
            )
        return candidates


class OxuHistoricalBackfillService(HistoricalBackfillService):
    seed_chunk_size = 250

    def _run(self, summary: BackfillSummary, *, max_pages: int, stop_after_empty_pages: int) -> None:
        state = self.state_store.load()
        phase = str(state.get("phase") or "seed")
        batch_number = int(state.get("batch_number") or 1)
        cursor = str(state.get("cursor") or "")
        empty_streak = int(state.get("empty_streak") or 0)
        last_signature = str(state.get("last_signature") or "")
        repeated_signature_count = int(state.get("repeated_signature_count") or 0)
        batches_scanned = int(state.get("batches_scanned") or 0)
        seed_offset = int(state.get("seed_offset") or 0)

        while True:
            if max_pages > 0 and batches_scanned >= max_pages:
                summary.stopped_reason = "max_batches_reached"
                break

            if phase == "seed":
                seed_items, cursor = self._discover_seed_candidates()
                if seed_offset >= len(seed_items):
                    phase = "batch"
                    batch_number = 2
                    seed_offset = 0
                    self.state_store.save(
                        {
                            "batch_number": batch_number,
                            "batches_scanned": batches_scanned,
                            "cursor": cursor,
                            "empty_streak": empty_streak,
                            "last_signature": last_signature,
                            "phase": phase,
                            "repeated_signature_count": repeated_signature_count,
                            "seed_offset": seed_offset,
                        }
                    )
                    if not cursor:
                        summary.stopped_reason = "archive_tail_reached"
                        break
                    continue

                chunk = seed_items[seed_offset : seed_offset + self.seed_chunk_size]
                batches_scanned += 1
                summary.pages_scanned = batches_scanned
                summary.last_page_scanned = 1
                self._upsert_candidates(chunk, summary)
                seed_offset += len(chunk)
                if seed_offset >= len(seed_items):
                    phase = "batch"
                    batch_number = 2
                    seed_offset = 0
                self.state_store.save(
                    {
                        "batch_number": batch_number,
                        "batches_scanned": batches_scanned,
                        "cursor": cursor,
                        "empty_streak": empty_streak,
                        "last_signature": last_signature,
                        "phase": phase,
                        "repeated_signature_count": repeated_signature_count,
                        "seed_offset": seed_offset,
                    }
                )
                if phase == "batch" and not cursor:
                    summary.stopped_reason = "archive_tail_reached"
                    break
                continue

            if not cursor:
                summary.stopped_reason = "archive_tail_reached"
                break

            try:
                candidates, next_cursor = self._discover_batch_candidates(cursor, batch_number)
            except Exception as exc:
                summary.errors.append(f"batch[{batch_number}]: {exc}")
                summary.stopped_reason = "batch_error"
                break

            signature = "|".join(candidate.url for candidate in candidates[:5])
            batches_scanned += 1
            summary.pages_scanned = batches_scanned
            summary.last_page_scanned = batch_number

            if not candidates:
                empty_streak += 1
                if empty_streak >= stop_after_empty_pages:
                    summary.stopped_reason = "archive_tail_reached"
                    break
            else:
                empty_streak = 0
                if signature and signature == last_signature:
                    repeated_signature_count += 1
                    if repeated_signature_count >= 1:
                        summary.stopped_reason = "archive_signature_repeated"
                        break
                else:
                    repeated_signature_count = 0
                last_signature = signature
                self._upsert_candidates(candidates, summary)

            if not next_cursor or next_cursor == cursor:
                summary.stopped_reason = "archive_tail_reached"
                cursor = next_cursor or cursor
                self.state_store.save(
                    {
                        "batch_number": batch_number,
                        "batches_scanned": batches_scanned,
                        "cursor": cursor,
                        "empty_streak": empty_streak,
                        "last_signature": last_signature,
                        "phase": phase,
                        "repeated_signature_count": repeated_signature_count,
                        "seed_offset": seed_offset,
                    }
                )
                break

            cursor = next_cursor
            batch_number += 1
            self.state_store.save(
                {
                    "batch_number": batch_number,
                    "batches_scanned": batches_scanned,
                    "cursor": cursor,
                    "empty_streak": empty_streak,
                    "last_signature": last_signature,
                    "phase": phase,
                    "repeated_signature_count": repeated_signature_count,
                    "seed_offset": seed_offset,
                }
            )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"

    def _discover_seed_candidates(self) -> tuple[list[ListingCandidate], str]:
        candidates_by_url: dict[str, ListingCandidate] = {}
        for candidate in self._discover_sitemap_post_candidates():
            key = normalize_url(candidate.url)
            if key in candidates_by_url:
                candidates_by_url[key].merge(candidate)
            else:
                candidates_by_url[key] = candidate

        soup, _ = self.client._get_soup(self.client.base_url)
        blocks = soup.select(".index-post-block")
        homepage_candidates: list[ListingCandidate] = []
        cursor = self.client._extend_from_blocks(homepage_candidates, blocks, "homepage-batch-1")
        for candidate in homepage_candidates:
            key = normalize_url(candidate.url)
            if key in candidates_by_url:
                candidates_by_url[key].merge(candidate)
            else:
                candidates_by_url[key] = candidate
        return list(candidates_by_url.values()), cursor

    def _discover_batch_candidates(self, cursor: str, batch_number: int) -> tuple[list[ListingCandidate], str]:
        soup, _ = self.client._get_soup(self.client.base_url)
        load_container = soup.select_one(".loadContainer")
        if load_container is None:
            raise ValueError("Missing loadContainer on Oxu homepage")
        data_url = normalize_space(load_container.get("data-url", ""))
        if not data_url:
            raise ValueError("Missing Oxu homepage infinity endpoint")
        response = self.client._request(
            f"{self.client.base_url}{data_url}",
            params={"date": cursor, "oldest": "1"},
            timeout=self.settings.request_timeout_seconds,
        )
        batch_soup = BeautifulSoup(response.content, "lxml")
        blocks = batch_soup.select(".index-post-block")
        candidates: list[ListingCandidate] = []
        next_cursor = self.client._extend_from_blocks(candidates, blocks, f"homepage-batch-{batch_number}")
        return candidates, next_cursor

    def _discover_sitemap_post_candidates(self) -> list[ListingCandidate]:
        response = self.client._request(
            f"{self.client.base_url}/sitemap-posts.xml",
            timeout=self.settings.request_timeout_seconds,
        )
        text = response.text if hasattr(response, "text") else response.content.decode("utf-8", "ignore")
        locs = re.findall(r"<loc>(.*?)</loc>", text)
        lastmods = re.findall(r"<lastmod>(.*?)</lastmod>", text)
        candidates: list[ListingCandidate] = []
        for index, raw_url in enumerate(locs):
            article_url = normalize_url(raw_url.strip())
            if not self.client._is_azerbaijani_article_url(article_url):
                continue
            published_at = lastmods[index].strip() if index < len(lastmods) else ""
            candidates.append(
                ListingCandidate(
                    url=article_url,
                    slug=self.client._extract_slug(article_url),
                    published_at=published_at,
                    discovery_sources={"sitemap-posts"},
                )
            )
        return candidates


def supported_historical_sources() -> tuple[str, ...]:
    return (
        AzertagClient.source_name,
        SonxeberClient.source_name,
        AzerbaijanAzClient.source_name,
        IkiSahilClient.source_name,
        YenixeberClient.source_name,
        ApaClient.source_name,
        TeleqrafClient.source_name,
        AzxeberClient.source_name,
        SiyasetinfoClient.source_name,
        MetbuatClient.source_name,
        OneNewsClient.source_name,
        SiaClient.source_name,
        XeberlerClient.source_name,
        IslamAzeriClient.source_name,
        IslamClient.source_name,
        AxarClient.source_name,
        MilliClient.source_name,
        ReportClient.source_name,
        IqtisadiyyatClient.source_name,
        OxuClient.source_name,
    )


def build_backfill_service(settings: Settings, database: Database, source_name: str):
    if source_name == AzertagClient.source_name:
        return AzertagHistoricalBackfillService(settings, database, AzertagClient(settings))
    if source_name == SonxeberClient.source_name:
        return SonxeberHistoricalBackfillService(settings, database, SonxeberClient(settings))
    if source_name == AzerbaijanAzClient.source_name:
        return AzerbaijanAzHistoricalBackfillService(settings, database, AzerbaijanAzClient(settings))
    if source_name == IkiSahilClient.source_name:
        return IkiSahilHistoricalBackfillService(settings, database, IkiSahilClient(settings))
    if source_name == YenixeberClient.source_name:
        return YenixeberHistoricalBackfillService(settings, database, YenixeberClient(settings))
    if source_name == ApaClient.source_name:
        return ApaHistoricalBackfillService(settings, database, ApaClient(settings))
    if source_name == TeleqrafClient.source_name:
        return TeleqrafHistoricalBackfillService(settings, database, TeleqrafClient(settings))
    if source_name == AzxeberClient.source_name:
        return AzxeberHistoricalBackfillService(settings, database, AzxeberClient(settings))
    if source_name == SiyasetinfoClient.source_name:
        return SiyasetinfoHistoricalBackfillService(settings, database, SiyasetinfoClient(settings))
    if source_name == MetbuatClient.source_name:
        return MetbuatHistoricalBackfillService(settings, database, MetbuatClient(settings))
    if source_name == OneNewsClient.source_name:
        return OneNewsHistoricalBackfillService(settings, database, OneNewsClient(settings))
    if source_name == SiaClient.source_name:
        return SiaHistoricalBackfillService(settings, database, SiaClient(settings))
    if source_name == XeberlerClient.source_name:
        return XeberlerHistoricalBackfillService(settings, database, XeberlerClient(settings))
    if source_name == IslamAzeriClient.source_name:
        return IslamAzeriHistoricalBackfillService(settings, database, IslamAzeriClient(settings))
    if source_name == IslamClient.source_name:
        return IslamHistoricalBackfillService(settings, database, IslamClient(settings))
    if source_name == AxarClient.source_name:
        return AxarHistoricalBackfillService(settings, database, AxarClient(settings))
    if source_name == MilliClient.source_name:
        return MilliHistoricalBackfillService(settings, database, MilliClient(settings))
    if source_name == ReportClient.source_name:
        return ReportHistoricalBackfillService(settings, database, ReportClient(settings))
    if source_name == IqtisadiyyatClient.source_name:
        return IqtisadiyyatHistoricalBackfillService(settings, database, IqtisadiyyatClient(settings))
    if source_name == OxuClient.source_name:
        return OxuHistoricalBackfillService(settings, database, OxuClient(settings))
    supported = ", ".join(supported_historical_sources())
    raise SystemExit(f"historical_backfill_not_supported_for_source={source_name}; supported={supported}")
