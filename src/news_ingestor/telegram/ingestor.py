from __future__ import annotations

import asyncio
import logging
import re
import time

from telethon import events
from telethon.utils import get_peer_id

from news_ingestor.db.models import Source
from news_ingestor.db.repository import Repository
from news_ingestor.db.schema import initialize_database
from news_ingestor.db.session import session_scope
from news_ingestor.schemas import RawIngestPayload
from news_ingestor.services.audit import build_audit_payload
from news_ingestor.services.checkpoints import CheckpointService
from news_ingestor.services.normalizer import NormalizerService
from news_ingestor.services.runtime_state import RuntimeStateStore
from news_ingestor.services.telegram_backfill_state import TelegramBackfillStateStore
from news_ingestor.settings import get_settings
from news_ingestor.telegram.client import build_client
from news_ingestor.telegram.serializer import serialize_message
from news_ingestor.utils.text import sha256_text
from news_ingestor.utils.time import utc_now


logger = logging.getLogger(__name__)

# FIX #4: How long a source record stays valid in the in-process cache
_SOURCE_CACHE_TTL_SECONDS = 60.0


def normalize_telegram_identifier(value: str) -> str:
    identifier = (value or "").strip()
    if identifier.lower().startswith("peer:"):
        peer_value = identifier.split(":", 1)[1].strip()
        return f"peer:{peer_value}"
    identifier = identifier.removeprefix("https://t.me/")
    identifier = identifier.removeprefix("http://t.me/")
    identifier = identifier.removeprefix("t.me/")
    identifier = identifier.removeprefix("@")
    identifier = identifier.strip("/")
    if re.fullmatch(r"-?\d+", identifier):
        return f"peer:{identifier}"
    return identifier


async def resolve_telegram_entity(client, identifier: str):
    normalized = normalize_telegram_identifier(identifier)
    if normalized.startswith("peer:"):
        peer_value = normalized.split(":", 1)[1].strip()
        return await client.get_entity(int(peer_value))
    return await client.get_entity(normalized)


def build_source_key(identifier: str) -> str:
    return f"telegram:{normalize_telegram_identifier(identifier).lower()}"


class TelegramWorker:
    def __init__(self, refresh_seconds: int):
        settings = get_settings()
        self.refresh_seconds = refresh_seconds
        self.backfill_batch_size = max(settings.telegram_backfill_limit, 100)
        self.client = build_client()
        self.normalizer = NormalizerService()
        self.runtime_state = RuntimeStateStore()
        self.backfill_state = TelegramBackfillStateStore()
        self.source_map: dict[int, Source] = {}
        self.source_identifier_map: dict[str, Source] = {}

        # FIX #4: TTL cache  {source_key -> (Source, expires_monotonic)}
        self._source_cache: dict[str, tuple[Source, float]] = {}

    # ── Init / Auth ───────────────────────────────────────────────────────────

    async def init_db(self) -> None:
        await initialize_database()

    async def ensure_authorized(self) -> None:
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("Telegram oturumu acik degil. Once `login-telegram` komutunu calistir.")

    async def refresh_sources(self) -> None:
        async with session_scope() as session:
            repo = Repository(session)
            sources = await repo.list_sources(source_type="telegram_channel", desired_state="running")
            self.source_map = {}
            self.source_identifier_map = {}
            for source in sources:
                try:
                    entity = await resolve_telegram_entity(self.client, source.identifier)
                    self.source_map[get_peer_id(entity)] = source
                    self.source_identifier_map[source.key] = source
                    self.runtime_state.set(source.key, runtime_status="running")
                except Exception as exc:
                    logger.exception("Telegram kaynagi resolve edilemedi: %s", source.key)
                    self.runtime_state.set(source.key, runtime_status="error", last_error=str(exc))

    # ── FIX #4: TTL-cached source lookup ─────────────────────────────────────

    async def _get_source_cached(self, repo: Repository, source_key: str) -> Source | None:
        """
        Return the Source from an in-process TTL cache to avoid a DB round-trip
        on every incoming live message.  The cache entry is refreshed every
        _SOURCE_CACHE_TTL_SECONDS seconds so state changes (pause/resume) still
        propagate within a predictable window.
        """
        cached = self._source_cache.get(source_key)
        if cached is not None and time.monotonic() < cached[1]:
            return cached[0]

        source = await repo.get_source_by_key(source_key)
        if source is not None:
            self._source_cache[source_key] = (source, time.monotonic() + _SOURCE_CACHE_TTL_SECONDS)
        else:
            self._source_cache.pop(source_key, None)
        return source

    def _invalidate_source_cache(self, source_key: str) -> None:
        """Force a fresh DB read on the next access (e.g. after pause/resume)."""
        self._source_cache.pop(source_key, None)

    # ── Backfill ──────────────────────────────────────────────────────────────

    async def full_backfill_source(self, source: Source, *, batch_size: int = 0) -> int:
        total = 0
        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.get_source_by_key(source.key)
            if source is None:
                return 0
            min_id_known = await repo.min_source_item_id(source.id)

        state = self.backfill_state.get(source.key)
        if state.historical_complete:
            return 0

        entity = await resolve_telegram_entity(self.client, source.identifier)
        offset_id = state.next_offset_id or min_id_known or 0
        total = state.total_persisted
        effective_batch_size = batch_size or self.backfill_batch_size

        logger.info(
            "Full backfill basladi: %s (offset_id=%d, min_known=%d, resumed=%s)",
            source.key,
            offset_id,
            min_id_known,
            bool(state.next_offset_id),
        )
        self.runtime_state.set(source.key, runtime_status="backfilling")

        while True:
            batch = []
            kwargs = {"limit": effective_batch_size, "reverse": False}
            if offset_id > 0:
                kwargs["offset_id"] = offset_id

            async for message in self.client.iter_messages(entity, **kwargs):
                batch.append(message)

            if not batch:
                self.backfill_state.mark_complete(
                    source.key,
                    last_oldest_id=offset_id or min_id_known,
                    total_persisted=total,
                )
                break

            # FIX #3: Checkpoint once per batch instead of once per message.
            batch_max_id = 0
            async with session_scope() as session:
                repo = Repository(session)
                source_fresh = await repo.get_source_by_key(source.key)
                if source_fresh is None:
                    break
                for message in reversed(batch):
                    persisted = await self._persist_message(repo, source_fresh, message)
                    if persisted:
                        total += 1
                        msg_id = getattr(message, "id", 0)
                        if msg_id > batch_max_id:
                            batch_max_id = msg_id
                    self.runtime_state.heartbeat(source.key)

                # Single checkpoint write for the entire batch
                if batch_max_id > 0:
                    checkpoint_service = CheckpointService(repo)
                    await checkpoint_service.advance_message_checkpoint(
                        source_fresh, last_message_id=batch_max_id
                    )

            ids = [getattr(message, "id", 0) for message in batch if getattr(message, "id", 0)]
            if not ids:
                self.backfill_state.mark_complete(
                    source.key,
                    last_oldest_id=offset_id or min_id_known,
                    total_persisted=total,
                )
                break

            new_min = min(ids)
            self.backfill_state.set_progress(
                source.key,
                next_offset_id=new_min,
                last_oldest_id=new_min,
                total_persisted=total,
            )
            logger.info(
                "Full backfill batch: %s | messages=%d | oldest_id=%d | total=%d",
                source.key,
                len(batch),
                new_min,
                total,
            )
            print(
                f"telegram_backfill_progress source={source.key}"
                f" batch={len(batch)} oldest_id={new_min} total_persisted={total}",
                flush=True,
            )

            if offset_id > 0 and new_min >= offset_id:
                self.backfill_state.mark_complete(
                    source.key,
                    last_oldest_id=new_min,
                    total_persisted=total,
                )
                break

            offset_id = new_min

        self.runtime_state.set(source.key, runtime_status="running")
        logger.info("Full backfill tamamlandi: %s | total=%d", source.key, total)
        print(f"telegram_backfill_complete source={source.key} total_persisted={total}", flush=True)
        return total

    async def backfill_all_sources_full(self) -> None:
        async with session_scope() as session:
            repo = Repository(session)
            sources = await repo.list_sources(source_type="telegram_channel", desired_state="running")
        for source in sources:
            try:
                await self.full_backfill_source(source)
            except Exception as exc:
                logger.exception("Full backfill hatasi: %s", source.key)
                self.runtime_state.set(source.key, runtime_status="error", last_error=str(exc))

    async def ingest_source_history(self, source: Source, *, limit: int = 0) -> None:
        state = self.backfill_state.get(source.key)
        if not state.historical_complete:
            logger.info("Telegram source historical incomplete; full backfill resume ediliyor: %s", source.key)
            await self.full_backfill_source(source)
            return

        # FIX #3: Collect all persisted message IDs, checkpoint once at the end.
        async with session_scope() as session:
            repo = Repository(session)
            checkpoint_service = CheckpointService(repo)
            source = await repo.get_source_by_key(source.key)
            if source is None:
                return
            checkpoint = await checkpoint_service.get_last_message_id(source)
            entity = await resolve_telegram_entity(self.client, source.identifier)
            backfill_limit = limit or self.backfill_batch_size

            batch_max_id = 0
            if checkpoint <= 0:
                latest_messages = []
                async for message in self.client.iter_messages(entity, limit=backfill_limit):
                    latest_messages.append(message)
                for message in reversed(latest_messages):
                    await self._persist_message(repo, source, message)
                    msg_id = getattr(message, "id", 0)
                    if msg_id > batch_max_id:
                        batch_max_id = msg_id
                    self.runtime_state.heartbeat(source.key)
            else:
                async for message in self.client.iter_messages(entity, min_id=checkpoint, reverse=True, limit=backfill_limit):
                    if getattr(message, "id", 0) <= checkpoint:
                        continue
                    await self._persist_message(repo, source, message)
                    msg_id = getattr(message, "id", 0)
                    if msg_id > batch_max_id:
                        batch_max_id = msg_id
                    self.runtime_state.heartbeat(source.key)

            # Single checkpoint write for the entire catch-up range
            if batch_max_id > 0:
                await checkpoint_service.advance_message_checkpoint(
                    source, last_message_id=batch_max_id
                )

    # ── Core persist (no checkpoint I/O — callers handle it in batch) ─────────

    async def _persist_message(self, repo: Repository, source: Source, message) -> bool:
        """
        Persist a single Telegram message.  Checkpointing is intentionally
        omitted here — callers write a single checkpoint after an entire batch,
        which dramatically reduces DB writes during bulk backfill.
        """
        fetched_at = utc_now()
        serialized = serialize_message(message, source_identifier=normalize_telegram_identifier(source.identifier))
        if serialized.get("message_type") == "MessageService":
            return False
        raw_payload = RawIngestPayload(
            source_item_id=str(serialized["message_id"]),
            source_event_type="telegram_message",
            fetched_at=fetched_at,
            observed_at=serialized["date"] or fetched_at,
            raw_payload={},
            raw_text=serialized["text"],
            origin_url=serialized["permalink"],
            content_hash=sha256_text(serialized["text"]),
            primary_image_url=serialized["image_urls"][0] if serialized["image_urls"] else "",
            image_urls=serialized["image_urls"],
            parse_status="pending",
        )
        news_record = await repo.upsert_news_raw(source, raw_payload)
        try:
            normalized = self.normalizer.normalize_telegram_message(
                source,
                {
                    **serialized,
                    "fetched_at": fetched_at,
                },
            )
            await repo.apply_normalized_news(news_record.id, source=source, payload=normalized)
        except Exception as exc:
            logger.exception("Telegram mesaji parse edilemedi: %s", source.key)
            await repo.mark_parse_failure(news_record.id, parser_error=str(exc))
        return True

    # ── Live handler ──────────────────────────────────────────────────────────

    async def handle_new_message(self, event) -> None:
        message = event.message
        chat_id = get_peer_id(message.peer_id) if getattr(message, "peer_id", None) else getattr(message, "chat_id", 0)
        source = self.source_map.get(chat_id)
        if source is None:
            return

        try:
            async with session_scope() as session:
                repo = Repository(session)

                # FIX #4: Use TTL cache — avoids a DB SELECT on every live message
                source_fresh = await self._get_source_cached(repo, source.key)
                if source_fresh is None or source_fresh.desired_state != "running":
                    return

                persisted = await self._persist_message(repo, source_fresh, message)

                # Live messages are single — checkpoint immediately so a crash
                # right after does not re-ingest the same message.
                if persisted:
                    checkpoint_service = CheckpointService(repo)
                    await checkpoint_service.advance_message_checkpoint(
                        source_fresh, last_message_id=getattr(message, "id", 0)
                    )

                self.runtime_state.set(source.key, runtime_status="running")
        except Exception as exc:
            logger.exception("Canli Telegram mesaji islenemedi: %s", source.key)
            self.runtime_state.set(source.key, runtime_status="error", last_error=str(exc))
            self._invalidate_source_cache(source.key)

    # ── Audit ─────────────────────────────────────────────────────────────────

    async def audit_source(self, source_key: str, *, limit: int = 10) -> dict:
        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.get_source_by_key(source_key)
            if source is None:
                raise RuntimeError(f"Source bulunamadi: {source_key}")
            entity = await resolve_telegram_entity(self.client, source.identifier)
            live_ids: list[str] = []
            scanned = 0
            scan_limit = max(limit * 5, 25)
            async for message in self.client.iter_messages(entity, limit=scan_limit):
                scanned += 1
                if type(message).__name__ == "MessageService":
                    continue
                live_ids.append(str(getattr(message, "id", 0)))
                if len(live_ids) >= limit:
                    break
            db_window_ids = await repo.latest_source_item_ids(source.id, limit=limit, numeric_order=True)
            db_present_ids = await repo.existing_source_item_ids(source.id, live_ids)
            audit = build_audit_payload(
                live_ids=live_ids,
                db_present_ids=db_present_ids,
                db_window_ids=db_window_ids,
                audit_type="telegram_live_vs_db",
            )
            await repo.add_audit(source, audit)
            return {
                "source_key": source.key,
                "status": audit.status,
                "live_latest_item_id": audit.live_latest_item_id,
                "db_latest_item_id": audit.db_latest_item_id,
                "missing_in_db": audit.details["missing_in_db"],
            }

    async def audit_all_sources(self, *, limit: int = 10) -> list[dict]:
        results = []
        async with session_scope() as session:
            repo = Repository(session)
            sources = await repo.list_sources(source_type="telegram_channel")
        for source in sources:
            results.append(await self.audit_source(source.key, limit=limit))
        return results

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        await self.init_db()
        await self.ensure_authorized()
        await self.refresh_sources()
        await self.backfill_all_sources()
        self.client.add_event_handler(self.handle_new_message, events.NewMessage(incoming=True))
        await self.client.catch_up()
        logger.info("Telegram worker basladi (historical backfill -> live handoff)")
        await asyncio.gather(self.refresh_loop(), self.client.run_until_disconnected())

    async def refresh_loop(self) -> None:
        while True:
            await self.refresh_sources()
            await self.backfill_all_sources()
            await asyncio.sleep(self.refresh_seconds)

    async def backfill_all_sources(self) -> None:
        async with session_scope() as session:
            repo = Repository(session)
            sources = await repo.list_sources(source_type="telegram_channel", desired_state="running")
        for source in sources:
            try:
                await self.ingest_source_history(source)
            except Exception as exc:
                logger.exception("Backfill hatasi: %s", source.key)
                self.runtime_state.set(source.key, runtime_status="error", last_error=str(exc))
