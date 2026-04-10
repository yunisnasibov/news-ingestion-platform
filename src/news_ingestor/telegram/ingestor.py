from __future__ import annotations

import asyncio
import logging
import re

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
from news_ingestor.telegram.client import build_client
from news_ingestor.telegram.serializer import serialize_message
from news_ingestor.utils.text import sha256_text
from news_ingestor.utils.time import utc_now


logger = logging.getLogger(__name__)


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
        self.refresh_seconds = refresh_seconds
        self.client = build_client()
        self.normalizer = NormalizerService()
        self.source_map: dict[int, Source] = {}
        self.source_identifier_map: dict[str, Source] = {}

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
                    source.display_name = source.display_name or getattr(entity, "title", "")
                    self.source_map[get_peer_id(entity)] = source
                    self.source_identifier_map[source.key] = source
                    await repo.update_source_runtime(source.id, runtime_status="running")
                except Exception as exc:
                    logger.exception("Telegram kaynagi resolve edilemedi: %s", source.key)
                    await repo.update_source_runtime(source.id, runtime_status="error", last_error=str(exc))

    async def ingest_source_history(self, source: Source, *, limit: int = 0) -> None:
        async with session_scope() as session:
            repo = Repository(session)
            checkpoint_service = CheckpointService(repo)
            source = await repo.get_source_by_key(source.key)
            if source is None:
                return
            checkpoint = await checkpoint_service.get_last_message_id(source)
            entity = await resolve_telegram_entity(self.client, source.identifier)
            backfill_limit = limit or 200
            if checkpoint <= 0:
                latest_messages = []
                async for message in self.client.iter_messages(entity, limit=backfill_limit):
                    latest_messages.append(message)
                for message in reversed(latest_messages):
                    await self._persist_message(repo, checkpoint_service, source, message)
                    await repo.heartbeat(source.id)
                return

            async for message in self.client.iter_messages(entity, min_id=checkpoint, reverse=True, limit=backfill_limit):
                if getattr(message, "id", 0) <= checkpoint:
                    continue
                await self._persist_message(repo, checkpoint_service, source, message)
                await repo.heartbeat(source.id)

    async def _persist_message(self, repo: Repository, checkpoint_service: CheckpointService, source: Source, message) -> None:
        fetched_at = utc_now()
        serialized = serialize_message(message, source_identifier=normalize_telegram_identifier(source.identifier))
        if serialized.get("message_type") == "MessageService":
            # The single-table schema only persists real news rows, so we keep the
            # checkpoint aligned to the last stored message instead of advancing it
            # over Telegram service events that we intentionally skip.
            return
        raw_payload = RawIngestPayload(
            source_item_id=str(serialized["message_id"]),
            source_event_type="telegram_message",
            fetched_at=fetched_at,
            observed_at=serialized["date"] or fetched_at,
            raw_payload=serialized["raw"],
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
                    "observed_at": serialized["date"] or fetched_at,
                },
            )
            await repo.apply_normalized_news(news_record.id, source=source, payload=normalized)
        except Exception as exc:
            logger.exception("Telegram mesaji parse edilemedi: %s", source.key)
            await repo.mark_parse_failure(news_record.id, parser_error=str(exc))
        await checkpoint_service.advance_message_checkpoint(
            source,
            last_message_id=getattr(message, "id", 0),
        )

    async def handle_new_message(self, event) -> None:
        message = event.message
        chat_id = get_peer_id(message.peer_id) if getattr(message, "peer_id", None) else getattr(message, "chat_id", 0)
        source = self.source_map.get(chat_id)
        if source is None:
            return

        try:
            async with session_scope() as session:
                repo = Repository(session)
                checkpoint_service = CheckpointService(repo)
                source = await repo.get_source_by_key(source.key)
                if source is None or source.desired_state != "running":
                    return
                await self._persist_message(repo, checkpoint_service, source, message)
                await repo.update_source_runtime(source.id, runtime_status="running")
        except Exception as exc:
            logger.exception("Canli Telegram mesaji islenemedi: %s", source.key)
            async with session_scope() as session:
                repo = Repository(session)
                source_db = await repo.get_source_by_key(source.key)
                if source_db is not None:
                    await repo.update_source_runtime(source_db.id, runtime_status="error", last_error=str(exc))

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

    async def run_forever(self) -> None:
        await self.init_db()
        await self.ensure_authorized()
        await self.refresh_sources()
        await self.backfill_all_sources()
        self.client.add_event_handler(self.handle_new_message, events.NewMessage(incoming=True))
        await self.client.catch_up()
        logger.info("Telegram worker basladi")
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
                async with session_scope() as session:
                    repo = Repository(session)
                    source_db = await repo.get_source_by_key(source.key)
                    if source_db is not None:
                        await repo.update_source_runtime(source_db.id, runtime_status="error", last_error=str(exc))
