from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import BigInteger, cast, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from news_ingestor.db.models import NewsRecord, Source, uuid_str
from news_ingestor.schemas import AuditPayload, NormalizedNewsPayload, RawIngestPayload
from news_ingestor.utils.time import utc_now


class Repository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def ensure_source(
        self,
        *,
        source_key: str,
        source_type: str,
        identifier: str,
        display_name: str,
    ) -> Source:
        source = await self.get_source_by_key(source_key)
        if source is None:
            now = utc_now()
            source = Source(
                id=uuid_str(),
                record_kind="source",
                source_key=source_key,
                source_item_id="",
                desired_state="running",
                created_at=now,
                updated_at=now,
            )
            self.session.add(source)
            await self.session.flush()
            return source

        return source

    async def get_source_by_key(self, source_key: str) -> Source | None:
        result = await self.session.execute(
            select(Source).where(Source.record_kind == "source", Source.source_key == source_key)
        )
        return result.scalar_one_or_none()

    async def get_source_by_id(self, source_id: str) -> Source | None:
        result = await self.session.execute(
            select(Source).where(Source.record_kind == "source", Source.id == source_id)
        )
        return result.scalar_one_or_none()

    async def list_sources(self, *, source_type: str = "", desired_state: str = "") -> Sequence[Source]:
        query = select(Source).where(Source.record_kind == "source").order_by(
            func.coalesce(Source.created_at, Source.updated_at).asc(),
            Source.source_key.asc(),
        )
        if desired_state:
            query = query.where(Source.desired_state == desired_state)
        result = await self.session.execute(query)
        rows = result.scalars().all()
        if source_type:
            rows = [row for row in rows if row.type == source_type]
        return rows

    async def count_sources(self, *, source_type: str = "") -> int:
        rows = await self.list_sources(source_type=source_type)
        return len(rows)

    async def set_source_state(self, source_key: str, desired_state: str) -> Source | None:
        source = await self.get_source_by_key(source_key)
        if source is None:
            return None
        source.desired_state = desired_state
        return source

    async def update_source_runtime(self, source_id: str, *, runtime_status: str, last_error: str = "") -> None:
        _ = (source_id, runtime_status, last_error)

    async def heartbeat(self, source_id: str) -> None:
        _ = source_id

    async def update_checkpoint(
        self,
        source_id: str,
        *,
        last_message_id: int = 0,
    ) -> Source | None:
        source = await self.get_source_by_id(source_id)
        if source is None:
            return None
        source.last_message_id = max(source.last_message_id, last_message_id)
        return source

    async def upsert_news_raw(self, source: Source, payload: RawIngestPayload) -> NewsRecord:
        values = {
            "id": uuid_str(),
            "record_kind": "news",
            "source_key": source.source_key,
            "source_item_id": payload.source_item_id,
            "fetched_at": payload.fetched_at,
            "published_at": payload.observed_at,
            "origin_url": payload.origin_url,
            "body_text": "",
            "desired_state": "running",
            "last_message_id": 0,
            "created_at": payload.fetched_at,
            "updated_at": payload.fetched_at,
        }
        statement = (
            pg_insert(NewsRecord)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["source_key", "source_item_id"],
                index_where=text("record_kind = 'news'"),
                set_={
                    "fetched_at": values["fetched_at"],
                    "published_at": values["published_at"],
                    "origin_url": values["origin_url"],
                    "created_at": func.coalesce(NewsRecord.created_at, values["created_at"]),
                    "updated_at": values["updated_at"],
                },
            )
            .returning(NewsRecord.id)
        )
        record_id = (await self.session.execute(statement)).scalar_one()
        result = await self.session.execute(
            select(NewsRecord).where(NewsRecord.record_kind == "news", NewsRecord.id == record_id)
        )
        return result.scalar_one()

    async def apply_normalized_news(
        self,
        news_id: str,
        *,
        source: Source,
        payload: NormalizedNewsPayload,
    ) -> NewsRecord | None:
        statement = (
            update(NewsRecord)
            .where(NewsRecord.record_kind == "news", NewsRecord.id == news_id)
            .values(
                source_item_id=payload.source_item_id,
                body_text=payload.body_text,
                published_at=payload.published_at,
                updated_at=payload.ingested_at,
            )
            .returning(NewsRecord.id)
        )
        record_id = (await self.session.execute(statement)).scalar_one_or_none()
        if record_id is None:
            return None
        result = await self.session.execute(
            select(NewsRecord).where(NewsRecord.record_kind == "news", NewsRecord.id == record_id)
        )
        return result.scalar_one_or_none()

    async def mark_parse_failure(self, news_id: str, *, parser_error: str) -> None:
        _ = (news_id, parser_error)

    async def latest_source_item_ids(self, source_id: str, *, limit: int = 10, numeric_order: bool = False) -> list[str]:
        source = await self.get_source_by_id(source_id)
        if source is None:
            return []
        query = select(NewsRecord.source_item_id).where(
            NewsRecord.record_kind == "news",
            NewsRecord.source_key == source.source_key,
        )
        if numeric_order:
            query = query.order_by(cast(NewsRecord.source_item_id, BigInteger).desc())
        else:
            query = query.order_by(
                NewsRecord.published_at.desc(),
                func.coalesce(
                    NewsRecord.created_at,
                    NewsRecord.updated_at,
                    NewsRecord.fetched_at,
                ).desc(),
            )
        result = await self.session.execute(query.limit(limit))
        return [item for item in result.scalars().all()]

    async def has_news_for_source(self, source_id: str) -> bool:
        source = await self.get_source_by_id(source_id)
        if source is None:
            return False
        result = await self.session.scalar(
            select(func.count()).select_from(NewsRecord).where(
                NewsRecord.record_kind == "news",
                NewsRecord.source_key == source.source_key,
            )
        )
        return bool(result)

    async def min_source_item_id(self, source_id: str) -> int:
        """Return the smallest numeric source_item_id for this source, or 0."""
        source = await self.get_source_by_id(source_id)
        if source is None:
            return 0
        result = await self.session.scalar(
            select(func.min(cast(NewsRecord.source_item_id, BigInteger))).where(
                NewsRecord.record_kind == "news",
                NewsRecord.source_key == source.source_key,
            )
        )
        return int(result) if result is not None else 0

    async def existing_source_item_ids(self, source_id: str, item_ids: Sequence[str]) -> list[str]:
        source = await self.get_source_by_id(source_id)
        if source is None or not item_ids:
            return []
        result = await self.session.execute(
            select(NewsRecord.source_item_id).where(
                NewsRecord.record_kind == "news",
                NewsRecord.source_key == source.source_key,
                NewsRecord.source_item_id.in_(list(item_ids)),
            )
        )
        return [item for item in result.scalars().all()]

    async def add_audit(self, source: Source, payload: AuditPayload) -> Source:
        return source

    async def status_summary(self) -> dict[str, Any]:
        total_sources = await self.session.scalar(
            select(func.count()).select_from(Source).where(Source.record_kind == "source")
        )
        running_sources = await self.session.scalar(
            select(func.count()).select_from(Source).where(
                Source.record_kind == "source",
                Source.desired_state == "running",
            )
        )
        paused_sources = await self.session.scalar(
            select(func.count()).select_from(Source).where(
                Source.record_kind == "source",
                Source.desired_state == "paused",
            )
        )
        news_count = await self.session.scalar(
            select(func.count()).select_from(NewsRecord).where(NewsRecord.record_kind == "news")
        )
        return {
            "total_sources": total_sources or 0,
            "running_sources": running_sources or 0,
            "paused_sources": paused_sources or 0,
            "news_count": news_count or 0,
        }
