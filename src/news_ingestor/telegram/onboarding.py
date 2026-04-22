from __future__ import annotations

from news_ingestor.db.repository import Repository
from news_ingestor.db.session import session_scope
from news_ingestor.telegram.ingestor import (
    TelegramWorker,
    build_source_key,
    normalize_telegram_identifier,
    resolve_telegram_entity,
)


class TelegramOnboardingService:
    def __init__(self, worker: TelegramWorker):
        self.worker = worker

    async def add_source(self, identifier: str) -> dict:
        clean_identifier = normalize_telegram_identifier(identifier)
        entity = await resolve_telegram_entity(self.worker.client, clean_identifier)
        source_key = build_source_key(clean_identifier)

        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.ensure_source(
                source_key=source_key,
                source_type="telegram_channel",
                identifier=clean_identifier,
                display_name=getattr(entity, "title", clean_identifier),
            )
            self.worker.runtime_state.set(source.key, runtime_status="idle")
            source_count = await repo.count_sources(source_type="telegram_channel")

        await self.worker.refresh_sources()
        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.get_source_by_key(source_key)
        if source is None:
            raise RuntimeError(f"Source olusturulamadi: {source_key}")

        await self.worker.ingest_source_history(source)
        source_audit = await self.worker.audit_source(source_key)
        all_audits = await self.worker.audit_all_sources(limit=5)
        milestone_audits = []
        if source_count > 0 and source_count % 5 == 0:
            milestone_audits = await self.worker.audit_all_sources(limit=10)

        return {
            "source_key": source_key,
            "source_audit": source_audit,
            "all_sources_audit": all_audits,
            "milestone_audits": milestone_audits,
        }
