from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon.utils import get_peer_id

from news_ingestor.db.schema import initialize_database
from news_ingestor.db.repository import Repository
from news_ingestor.db.session import session_scope
from news_ingestor.logging import configure_logging
from news_ingestor.services.runtime_state import RuntimeStateStore


class TelegramSourceCreate(BaseModel):
    identifier: str


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()
    await initialize_database()
    yield


app = FastAPI(title="news-ingestor-control", lifespan=lifespan)
runtime_state = RuntimeStateStore()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    async with session_scope() as session:
        repo = Repository(session)
        return await repo.status_summary()


@app.get("/sources")
async def sources():
    async with session_scope() as session:
        repo = Repository(session)
        rows = await repo.list_sources()
        runtime_state.annotate(rows)
        return [
            {
                "key": source.key,
                "type": source.type,
                "identifier": source.identifier,
                "display_name": source.display_name,
                "desired_state": source.desired_state,
                "runtime_status": source.runtime_status,
                "last_error": source.last_error,
            }
            for source in rows
        ]


@app.post("/sources/telegram")
async def create_telegram_source(payload: TelegramSourceCreate):
    from news_ingestor.telegram.ingestor import TelegramWorker
    from news_ingestor.telegram.onboarding import TelegramOnboardingService
    from news_ingestor.settings import get_settings

    worker = TelegramWorker(refresh_seconds=get_settings().telegram_refresh_seconds)
    await worker.ensure_authorized()
    try:
        service = TelegramOnboardingService(worker)
        return await service.add_source(payload.identifier)
    finally:
        await worker.client.disconnect()


@app.get("/telegram/dialogs/search")
async def search_telegram_dialogs(q: str):
    from news_ingestor.telegram.ingestor import TelegramWorker
    from news_ingestor.settings import get_settings

    query = (q or "").strip().lower()
    worker = TelegramWorker(refresh_seconds=get_settings().telegram_refresh_seconds)
    await worker.ensure_authorized()
    try:
        matches = []
        async for dialog in worker.client.iter_dialogs():
            entity = dialog.entity
            title = getattr(dialog, "name", "") or getattr(entity, "title", "") or ""
            username = getattr(entity, "username", "") or ""
            haystack = f"{title} {username}".lower()
            if query and query not in haystack:
                continue
            peer_id = get_peer_id(entity)
            matches.append(
                {
                    "title": title,
                    "username": username,
                    "peer_id": peer_id,
                    "suggested_identifier": username or f"peer:{peer_id}",
                }
            )
        return matches
    finally:
        await worker.client.disconnect()


@app.post("/sources/{source_key}/pause")
async def pause_source(source_key: str):
    async with session_scope() as session:
        repo = Repository(session)
        source = await repo.set_source_state(source_key, "paused")
        if source is None:
            raise HTTPException(status_code=404, detail="Source not found")
        runtime_state.set(source.key, runtime_status="paused")
        return {"status": "paused", "source_key": source.key}


@app.post("/sources/{source_key}/resume")
async def resume_source(source_key: str):
    async with session_scope() as session:
        repo = Repository(session)
        source = await repo.set_source_state(source_key, "running")
        if source is None:
            raise HTTPException(status_code=404, detail="Source not found")
        runtime_state.set(source.key, runtime_status="idle")
        return {"status": "running", "source_key": source.key}
