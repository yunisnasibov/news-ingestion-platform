from __future__ import annotations

import asyncio
import json

import typer
from sqlalchemy import text
from telethon.errors import SessionPasswordNeededError

from news_ingestor.db.schema import initialize_database
from news_ingestor.db.repository import Repository
from news_ingestor.db.session import session_scope
from news_ingestor.logging import configure_logging
from news_ingestor.services.runtime_state import RuntimeStateStore
from news_ingestor.services.telegram_backfill_state import TelegramBackfillStateStore
from news_ingestor.settings import get_settings
from news_ingestor.telegram.client import build_client
from news_ingestor.telegram.ingestor import TelegramWorker
from news_ingestor.telegram.onboarding import TelegramOnboardingService


app = typer.Typer(no_args_is_help=True)


async def _init_db() -> None:
    await initialize_database()


async def _reset_content() -> dict[str, int]:
    await initialize_database()
    async with session_scope() as session:
        news_rows = int((await session.execute(text("SELECT COUNT(*) FROM news WHERE record_kind = 'news'"))).scalar_one() or 0)
        source_rows = int((await session.execute(text("SELECT COUNT(*) FROM news WHERE record_kind = 'source'"))).scalar_one() or 0)
        articles_table_exists = bool(
            (
                await session.execute(
                    text(
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.tables
                          WHERE table_schema = 'public'
                            AND table_name = 'articles'
                        )
                        """
                    )
                )
            ).scalar_one()
        )
        articles_rows = 0
        if articles_table_exists:
            articles_rows = int((await session.execute(text("SELECT COUNT(*) FROM articles"))).scalar_one() or 0)
            await session.execute(text("TRUNCATE TABLE articles RESTART IDENTITY"))
        await session.execute(text("DELETE FROM news WHERE record_kind = 'news'"))
        await session.execute(text("UPDATE news SET last_message_id = 0 WHERE record_kind = 'source'"))

    RuntimeStateStore().clear_all()
    TelegramBackfillStateStore().clear_all()
    return {
        "cleared_news_rows": news_rows,
        "cleared_article_rows": articles_rows,
        "preserved_source_rows": source_rows,
    }


@app.command("init-db")
def init_db() -> None:
    configure_logging()
    asyncio.run(_init_db())
    typer.echo("Database schema is ready.")


@app.command("login-telegram")
def login_telegram() -> None:
    configure_logging()

    async def _login():
        settings = get_settings()
        client = build_client()
        await client.connect()
        try:
            if not await client.is_user_authorized():
                await client.send_code_request(settings.telegram_phone)
                code = typer.prompt("Telegram login code").strip()
                try:
                    await client.sign_in(phone=settings.telegram_phone, code=code)
                except SessionPasswordNeededError:
                    password = typer.prompt("Telegram 2FA password", hide_input=True).strip()
                    await client.sign_in(password=password)
            me = await client.get_me()
            typer.echo(f"Telegram session ready for {getattr(me, 'username', '') or getattr(me, 'id', '')}.")
        finally:
            await client.disconnect()

    asyncio.run(_login())


@app.command("run-telegram-worker")
def run_telegram_worker() -> None:
    configure_logging()

    async def _run():
        settings = get_settings()
        worker = TelegramWorker(refresh_seconds=settings.telegram_refresh_seconds)
        await worker.run_forever()

    asyncio.run(_run())


@app.command("backfill-all-telegram")
def backfill_all_telegram() -> None:
    """Run full historical backfill for ALL active Telegram channels, then exit."""
    configure_logging()

    async def _backfill():
        settings = get_settings()
        worker = TelegramWorker(refresh_seconds=settings.telegram_refresh_seconds)
        await worker.init_db()
        await worker.ensure_authorized()
        await worker.refresh_sources()
        await worker.backfill_all_sources_full()
        await worker.client.disconnect()

    asyncio.run(_backfill())
    typer.echo("Telegram full backfill completed.")


@app.command("add-telegram-source")
def add_telegram_source(identifier: str) -> None:
    configure_logging()

    async def _add():
        settings = get_settings()
        worker = TelegramWorker(refresh_seconds=settings.telegram_refresh_seconds)
        await worker.init_db()
        await worker.ensure_authorized()
        service = TelegramOnboardingService(worker)
        result = await service.add_source(identifier)
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(_add())


@app.command("audit-telegram")
def audit_telegram(
    source_key: str = typer.Option("", help="Specific source key, for example telegram:bakutvxeber"),
    all_sources: bool = typer.Option(False, "--all-sources", help="Audit all Telegram sources"),
    limit: int = typer.Option(10, help="How many latest messages to compare"),
) -> None:
    configure_logging()

    async def _audit():
        settings = get_settings()
        worker = TelegramWorker(refresh_seconds=settings.telegram_refresh_seconds)
        await worker.init_db()
        await worker.ensure_authorized()
        if all_sources:
            result = await worker.audit_all_sources(limit=limit)
        else:
            if not source_key:
                raise typer.BadParameter("source_key veya --all-sources gerekli")
            result = await worker.audit_source(source_key, limit=limit)
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(_audit())


@app.command("pause-source")
def pause_source(source_key: str) -> None:
    configure_logging()

    async def _pause():
        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.set_source_state(source_key, "paused")
            if source is None:
                raise typer.Exit(code=1)
            RuntimeStateStore().set(source.key, runtime_status="paused")
            typer.echo(f"Paused {source.key}")

    asyncio.run(_pause())


@app.command("resume-source")
def resume_source(source_key: str) -> None:
    configure_logging()

    async def _resume():
        async with session_scope() as session:
            repo = Repository(session)
            source = await repo.set_source_state(source_key, "running")
            if source is None:
                raise typer.Exit(code=1)
            RuntimeStateStore().set(source.key, runtime_status="idle")
            typer.echo(f"Running {source.key}")

    asyncio.run(_resume())


@app.command("reset-content")
def reset_content(
    force: bool = typer.Option(
        False,
        "--force",
        help="Delete stored articles and news rows while preserving Telegram source definitions.",
    ),
) -> None:
    configure_logging()
    if not force and not typer.confirm(
        "Stored articles and ingested news will be deleted, but Telegram source definitions will be kept. Continue?"
    ):
        raise typer.Abort()

    result = asyncio.run(_reset_content())
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("source-status")
def source_status() -> None:
    configure_logging()

    async def _status():
        async with session_scope() as session:
            repo = Repository(session)
            rows = await repo.list_sources()
            summary = await repo.status_summary()
            RuntimeStateStore().annotate(rows)
            typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))
            typer.echo(
                json.dumps(
                    [
                        {
                            "key": source.key,
                            "type": source.type,
                            "desired_state": source.desired_state,
                            "runtime_status": source.runtime_status,
                            "identifier": source.identifier,
                            "last_error": source.last_error,
                        }
                        for source in rows
                    ],
                    ensure_ascii=False,
                    indent=2,
                )
            )

    asyncio.run(_status())


if __name__ == "__main__":
    app()
