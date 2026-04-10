from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from news_ingestor.settings import get_settings


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


@asynccontextmanager
async def session_scope():
    session = get_session_factory()()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
