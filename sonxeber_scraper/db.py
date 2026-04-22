from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .config import Settings
from .models import ArticleRecord, SyncSummary
from .utils import utc_now_iso

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.connection: psycopg.Connection[Any] | None = None

    def initialize(self) -> None:
        self._ensure_database_exists()
        self.connection = psycopg.connect(
            **self.settings.postgres_connect_kwargs(),
            autocommit=True,
            row_factory=dict_row,
        )
        self.connection.execute(
            sql.SQL("SET TIME ZONE {}").format(sql.Literal(self.settings.postgres_timezone))
        )
        if self._articles_need_migration():
            self._migrate_articles_schema()
        self._create_tables()
        self._drop_auxiliary_tables()

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    # ── FIX #1: Auto-reconnect ────────────────────────────────────────────────

    def _reconnect(self) -> None:
        """Close stale connection and open a fresh one."""
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None
        logger.warning("db_reconnecting host=%s port=%s", self.settings.postgres_host, self.settings.postgres_port)
        self.connection = psycopg.connect(
            **self.settings.postgres_connect_kwargs(),
            autocommit=True,
            row_factory=dict_row,
        )
        self.connection.execute(
            sql.SQL("SET TIME ZONE {}").format(sql.Literal(self.settings.postgres_timezone))
        )

    def _ensure_connected(self) -> None:
        """Ping the server; reconnect transparently on a dropped connection."""
        if self.connection is None:
            return
        try:
            self.connection.execute("SELECT 1")
        except psycopg.OperationalError:
            self._reconnect()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _ensure_database_exists(self) -> None:
        with psycopg.connect(
            **self.settings.postgres_connect_kwargs(admin=True),
            autocommit=True,
            row_factory=dict_row,
        ) as admin_connection:
            exists = admin_connection.execute(
                """
                SELECT 1
                FROM pg_database
                WHERE datname = %s
                """,
                (self.settings.postgres_dbname,),
            ).fetchone()
            if exists is None:
                admin_connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.settings.postgres_dbname)
                    )
                )
                admin_connection.execute(
                    sql.SQL("ALTER DATABASE {} SET timezone = {}").format(
                        sql.Identifier(self.settings.postgres_dbname),
                        sql.Literal(self.settings.postgres_timezone),
                    )
                )

    def _require_connection(self) -> psycopg.Connection[Any]:
        if self.connection is None:
            raise RuntimeError("Database is not initialized")
        return self.connection

    def _table_exists(self, table_name: str) -> bool:
        row = self._require_connection().execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
            ) AS exists
            """,
            (table_name,),
        ).fetchone()
        return bool(row and row["exists"])

    def _articles_need_migration(self) -> bool:
        if not self._table_exists("articles"):
            return False
        columns = {
            row["column_name"]
            for row in self._require_connection().execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'articles'
                """
            ).fetchall()
        }
        return "article_url" not in columns

    def _create_tables(self) -> None:
        connection = self._require_connection()
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id BIGSERIAL PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_article_id BIGINT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                content_text TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT 'uncategorized',
                article_url TEXT NOT NULL,
                image_url TEXT NOT NULL DEFAULT '',
                published_at TEXT NOT NULL DEFAULT '',
                fetched_at TIMESTAMPTZ NOT NULL,
                content_hash TEXT NOT NULL DEFAULT '',
                UNIQUE(source_name, source_article_id)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_articles_source_article_url
            ON articles(source_name, article_url)
            """
        )
        # Add content_hash column if missing (migration for existing DBs)
        columns = {
            row["column_name"]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'articles'
                """
            ).fetchall()
        }
        if "content_hash" not in columns:
            connection.execute("ALTER TABLE articles ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''")

    def _drop_auxiliary_tables(self) -> None:
        connection = self._require_connection()
        connection.execute("DROP TABLE IF EXISTS article_meta")
        connection.execute("DROP TABLE IF EXISTS sync_runs")

    def _migrate_articles_schema(self) -> None:
        connection = self._require_connection()
        with connection.transaction():
            connection.execute("ALTER TABLE articles RENAME TO articles_legacy")
            connection.execute(
                """
                CREATE TABLE articles (
                    id BIGSERIAL PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    source_article_id BIGINT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    content_text TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT 'uncategorized',
                    article_url TEXT NOT NULL,
                    image_url TEXT NOT NULL DEFAULT '',
                    published_at TEXT NOT NULL DEFAULT '',
                    fetched_at TIMESTAMPTZ NOT NULL,
                    UNIQUE(source_name, source_article_id)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO articles (
                    source_name,
                    source_article_id,
                    title,
                    content_text,
                    category,
                    article_url,
                    image_url,
                    published_at,
                    fetched_at
                )
                SELECT
                    source_name,
                    source_article_id,
                    title,
                    content_text,
                    category,
                    url,
                    hero_image_url,
                    published_date_raw,
                    detail_fetched_at
                FROM articles_legacy
                """
            )
            connection.execute("DROP TABLE articles_legacy")

    # ── Public API ────────────────────────────────────────────────────────────

    def get_max_source_article_id(self, source_name: str) -> int | None:
        self._ensure_connected()
        row = self._require_connection().execute(
            """
            SELECT MAX(source_article_id) AS max_id
            FROM articles
            WHERE source_name = %s
            """,
            (source_name,),
        ).fetchone()
        if row is None or row["max_id"] is None:
            return None
        return int(row["max_id"])

    def get_min_source_article_id(self, source_name: str) -> int | None:
        self._ensure_connected()
        row = self._require_connection().execute(
            """
            SELECT MIN(source_article_id) AS min_id
            FROM articles
            WHERE source_name = %s
            """,
            (source_name,),
        ).fetchone()
        if row is None or row["min_id"] is None:
            return None
        return int(row["min_id"])

    def get_existing_article_ids(self, source_name: str, article_ids: list[int]) -> set[int]:
        if not article_ids:
            return set()
        self._ensure_connected()
        rows = self._require_connection().execute(
            """
            SELECT source_article_id
            FROM articles
            WHERE source_name = %s
              AND source_article_id = ANY(%s)
            """,
            (source_name, article_ids),
        ).fetchall()
        return {int(row["source_article_id"]) for row in rows}

    def get_existing_article_urls(self, source_name: str, urls: list[str]) -> set[str]:
        if not urls:
            return set()
        self._ensure_connected()
        rows = self._require_connection().execute(
            """
            SELECT article_url
            FROM articles
            WHERE source_name = %s
              AND article_url = ANY(%s)
            """,
            (source_name, urls),
        ).fetchall()
        return {str(row["article_url"]) for row in rows}

    # ── FIX #2: True Upsert via INSERT … ON CONFLICT ──────────────────────────

    def upsert_article(self, record: ArticleRecord) -> str:
        """
        Atomically insert-or-update using PostgreSQL's native upsert.
        Replaces the old SELECT → INSERT/UPDATE pattern, which was racy and
        required two round-trips per article.

        `xmax = 0` is the standard PostgreSQL idiom to detect whether a row
        was freshly inserted (xmax == 0) or updated (xmax != 0) inside an
        ON CONFLICT DO UPDATE clause.
        """
        self._ensure_connected()
        now = utc_now_iso()
        row = self._require_connection().execute(
            """
            INSERT INTO articles (
                source_name,
                source_article_id,
                title,
                content_text,
                category,
                article_url,
                image_url,
                published_at,
                fetched_at,
                content_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_name, source_article_id) DO UPDATE SET
                title           = EXCLUDED.title,
                content_text    = EXCLUDED.content_text,
                category        = EXCLUDED.category,
                article_url     = EXCLUDED.article_url,
                image_url       = EXCLUDED.image_url,
                published_at    = EXCLUDED.published_at,
                fetched_at      = EXCLUDED.fetched_at,
                content_hash    = EXCLUDED.content_hash
            RETURNING (xmax = 0) AS inserted
            """,
            (
                record.source_name,
                record.source_article_id,
                record.title,
                record.content_text,
                record.category,
                record.url,
                record.hero_image_url,
                record.published_at or record.published_date_raw,
                now,
                record.content_hash,
            ),
        ).fetchone()
        return "inserted" if row and row["inserted"] else "updated"

    def record_sync_run(self, source_name: str, summary: SyncSummary, started_at: str) -> None:
        # Sync results are logged to stdout — DB stays clean with only data tables.
        pass

    def get_article_count(self, source_name: str | None = None) -> int:
        self._ensure_connected()
        if source_name is None:
            row = self._require_connection().execute(
                "SELECT COUNT(*) AS count FROM articles"
            ).fetchone()
            return int(row["count"] if row else 0)
        row = self._require_connection().execute(
            """
            SELECT COUNT(*) AS count
            FROM articles
            WHERE source_name = %s
            """,
            (source_name,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def get_article_counts_by_source(self) -> list[dict[str, object]]:
        self._ensure_connected()
        return self._require_connection().execute(
            """
            SELECT source_name, COUNT(*) AS count
            FROM articles
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()
