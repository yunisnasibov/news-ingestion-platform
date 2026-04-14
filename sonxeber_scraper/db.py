from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .config import Settings
from .models import ArticleRecord, SyncSummary
from .utils import utc_now_iso


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
        # Clean up any auxiliary tables that shouldn't exist
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

    def get_max_source_article_id(self, source_name: str) -> int | None:
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

    def upsert_article(self, record: ArticleRecord) -> str:
        connection = self._require_connection()
        now = utc_now_iso()
        with connection.transaction():
            existing = connection.execute(
                """
                SELECT id
                FROM articles
                WHERE source_name = %s
                  AND (
                    source_article_id = %s
                    OR article_url = %s
                  )
                ORDER BY CASE WHEN source_article_id = %s THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (
                    record.source_name,
                    record.source_article_id,
                    record.url,
                    record.source_article_id,
                ),
            ).fetchone()

            status = "inserted" if existing is None else "updated"

            if existing is None:
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
                        fetched_at,
                        content_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
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
                )
                return status

            article_id = existing["id"]
            connection.execute(
                """
                UPDATE articles
                SET
                    source_article_id = %s,
                    title = %s,
                    content_text = %s,
                    category = %s,
                    article_url = %s,
                    image_url = %s,
                    published_at = %s,
                    fetched_at = %s,
                    content_hash = %s
                WHERE id = %s
                """,
                (
                    record.source_article_id,
                    record.title,
                    record.content_text,
                    record.category,
                    record.url,
                    record.hero_image_url,
                    record.published_at or record.published_date_raw,
                    now,
                    record.content_hash,
                    article_id,
                ),
            )
            return status

    def record_sync_run(self, source_name: str, summary: SyncSummary, started_at: str) -> None:
        # Sync results are logged to stdout — DB stays clean with only data tables.
        pass

    def get_article_count(self, source_name: str | None = None) -> int:
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
        return self._require_connection().execute(
            """
            SELECT source_name, COUNT(*) AS count
            FROM articles
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()
