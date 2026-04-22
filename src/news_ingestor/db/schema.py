from __future__ import annotations

from sqlalchemy import text

from news_ingestor.db.base import Base
from news_ingestor.db.session import get_engine


LEGACY_TABLES = [
    "canonical_news_links",
    "canonical_news",
    "news_items",
    "raw_ingest_records",
    "source_audits",
    "source_checkpoints",
    "sources",
]


async def initialize_database() -> None:
    async with get_engine().begin() as connection:
        if await _legacy_tables_exist(connection):
            await _migrate_legacy_schema_to_unified(connection)
            await _drop_legacy_tables(connection)
        elif await _split_schema_exists(connection):
            await _migrate_split_schema_to_unified(connection)
        await connection.run_sync(Base.metadata.create_all)
        await _drop_obsolete_unified_columns(connection)
        await _ensure_unified_indexes(connection)


async def _table_exists(connection, table_name: str) -> bool:
    result = await connection.execute(
        text(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_name = :table_name
            )
            """
        ),
        {"table_name": table_name},
    )
    return bool(result.scalar())


async def _legacy_tables_exist(connection) -> bool:
    result = await connection.execute(
        text(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_name IN (
                  'canonical_news_links',
                  'canonical_news',
                  'news_items',
                  'raw_ingest_records',
                  'source_audits',
                  'source_checkpoints',
                  'sources'
                )
            )
            """
        )
    )
    return bool(result.scalar())


async def _split_schema_exists(connection) -> bool:
    return await _table_exists(connection, "source_state")


async def _create_unified_temp_table(connection) -> None:
    await connection.execute(text("DROP TABLE IF EXISTS news_unified_tmp"))
    await connection.execute(
        text(
            """
            CREATE TABLE news_unified_tmp (
              id varchar(36) PRIMARY KEY,
              record_kind varchar(16) NOT NULL DEFAULT 'news',
              source_key varchar(255) NOT NULL DEFAULT '',
              source_item_id varchar(255) NOT NULL DEFAULT '',
              fetched_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
              observed_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
              published_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
              origin_url varchar(1024) NOT NULL DEFAULT '',
              body_text text NOT NULL DEFAULT '',
              raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
              raw_text text NOT NULL DEFAULT '',
              desired_state varchar(32) NOT NULL DEFAULT 'running',
              last_message_id integer NOT NULL DEFAULT 0,
              created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )


async def _migrate_split_schema_to_unified(connection) -> None:
    await _create_unified_temp_table(connection)

    await connection.execute(
        text(
            """
            INSERT INTO news_unified_tmp (
              id,
              record_kind,
              source_key,
              desired_state,
              last_message_id,
              created_at,
              updated_at
            )
            SELECT
              id,
              'source',
              key,
              desired_state,
              last_message_id,
              created_at,
              updated_at
            FROM source_state
            """
        )
    )

    await connection.execute(
        text(
            """
            INSERT INTO news_unified_tmp (
              id,
              record_kind,
              source_key,
              source_item_id,
              fetched_at,
              observed_at,
              published_at,
              origin_url,
              body_text,
              raw_payload,
              raw_text,
              created_at,
              updated_at
            )
            SELECT
              n.id,
              'news',
              s.key,
              n.source_item_id,
              n.fetched_at,
              n.observed_at,
              n.published_at,
              n.origin_url,
              n.body_text,
              n.raw_payload,
              n.raw_text,
              n.created_at,
              n.updated_at
            FROM news n
            JOIN source_state s ON s.id = n.source_id
            """
        )
    )

    await connection.execute(text('DROP TABLE IF EXISTS "news" CASCADE'))
    await connection.execute(text('DROP TABLE IF EXISTS "source_state" CASCADE'))
    await connection.execute(text("ALTER TABLE news_unified_tmp RENAME TO news"))

    await _ensure_unified_indexes(connection)


async def _migrate_legacy_schema_to_unified(connection) -> None:
    await _create_unified_temp_table(connection)

    await connection.execute(
        text(
            """
            INSERT INTO news_unified_tmp (
              id,
              record_kind,
              source_key,
              desired_state,
              last_message_id,
              created_at,
              updated_at
            )
            SELECT
              s.id,
              'source',
              s.key,
              COALESCE(s.desired_state, 'running'),
              COALESCE(sc.last_message_id, 0),
              COALESCE(s.created_at, CURRENT_TIMESTAMP),
              COALESCE(s.updated_at, CURRENT_TIMESTAMP)
            FROM sources s
            LEFT JOIN source_checkpoints sc ON sc.source_id = s.id
            """
        )
    )

    await connection.execute(
        text(
            """
            INSERT INTO news_unified_tmp (
              id,
              record_kind,
              source_key,
              source_item_id,
              fetched_at,
              observed_at,
              published_at,
              origin_url,
              body_text,
              raw_payload,
              raw_text,
              created_at,
              updated_at
            )
            SELECT
              r.id,
              'news',
              s.key,
              r.source_item_id,
              r.fetched_at,
              r.observed_at,
              COALESCE(n.published_at, r.observed_at),
              COALESCE(r.origin_url, ''),
              COALESCE(n.body_text, ''),
              COALESCE(r.raw_payload, '{}'::jsonb),
              COALESCE(r.raw_text, ''),
              COALESCE(r.created_at, CURRENT_TIMESTAMP),
              COALESCE(n.updated_at, r.updated_at, CURRENT_TIMESTAMP)
            FROM raw_ingest_records r
            JOIN sources s ON s.id = r.source_id
            LEFT JOIN news_items n ON n.raw_record_id = r.id
            """
        )
    )

    await connection.execute(text('DROP TABLE IF EXISTS "news" CASCADE'))
    await connection.execute(text("ALTER TABLE news_unified_tmp RENAME TO news"))
    await _ensure_unified_indexes(connection)


async def _ensure_unified_indexes(connection) -> None:
    statements = [
        "DROP INDEX IF EXISTS uq_news_source_item",
        "DROP INDEX IF EXISTS ix_news_source_id",
        "DROP INDEX IF EXISTS ix_news_source_observed",
        "DROP INDEX IF EXISTS ix_news_source_published",
        "DROP INDEX IF EXISTS ix_news_record_kind",
        "DROP INDEX IF EXISTS ix_news_source_kind_desired",
        "DROP INDEX IF EXISTS ix_news_source_kind_runtime",
        "DROP INDEX IF EXISTS uq_news_source_row",
        "DROP INDEX IF EXISTS uq_news_message_row",
        "CREATE INDEX IF NOT EXISTS ix_news_record_kind ON news USING btree (record_kind)",
        "CREATE INDEX IF NOT EXISTS ix_news_source_kind_desired ON news USING btree (record_kind, desired_state)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_source_row ON news USING btree (source_key) WHERE record_kind = 'source'",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_message_row ON news USING btree (source_key, source_item_id) WHERE record_kind = 'news'",
        "CREATE INDEX IF NOT EXISTS ix_news_source_published ON news USING btree (source_key, published_at) WHERE record_kind = 'news'",
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _drop_obsolete_unified_columns(connection) -> None:
    if not await _table_exists(connection, "news"):
        return

    statements = [
        "ALTER TABLE news DROP COLUMN IF EXISTS source_event_type",
        "ALTER TABLE news DROP COLUMN IF EXISTS config",
        "ALTER TABLE news DROP COLUMN IF EXISTS last_event_at",
        "ALTER TABLE news DROP COLUMN IF EXISTS last_audit_details",
        "ALTER TABLE news DROP COLUMN IF EXISTS dedupe_key",
        "ALTER TABLE news DROP COLUMN IF EXISTS parser_error",
        "ALTER TABLE news DROP COLUMN IF EXISTS missing_fields",
        "ALTER TABLE news DROP COLUMN IF EXISTS quality_flags",
        "ALTER TABLE news DROP COLUMN IF EXISTS last_audit_status",
        "ALTER TABLE news DROP COLUMN IF EXISTS audit_checked_at",
        "ALTER TABLE news DROP COLUMN IF EXISTS title",
        "ALTER TABLE news DROP COLUMN IF EXISTS parse_status",
        "ALTER TABLE news DROP COLUMN IF EXISTS identifier",
        "ALTER TABLE news DROP COLUMN IF EXISTS display_name",
        "ALTER TABLE news DROP COLUMN IF EXISTS runtime_status",
        "ALTER TABLE news DROP COLUMN IF EXISTS last_heartbeat_at",
        "ALTER TABLE news DROP COLUMN IF EXISTS last_error",
        # Simplified schema — these were write-only, never read back
        "ALTER TABLE news DROP COLUMN IF EXISTS raw_payload",
        "ALTER TABLE news DROP COLUMN IF EXISTS raw_text",
        "ALTER TABLE news DROP COLUMN IF EXISTS observed_at",
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _drop_legacy_tables(connection) -> None:
    for table_name in LEGACY_TABLES:
        await connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
