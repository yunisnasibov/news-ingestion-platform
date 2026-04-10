from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from news_ingestor.db.base import Base, TimestampMixin
from news_ingestor.utils.time import EPOCH


def uuid_str() -> str:
    return str(uuid.uuid4())


class Record(Base, TimestampMixin):
    __tablename__ = "news"
    __table_args__ = (
        Index("ix_news_record_kind", "record_kind"),
        Index("ix_news_source_kind_desired", "record_kind", "desired_state"),
        Index("ix_news_source_kind_runtime", "record_kind", "runtime_status"),
        Index(
            "uq_news_source_row",
            "source_key",
            unique=True,
            postgresql_where=text("record_kind = 'source'"),
        ),
        Index(
            "uq_news_message_row",
            "source_key",
            "source_item_id",
            unique=True,
            postgresql_where=text("record_kind = 'news'"),
        ),
        Index(
            "ix_news_source_observed",
            "source_key",
            "observed_at",
            postgresql_where=text("record_kind = 'news'"),
        ),
        Index(
            "ix_news_source_published",
            "source_key",
            "published_at",
            postgresql_where=text("record_kind = 'news'"),
        ),
        Index(
            "ix_news_dedupe_key",
            "dedupe_key",
            postgresql_where=text("record_kind = 'news'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    record_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="news", server_default="news")
    source_key: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    source_item_id: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: EPOCH,
        server_default=text("'1970-01-01T00:00:00+00:00'::timestamptz"),
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: EPOCH,
        server_default=text("'1970-01-01T00:00:00+00:00'::timestamptz"),
    )
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: EPOCH,
        server_default=text("'1970-01-01T00:00:00+00:00'::timestamptz"),
    )
    origin_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="", server_default="")
    title: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    body_text: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    raw_payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    raw_text: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    parse_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    parser_error: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    missing_fields: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    quality_flags: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )

    identifier: Mapped[str] = mapped_column(String(512), nullable=False, default="", server_default="")
    display_name: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    desired_state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="running",
        server_default="running",
    )
    runtime_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="idle",
        server_default="idle",
    )
    last_message_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: EPOCH,
        server_default=text("'1970-01-01T00:00:00+00:00'::timestamptz"),
    )
    last_error: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    last_audit_status: Mapped[str] = mapped_column(String(32), nullable=False, default="", server_default="")
    audit_checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: EPOCH,
        server_default=text("'1970-01-01T00:00:00+00:00'::timestamptz"),
    )

    @property
    def key(self) -> str:
        return self.source_key

    @property
    def type(self) -> str:
        return "telegram_channel"


Source = Record
NewsRecord = Record
