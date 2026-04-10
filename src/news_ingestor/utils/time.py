from __future__ import annotations

from datetime import UTC, datetime


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)

