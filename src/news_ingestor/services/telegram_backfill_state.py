from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from news_ingestor.settings import get_settings
from news_ingestor.utils.time import utc_now


@dataclass(slots=True)
class TelegramBackfillSnapshot:
    historical_complete: bool = False
    next_offset_id: int = 0
    last_oldest_id: int = 0
    total_persisted: int = 0
    updated_at: str = ""


class TelegramBackfillStateStore:
    def __init__(self, path: Path | None = None) -> None:
        settings = get_settings()
        self.path = path or (settings.telegram_session_dir / "telegram_backfill_state.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(".lock")

    def get(self, source_key: str) -> TelegramBackfillSnapshot:
        payload = self._read_state()
        item = payload.get(source_key, {})
        return TelegramBackfillSnapshot(
            historical_complete=bool(item.get("historical_complete", False)),
            next_offset_id=int(item.get("next_offset_id", 0) or 0),
            last_oldest_id=int(item.get("last_oldest_id", 0) or 0),
            total_persisted=int(item.get("total_persisted", 0) or 0),
            updated_at=str(item.get("updated_at", "") or ""),
        )

    def set_progress(
        self,
        source_key: str,
        *,
        next_offset_id: int,
        last_oldest_id: int,
        total_persisted: int,
    ) -> TelegramBackfillSnapshot:
        return self._update(
            source_key,
            {
                "historical_complete": False,
                "next_offset_id": int(next_offset_id),
                "last_oldest_id": int(last_oldest_id),
                "total_persisted": int(total_persisted),
                "updated_at": utc_now().isoformat(),
            },
        )

    def mark_complete(
        self,
        source_key: str,
        *,
        last_oldest_id: int,
        total_persisted: int,
    ) -> TelegramBackfillSnapshot:
        return self._update(
            source_key,
            {
                "historical_complete": True,
                "next_offset_id": 0,
                "last_oldest_id": int(last_oldest_id),
                "total_persisted": int(total_persisted),
                "updated_at": utc_now().isoformat(),
            },
        )

    def clear(self, source_key: str) -> None:
        def mutate(payload: dict[str, dict[str, Any]]) -> None:
            payload.pop(source_key, None)

        self._mutate_state(mutate)

    def clear_all(self) -> None:
        def mutate(payload: dict[str, dict[str, Any]]) -> None:
            payload.clear()

        self._mutate_state(mutate)

    def _update(self, source_key: str, item: dict[str, Any]) -> TelegramBackfillSnapshot:
        def mutate(payload: dict[str, dict[str, Any]]) -> None:
            payload[source_key] = item

        self._mutate_state(mutate)
        return self.get(source_key)

    def _read_state(self) -> dict[str, dict[str, Any]]:
        with self._lock_file():
            return self._read_unlocked()

    def _mutate_state(self, callback) -> None:
        with self._lock_file():
            payload = self._read_unlocked()
            callback(payload)
            self._write_unlocked(payload)

    def _read_unlocked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            content = self.path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return {}
        if not content:
            return {}
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            return {}
        if not isinstance(data, dict):
            return {}
        normalized: dict[str, dict[str, Any]] = {}
        for key, value in data.items():
            if isinstance(key, str) and isinstance(value, dict):
                normalized[key] = value
        return normalized

    def _write_unlocked(self, payload: dict[str, dict[str, Any]]) -> None:
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp_path, self.path)

    def _lock_file(self):
        import fcntl

        self.lock_path.touch(exist_ok=True)
        handle = self.lock_path.open("r+", encoding="utf-8")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)

        class _Locker:
            def __enter__(self_nonlocal):
                return handle

            def __exit__(self_nonlocal, exc_type, exc, tb):
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()

        return _Locker()
