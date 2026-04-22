from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from news_ingestor.settings import get_settings
from news_ingestor.utils.time import EPOCH, utc_now


@dataclass(slots=True)
class RuntimeSnapshot:
    runtime_status: str = "idle"
    last_heartbeat_at: datetime = EPOCH
    last_error: str = ""


class RuntimeStateStore:
    def __init__(self, path: Path | None = None) -> None:
        settings = get_settings()
        self.path = path or (settings.telegram_session_dir / "runtime_state.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(".lock")

    def get(self, source_key: str) -> RuntimeSnapshot:
        payload = self._read_state()
        item = payload.get(source_key, {})
        heartbeat_raw = item.get("last_heartbeat_at", "")
        try:
            heartbeat = datetime.fromisoformat(heartbeat_raw) if heartbeat_raw else EPOCH
        except ValueError:
            heartbeat = EPOCH
        return RuntimeSnapshot(
            runtime_status=str(item.get("runtime_status", "idle") or "idle"),
            last_heartbeat_at=heartbeat,
            last_error=str(item.get("last_error", "") or ""),
        )

    def set(self, source_key: str, *, runtime_status: str, last_error: str = "") -> RuntimeSnapshot:
        return self._update(
            source_key,
            {
                "runtime_status": runtime_status,
                "last_heartbeat_at": utc_now().isoformat(),
                "last_error": last_error,
            },
        )

    def heartbeat(self, source_key: str) -> RuntimeSnapshot:
        current = self.get(source_key)
        return self._update(
            source_key,
            {
                "runtime_status": current.runtime_status or "running",
                "last_heartbeat_at": utc_now().isoformat(),
                "last_error": current.last_error,
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

    def annotate(self, sources: list[Any]) -> None:
        payload = self._read_state()
        for source in sources:
            item = payload.get(source.key, {})
            heartbeat_raw = item.get("last_heartbeat_at", "")
            try:
                heartbeat = datetime.fromisoformat(heartbeat_raw) if heartbeat_raw else EPOCH
            except ValueError:
                heartbeat = EPOCH
            source.runtime_status = str(item.get("runtime_status", "idle") or "idle")
            source.last_heartbeat_at = heartbeat
            source.last_error = str(item.get("last_error", "") or "")

    def _update(self, source_key: str, item: dict[str, Any]) -> RuntimeSnapshot:
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
