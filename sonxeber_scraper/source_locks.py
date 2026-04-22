from __future__ import annotations

import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .config import Settings


class SourceLockError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourceLockPaths:
    live: Path
    backfill: Path


class SourceLockManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        default_lock_dir = self.settings.project_root / "data" / "locks"
        self.lock_dir = Path(os.getenv("SONXEBER_LOCK_DIR", str(default_lock_dir)))
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        self.lock_heartbeat_interval_seconds = 1.0
        self.lock_stale_after_seconds = 5.0
        self.current_hostname = socket.gethostname()
        self.current_pid = os.getpid()
        self.current_pid_start_time = self._read_process_start_time(self.current_pid)
        self._cleanup_stale_locks()

    def paths_for(self, source_name: str) -> SourceLockPaths:
        safe_name = source_name.replace("/", "_").replace(":", "_")
        return SourceLockPaths(
            live=self.lock_dir / f"{safe_name}.live.lock",
            backfill=self.lock_dir / f"{safe_name}.backfill.lock",
        )

    def has_backfill_lock(self, source_name: str) -> bool:
        self._cleanup_stale_locks()
        path = self.paths_for(source_name).backfill
        if not path.exists():
            return False
        if self._is_lock_stale(path):
            self._remove_lock(path)
            return False
        return True

    @contextmanager
    def live_lock(self, source_name: str) -> Iterator[None]:
        self._cleanup_stale_locks()
        paths = self.paths_for(source_name)
        if paths.backfill.exists():
            if self._is_lock_stale(paths.backfill):
                self._remove_lock(paths.backfill)
            else:
                raise SourceLockError(f"backfill_active source={source_name}")
        self._create_lock(paths.live, source_name=source_name, lock_kind="live")
        with self._heartbeat(paths.live):
            try:
                yield
            finally:
                self._remove_lock(paths.live)

    @contextmanager
    def backfill_lock(
        self,
        source_name: str,
        *,
        wait_for_live_seconds: int = 120,
        poll_interval_seconds: float = 1.0,
    ) -> Iterator[None]:
        self._cleanup_stale_locks()
        paths = self.paths_for(source_name)
        self._create_lock(paths.backfill, source_name=source_name, lock_kind="backfill")
        with self._heartbeat(paths.backfill):
            try:
                deadline = time.time() + max(wait_for_live_seconds, 0)
                while paths.live.exists():
                    if self._is_lock_stale(paths.live):
                        self._remove_lock(paths.live)
                        break
                    if wait_for_live_seconds <= 0 or time.time() >= deadline:
                        raise SourceLockError(f"live_sync_still_active source={source_name}")
                    time.sleep(poll_interval_seconds)
                yield
            finally:
                self._remove_lock(paths.backfill)

    @contextmanager
    def _heartbeat(self, path: Path) -> Iterator[None]:
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(path, stop_event),
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            thread.join(timeout=self.lock_heartbeat_interval_seconds * 2)

    def _heartbeat_loop(self, path: Path, stop_event: threading.Event) -> None:
        while not stop_event.wait(self.lock_heartbeat_interval_seconds):
            try:
                os.utime(path, None)
            except FileNotFoundError:
                return

    def _create_lock(self, path: Path, *, source_name: str, lock_kind: str) -> None:
        payload = {
            "source_name": source_name,
            "lock_kind": lock_kind,
            "pid": self.current_pid,
            "hostname": self.current_hostname,
            "pid_start_time": self.current_pid_start_time,
            "created_at": int(time.time()),
        }
        if path.exists() and self._is_lock_stale(path):
            self._remove_lock(path)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise SourceLockError(f"lock_already_exists path={path}") from exc
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
        except Exception:
            self._remove_lock(path)
            raise

    def _remove_lock(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def _is_lock_stale(self, path: Path) -> bool:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return False

        if (time.time() - stat.st_mtime) > self.lock_stale_after_seconds:
            return True

        payload = self._read_lock_payload(path)
        if not payload:
            return False

        lock_hostname = str(payload.get("hostname") or "")
        if lock_hostname != self.current_hostname:
            return False

        lock_pid = payload.get("pid")
        try:
            lock_pid_int = int(lock_pid)
        except (TypeError, ValueError):
            return True

        current_start = self._read_process_start_time(lock_pid_int)
        if not current_start:
            return True

        saved_start = str(payload.get("pid_start_time") or "")
        if not saved_start:
            return True
        if current_start != saved_start:
            return True
        return False

    def _cleanup_stale_locks(self) -> None:
        for path in self.lock_dir.glob("*.lock"):
            if self._is_lock_stale(path):
                self._remove_lock(path)

    def _read_lock_payload(self, path: Path) -> dict[str, object]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _read_process_start_time(pid: int) -> str:
        try:
            stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        except OSError:
            return ""
        parts = stat_text.split()
        if len(parts) <= 21:
            return ""
        return parts[21]
