from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

from .config import Settings


class ProcessController:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def start(self) -> tuple[bool, str]:
        pid = self._get_running_pid()
        if pid is not None:
            return True, f"running pid={pid} log={self.settings.log_path}"

        self.settings.ensure_paths()
        with self.settings.log_path.open("ab") as log_file:
            process = subprocess.Popen(
                [sys.executable, "main.py", "poll"],
                cwd=self.settings.project_root,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=os.environ.copy(),
            )

        time.sleep(1)
        if process.poll() is not None:
            self._remove_pid_file()
            return False, f"failed_to_start exit_code={process.returncode} log={self.settings.log_path}"

        self.settings.pid_path.write_text(str(process.pid))
        return True, f"started pid={process.pid} log={self.settings.log_path}"

    def stop(self) -> tuple[bool, str]:
        pid = self._read_pid()
        if pid is None:
            self._remove_pid_file()
            return True, "already_stopped"

        if not self._is_running(pid):
            self._remove_pid_file()
            return True, f"stale_pid_removed pid={pid}"

        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + 10
        while time.time() < deadline:
            if not self._is_running(pid):
                self._remove_pid_file()
                return True, f"stopped pid={pid}"
            time.sleep(0.25)

        os.kill(pid, signal.SIGKILL)
        self._remove_pid_file()
        return True, f"killed pid={pid}"

    def status(self) -> str:
        pid = self._get_running_pid()
        if pid is None:
            return (
                "stopped "
                f"database={self.settings.database_display_name()} "
                f"log={self.settings.log_path}"
            )
        return (
            f"running pid={pid} "
            f"database={self.settings.database_display_name()} "
            f"log={self.settings.log_path}"
        )

    def _read_pid(self) -> int | None:
        if not self.settings.pid_path.exists():
            return None
        try:
            return int(self.settings.pid_path.read_text().strip())
        except ValueError:
            self._remove_pid_file()
            return None

    def _get_running_pid(self) -> int | None:
        pid = self._read_pid()
        if pid is None:
            return None
        if self._is_running(pid):
            return pid
        self._remove_pid_file()
        return None

    def _is_running(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _remove_pid_file(self) -> None:
        try:
            self.settings.pid_path.unlink()
        except FileNotFoundError:
            pass
