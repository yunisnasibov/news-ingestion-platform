from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sonxeber_scraper.config import Settings
from sonxeber_scraper.source_locks import SourceLockManager


class SourceLockManagerTests(unittest.TestCase):
    def test_same_host_pid_start_time_mismatch_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.dict(os.environ, {"SONXEBER_LOCK_DIR": temp_dir}, clear=False):
                manager = SourceLockManager(Settings())
                path = manager.paths_for("azertag.az").backfill
                payload = {
                    "source_name": "azertag.az",
                    "lock_kind": "backfill",
                    "pid": os.getpid(),
                    "hostname": manager.current_hostname,
                    "pid_start_time": "stale-start-time",
                    "created_at": 0,
                }
                path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertTrue(manager._is_lock_stale(path))

    def test_other_host_recent_lock_is_not_treated_as_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.dict(os.environ, {"SONXEBER_LOCK_DIR": temp_dir}, clear=False):
                manager = SourceLockManager(Settings())
                path = manager.paths_for("azertag.az").backfill
                payload = {
                    "source_name": "azertag.az",
                    "lock_kind": "backfill",
                    "pid": 1,
                    "hostname": "different-host",
                    "pid_start_time": "whatever",
                    "created_at": 0,
                }
                path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertFalse(manager._is_lock_stale(path))


if __name__ == "__main__":
    unittest.main()
