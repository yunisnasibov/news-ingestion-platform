"""Sonxeber.az scraper package."""

from __future__ import annotations

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _patch_requests_sessions() -> None:
    if getattr(requests, "_sonxeber_retry_patch", False):
        return

    base_session = requests.sessions.Session
    retry_total = int(os.getenv("SONXEBER_REQUEST_RETRY_TOTAL", "3"))
    backoff_seconds = float(os.getenv("SONXEBER_REQUEST_RETRY_BACKOFF_SECONDS", "1.0"))
    pool_size = int(os.getenv("SONXEBER_REQUEST_POOL_SIZE", "100"))

    class RetrySession(base_session):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            retry = Retry(
                total=retry_total,
                connect=retry_total,
                read=retry_total,
                status=retry_total,
                backoff_factor=backoff_seconds,
                allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
                status_forcelist=(408, 425, 429, 500, 502, 503, 504),
                raise_on_status=False,
                respect_retry_after_header=True,
            )
            adapter = HTTPAdapter(
                max_retries=retry,
                pool_connections=pool_size,
                pool_maxsize=pool_size,
            )
            self.mount("http://", adapter)
            self.mount("https://", adapter)

    requests.sessions.Session = RetrySession
    requests.Session = RetrySession
    requests._sonxeber_retry_patch = True


_patch_requests_sessions()
