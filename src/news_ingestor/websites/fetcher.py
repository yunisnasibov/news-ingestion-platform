from __future__ import annotations

from dataclasses import dataclass

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from news_ingestor.utils.time import utc_now


@dataclass(slots=True)
class WebsiteFetchResult:
    url: str
    final_url: str
    status_code: int
    html: str
    fetched_at: str


class WebsiteFetcher:
    def __init__(self, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def fetch(self, url: str) -> WebsiteFetchResult:
        async with httpx.AsyncClient(follow_redirects=True, timeout=self.timeout_seconds) as client:
            response = await client.get(url, headers={"User-Agent": "news-ingestor/0.1"})
            response.raise_for_status()
            return WebsiteFetchResult(
                url=url,
                final_url=str(response.url),
                status_code=response.status_code,
                html=response.text,
                fetched_at=utc_now().isoformat(),
            )
