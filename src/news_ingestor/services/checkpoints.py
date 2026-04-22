from __future__ import annotations

from news_ingestor.db.models import Source
from news_ingestor.db.repository import Repository
class CheckpointService:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def get_last_message_id(self, source: Source) -> int:
        current = await self.repo.get_source_by_id(source.id)
        return current.last_message_id if current is not None else 0

    async def advance_message_checkpoint(self, source: Source, *, last_message_id: int) -> None:
        await self.repo.update_checkpoint(
            source.id,
            last_message_id=last_message_id,
        )
