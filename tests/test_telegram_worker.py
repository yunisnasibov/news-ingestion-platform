from __future__ import annotations

import importlib
import sys
import types
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

telethon_module = types.ModuleType('telethon')
telethon_module.TelegramClient = object
telethon_module.events = types.SimpleNamespace(NewMessage=object)
telethon_utils_module = types.ModuleType('telethon.utils')
telethon_utils_module.get_peer_id = lambda value: value
sys.modules['telethon'] = telethon_module
sys.modules['telethon.utils'] = telethon_utils_module

models_module = types.ModuleType('news_ingestor.db.models')
models_module.Source = object
sys.modules['news_ingestor.db.models'] = models_module

repository_module = types.ModuleType('news_ingestor.db.repository')
repository_module.Repository = object
sys.modules['news_ingestor.db.repository'] = repository_module

schema_module = types.ModuleType('news_ingestor.db.schema')
async def _noop_init_db():
    return None
schema_module.initialize_database = _noop_init_db
sys.modules['news_ingestor.db.schema'] = schema_module

session_module = types.ModuleType('news_ingestor.db.session')
session_module.session_scope = None
sys.modules['news_ingestor.db.session'] = session_module

schemas_module = types.ModuleType('news_ingestor.schemas')
class RawIngestPayload:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
schemas_module.RawIngestPayload = RawIngestPayload
sys.modules['news_ingestor.schemas'] = schemas_module

audit_module = types.ModuleType('news_ingestor.services.audit')
audit_module.build_audit_payload = lambda **kwargs: SimpleNamespace(
    status='ok',
    live_latest_item_id='',
    db_latest_item_id='',
    details={'missing_in_db': []},
)
sys.modules['news_ingestor.services.audit'] = audit_module

checkpoints_module = types.ModuleType('news_ingestor.services.checkpoints')
class CheckpointService:
    def __init__(self, repo):
        self.repo = repo
    async def get_last_message_id(self, source):
        return 0
    async def advance_message_checkpoint(self, source, *, last_message_id: int):
        return None
checkpoints_module.CheckpointService = CheckpointService
sys.modules['news_ingestor.services.checkpoints'] = checkpoints_module

normalizer_module = types.ModuleType('news_ingestor.services.normalizer')
class NormalizerService:
    def normalize_telegram_message(self, source, payload):
        return payload
normalizer_module.NormalizerService = NormalizerService
sys.modules['news_ingestor.services.normalizer'] = normalizer_module

runtime_state_module = types.ModuleType('news_ingestor.services.runtime_state')
class RuntimeStateStore:
    def set(self, *args, **kwargs):
        return None
    def heartbeat(self, *args, **kwargs):
        return None
runtime_state_module.RuntimeStateStore = RuntimeStateStore
sys.modules['news_ingestor.services.runtime_state'] = runtime_state_module

telegram_backfill_state_module = types.ModuleType('news_ingestor.services.telegram_backfill_state')
class TelegramBackfillStateStore:
    def get(self, *args, **kwargs):
        return SimpleNamespace(
            historical_complete=False,
            next_offset_id=0,
            last_oldest_id=0,
            total_persisted=0,
        )
    def set_progress(self, *args, **kwargs):
        return None
    def mark_complete(self, *args, **kwargs):
        return None
    def clear_all(self):
        return None
telegram_backfill_state_module.TelegramBackfillStateStore = TelegramBackfillStateStore
sys.modules['news_ingestor.services.telegram_backfill_state'] = telegram_backfill_state_module

settings_module = types.ModuleType('news_ingestor.settings')
settings_module.get_settings = lambda: SimpleNamespace(telegram_backfill_limit=200)
sys.modules['news_ingestor.settings'] = settings_module

client_module = types.ModuleType('news_ingestor.telegram.client')
client_module.build_client = lambda: None
sys.modules['news_ingestor.telegram.client'] = client_module

serializer_module = types.ModuleType('news_ingestor.telegram.serializer')
serializer_module.serialize_message = lambda *args, **kwargs: {}
sys.modules['news_ingestor.telegram.serializer'] = serializer_module

text_module = types.ModuleType('news_ingestor.utils.text')
text_module.sha256_text = lambda value: 'hash'
sys.modules['news_ingestor.utils.text'] = text_module

time_module = types.ModuleType('news_ingestor.utils.time')
time_module.utc_now = lambda: None
sys.modules['news_ingestor.utils.time'] = time_module

ingestor = importlib.import_module('news_ingestor.telegram.ingestor')
TelegramWorker = ingestor.TelegramWorker


class TelegramWorkerResetTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_empty_source_uses_full_backfill_even_if_checkpoint_exists(self) -> None:
        source = SimpleNamespace(
            key='telegram:test-channel',
            id='source-1',
            identifier='test-channel',
        )
        repo = AsyncMock()
        repo.get_source_by_key.return_value = source
        repo.has_news_for_source.return_value = False

        @asynccontextmanager
        async def fake_session_scope():
            yield object()

        worker = object.__new__(TelegramWorker)
        worker.client = MagicMock()
        worker.runtime_state = MagicMock()
        worker.normalizer = MagicMock()
        worker.backfill_state = MagicMock()
        worker.backfill_state.get.return_value = SimpleNamespace(historical_complete=False)
        worker.full_backfill_source = AsyncMock()

        with patch('news_ingestor.telegram.ingestor.session_scope', fake_session_scope), patch(
            'news_ingestor.telegram.ingestor.Repository',
            return_value=repo,
        ), patch('news_ingestor.telegram.ingestor.resolve_telegram_entity', new=AsyncMock()):
            await TelegramWorker.ingest_source_history(worker, source)

        worker.full_backfill_source.assert_awaited_once_with(source)
        worker.client.iter_messages.assert_not_called()

    async def test_incomplete_backfill_resumes_full_backfill_even_if_news_exist(self) -> None:
        source = SimpleNamespace(
            key='telegram:test-channel',
            id='source-1',
            identifier='test-channel',
        )

        worker = object.__new__(TelegramWorker)
        worker.client = MagicMock()
        worker.runtime_state = MagicMock()
        worker.normalizer = MagicMock()
        worker.backfill_state = MagicMock()
        worker.backfill_state.get.return_value = SimpleNamespace(historical_complete=False)
        worker.full_backfill_source = AsyncMock()

        await TelegramWorker.ingest_source_history(worker, source)

        worker.full_backfill_source.assert_awaited_once_with(source)
        worker.client.iter_messages.assert_not_called()


if __name__ == '__main__':
    unittest.main()
