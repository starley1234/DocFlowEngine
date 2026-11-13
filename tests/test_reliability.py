"""Дополнительные проверки гонок, lease и соответствия миграции моделям."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.models.db import Document, QueueTask, TaskStatus
from src.queue.repository import TaskClaim
from src.queue.worker import Worker
from src.storage import LocalStorage
from tests.helpers import ingest_png, provider_handler


async def test_concurrent_duplicate_upload(
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    results = await asyncio.gather(
        ingest_png(service, png_bytes, "a.png"), ingest_png(service, png_bytes, "b.png")
    )
    assert results[0].document_id == results[1].document_id
    assert sum(result.duplicate for result in results) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Document)) == 1
        assert await session.scalar(select(func.count()).select_from(QueueTask)) == 1
    assert len(list((storage.root / "raw").iterdir())) == 1


async def test_worker_cancels_after_lease_lost(
    settings: Settings,
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uploaded = await ingest_png(service, png_bytes)

    async def slow_run(claim: TaskClaim) -> None:
        async with session_factory.begin() as session:
            await session.execute(
                update(QueueTask)
                .where(QueueTask.id == claim.task_id)
                .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        await asyncio.sleep(10)
        pytest.fail("устаревший воркер должен быть отменён")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_handler())) as client:
        worker = Worker(settings, session_factory, storage, client)
        monkeypatch.setattr(worker.runner, "run", slow_run)
        assert await worker.process_next()
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None
    assert task.status == TaskStatus.CONVERTING
    assert task.lease_expires_at is not None
    assert task.lease_expires_at < datetime.now(UTC)


def test_migration_matches_models(migrated_database: str) -> None:
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option("sqlalchemy.url", migrated_database.replace("%", "%%"))
    command.check(config)
