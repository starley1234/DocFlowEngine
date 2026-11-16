"""Интеграционные тесты очереди на настоящем PostgreSQL."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.models.db import Document, DocumentEmbedding, QueueTask, TaskStatus
from src.queue.repository import QueueRepository
from src.queue.worker import Worker
from src.storage import LocalStorage
from tests.helpers import ingest_png, provider_handler


async def test_claim_skips_locked_row(
    service, session_factory: async_sessionmaker[AsyncSession], png_bytes: bytes
) -> None:
    first = await ingest_png(service, png_bytes, "first.png")
    second = await ingest_png(service, png_bytes + b"unique", "second.png")
    repository = QueueRepository(session_factory, 30)
    async with session_factory.begin() as session:
        await session.scalar(
            select(QueueTask).where(QueueTask.id == first.task_id).with_for_update()
        )
        claim = await repository.claim(uuid.uuid4())
        assert claim is not None
        assert claim.task_id == second.task_id
    claims = await asyncio.gather(repository.claim(uuid.uuid4()), repository.claim(uuid.uuid4()))
    assert sum(claim is not None for claim in claims) == 1


async def test_stale_lease_and_fencing(
    service, session_factory: async_sessionmaker[AsyncSession], png_bytes: bytes
) -> None:
    await ingest_png(service, png_bytes)
    repository = QueueRepository(session_factory, 30)
    old = await repository.claim(uuid.uuid4())
    assert old is not None
    async with session_factory.begin() as session:
        await session.execute(
            update(QueueTask)
            .where(QueueTask.id == old.task_id)
            .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    new = await repository.claim(uuid.uuid4())
    assert new is not None
    assert new.lock_version == old.lock_version + 1
    assert new.retry_count == 1
    assert not await repository.update_status(old, TaskStatus.OCR)
    assert not await repository.complete(old, "stale", {}, [])
    assert await repository.complete(new, "fresh", {"summary": "ok"}, [])


async def test_worker_end_to_end(
    settings: Settings,
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    uploaded = await ingest_png(service, png_bytes)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_handler())) as client:
        worker = Worker(settings, session_factory, storage, client)
        assert await worker.process_next()
        assert not await worker.process_next()
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
        document = await session.get(Document, uploaded.document_id)
        embeddings = (await session.scalars(select(DocumentEmbedding))).all()
    assert task is not None and task.status == TaskStatus.COMPLETED
    assert document is not None
    assert document.extracted_data["sender_org"] == "ООО Ромашка"
    assert "оплатить" in document.raw_ocr_text
    assert embeddings
    assert storage.resolve(document.file_path).is_file()


async def test_transient_retry_then_success(
    settings: Settings,
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    uploaded = await ingest_png(service, png_bytes)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path == "/predict" and calls == 0:
            calls += 1
            return httpx.Response(503)
        return provider_handler()(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        worker = Worker(settings, session_factory, storage, client)
        await worker.process_next()
        async with session_factory() as session:
            task = await session.get(QueueTask, uploaded.task_id)
        assert task is not None and task.status == TaskStatus.QUEUED and task.retry_count == 1
        await worker.process_next()
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None and task.status == TaskStatus.COMPLETED


async def test_permanent_failure_and_manual_retry(
    settings: Settings,
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    uploaded = await ingest_png(service, png_bytes)
    transport = httpx.MockTransport(provider_handler(llm_content="invalid json"))
    async with httpx.AsyncClient(transport=transport) as client:
        worker = Worker(settings, session_factory, storage, client)
        await worker.process_next()
    repository = QueueRepository(session_factory, 30)
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None and task.status == TaskStatus.FAILED and task.retry_count == 1
    assert await repository.retry_failed(uploaded.task_id)
    assert not await repository.retry_failed(uploaded.task_id)


async def test_retry_limit_exhausted(
    settings: Settings,
    service,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    uploaded = await ingest_png(service, png_bytes)
    transport = httpx.MockTransport(provider_handler(ocr_status=503))
    async with httpx.AsyncClient(transport=transport) as client:
        worker = Worker(settings, session_factory, storage, client)
        for _ in range(settings.max_retries):
            assert await worker.process_next()
        assert not await worker.process_next()
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == settings.max_retries


async def test_stale_lease_exhausts_attempts(
    service, session_factory: async_sessionmaker[AsyncSession], png_bytes: bytes
) -> None:
    uploaded = await ingest_png(service, png_bytes)
    async with session_factory.begin() as session:
        await session.execute(
            update(QueueTask)
            .where(QueueTask.id == uploaded.task_id)
            .values(
                status=TaskStatus.OCR,
                retry_count=2,
                max_retries=3,
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
    repository = QueueRepository(session_factory, 30)
    assert await repository.claim(uuid.uuid4()) is None
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 3
