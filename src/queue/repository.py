"""Надёжная очередь: SKIP LOCKED, lease, heartbeat и fencing token."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from src.models.db import PROCESSING_STATUSES, Document, DocumentEmbedding, QueueTask, TaskStatus
from src.pipeline.context import EmbeddingChunk


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class TaskClaim:
    task_id: uuid.UUID
    document_id: uuid.UUID
    pipeline_name: str
    worker_id: uuid.UUID
    lock_version: int
    retry_count: int
    max_retries: int


class QueueRepository:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], lease_seconds: int
    ) -> None:
        self.session_factory = session_factory
        self.lease_seconds = lease_seconds

    async def claim(self, worker_id: uuid.UUID) -> TaskClaim | None:
        """Захватывает одну задачу; параллельные воркеры пропускают заблокированные строки."""
        async with self.session_factory.begin() as session:
            while True:
                now = utc_now()
                statement = (
                    select(QueueTask)
                    .where(
                        or_(
                            (QueueTask.status == TaskStatus.QUEUED)
                            & (QueueTask.available_at <= now)
                            & (QueueTask.retry_count < QueueTask.max_retries),
                            QueueTask.status.in_(PROCESSING_STATUSES)
                            & (
                                (QueueTask.lease_expires_at <= now)
                                | QueueTask.lease_expires_at.is_(None)
                            ),
                        )
                    )
                    .order_by(QueueTask.created_at, QueueTask.id)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
                task = await session.scalar(statement)
                if task is None:
                    return None
                if task.status in PROCESSING_STATUSES:
                    # Воркер мог упасть. Такая попытка расходует лимит попыток.
                    task.retry_count += 1
                    if task.retry_count >= task.max_retries:
                        task.status = TaskStatus.FAILED
                        task.error_message = "Истёк lease после падения воркера"
                        task.worker_id = None
                        task.locked_at = None
                        task.lease_expires_at = None
                        task.updated_at = now
                        continue
                task.status = TaskStatus.CONVERTING
                task.lock_version += 1
                task.worker_id = worker_id
                task.locked_at = now
                task.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
                task.updated_at = now
                task.error_message = None
                return TaskClaim(
                    task.id,
                    task.document_id,
                    task.pipeline_name,
                    worker_id,
                    task.lock_version,
                    task.retry_count,
                    task.max_retries,
                )

    @staticmethod
    def _fence(claim: TaskClaim, now: datetime) -> tuple[ColumnElement[bool], ...]:
        return (
            QueueTask.id == claim.task_id,
            QueueTask.worker_id == claim.worker_id,
            QueueTask.lock_version == claim.lock_version,
            QueueTask.lease_expires_at > now,
            QueueTask.status.in_(PROCESSING_STATUSES),
        )

    async def update_status(self, claim: TaskClaim, status: TaskStatus) -> bool:
        now = utc_now()
        async with self.session_factory.begin() as session:
            result = await session.scalar(
                update(QueueTask)
                .where(*self._fence(claim, now))
                .values(status=status, updated_at=now)
                .returning(QueueTask.id)
            )
            return result is not None

    async def heartbeat(self, claim: TaskClaim) -> bool:
        now = utc_now()
        async with self.session_factory.begin() as session:
            result = await session.scalar(
                update(QueueTask)
                .where(*self._fence(claim, now))
                .values(
                    lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                    updated_at=now,
                )
                .returning(QueueTask.id)
            )
            return result is not None

    async def complete(
        self,
        claim: TaskClaim,
        raw_ocr_text: str,
        extracted_data: dict[str, object],
        embeddings: list[EmbeddingChunk],
    ) -> bool:
        """Результат, векторы и COMPLETED фиксируются одной транзакцией."""
        now = utc_now()
        async with self.session_factory.begin() as session:
            task_id = await session.scalar(
                update(QueueTask)
                .where(*self._fence(claim, now))
                .values(
                    status=TaskStatus.COMPLETED,
                    error_message=None,
                    worker_id=None,
                    locked_at=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
                .returning(QueueTask.id)
            )
            if task_id is None:
                return False
            await session.execute(
                update(Document)
                .where(Document.id == claim.document_id)
                .values(
                    raw_ocr_text=raw_ocr_text,
                    extracted_data=extracted_data,
                    updated_at=now,
                )
            )
            await session.execute(
                delete(DocumentEmbedding).where(DocumentEmbedding.document_id == claim.document_id)
            )
            if embeddings:
                await session.execute(
                    insert(DocumentEmbedding),
                    [
                        {
                            "document_id": claim.document_id,
                            "chunk_index": chunk.chunk_index,
                            "content": chunk.content,
                            "embedding": chunk.embedding,
                        }
                        for chunk in embeddings
                    ],
                )
            return True

    async def fail(
        self,
        claim: TaskClaim,
        error_message: str,
        *,
        retryable: bool,
        base_delay: float,
        max_delay: float,
    ) -> bool:
        now = utc_now()
        retry_count = claim.retry_count + 1
        will_retry = retryable and retry_count < claim.max_retries
        delay = min(base_delay * (2 ** max(retry_count - 1, 0)), max_delay)
        async with self.session_factory.begin() as session:
            task_id = await session.scalar(
                update(QueueTask)
                .where(*self._fence(claim, now))
                .values(
                    status=TaskStatus.QUEUED if will_retry else TaskStatus.FAILED,
                    retry_count=retry_count,
                    error_message=error_message[:2000],
                    available_at=now + timedelta(seconds=delay) if will_retry else now,
                    worker_id=None,
                    locked_at=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
                .returning(QueueTask.id)
            )
            return task_id is not None

    async def retry_failed(self, task_id: uuid.UUID) -> bool:
        """Ручной повтор только терминально упавшей задачи."""
        now = utc_now()
        async with self.session_factory.begin() as session:
            result = await session.scalar(
                update(QueueTask)
                .where(QueueTask.id == task_id, QueueTask.status == TaskStatus.FAILED)
                .values(
                    status=TaskStatus.QUEUED,
                    retry_count=0,
                    error_message=None,
                    available_at=now,
                    worker_id=None,
                    locked_at=None,
                    lease_expires_at=None,
                    lock_version=QueueTask.lock_version + 1,
                    updated_at=now,
                )
                .returning(QueueTask.id)
            )
            return result is not None
