"""Постановка задачи в той же транзакции, что и создание документа."""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.db import QueueTask, TaskStatus


async def enqueue(
    session: AsyncSession,
    document_id: uuid.UUID,
    pipeline_name: str,
    max_retries: int,
) -> QueueTask:
    task = QueueTask(
        document_id=document_id,
        pipeline_name=pipeline_name,
        status=TaskStatus.QUEUED,
        max_retries=max_retries,
    )
    session.add(task)
    await session.flush()
    return task
