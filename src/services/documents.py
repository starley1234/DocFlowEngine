"""Регистрация документа и задачи с дедупликацией по SHA-256."""

import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.errors import ConflictError
from src.models.db import Document, QueueTask, TaskStatus
from src.models.schemas import UploadResponse
from src.pipeline.config import load_pipeline
from src.pipeline.registry import PipelineRegistry
from src.queue.producer import enqueue
from src.storage import LocalStorage, StagedFile

SourceType = Literal["api", "folder", "odata"]


class DocumentService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        storage: LocalStorage,
        settings: Settings,
    ) -> None:
        self.session_factory = session_factory
        self.storage = storage
        self.settings = settings

    async def ingest_stream(
        self,
        file_name: str | None,
        chunks: AsyncIterator[bytes],
        source_type: SourceType,
        pipeline_name: str,
    ) -> UploadResponse:
        config = load_pipeline(pipeline_name, self.settings.pipeline_dir)
        PipelineRegistry.validate_config(config)
        staged = await self.storage.stage_stream(file_name, chunks, self.settings.max_upload_bytes)
        try:
            return await self._register(staged, source_type, pipeline_name)
        finally:
            await self.storage.discard(staged.path)

    async def ingest_path(
        self, path: Path, source_type: SourceType, pipeline_name: str
    ) -> UploadResponse:
        config = load_pipeline(pipeline_name, self.settings.pipeline_dir)
        PipelineRegistry.validate_config(config)
        staged = await self.storage.stage_path(path, self.settings.max_upload_bytes)
        try:
            return await self._register(staged, source_type, pipeline_name)
        finally:
            await self.storage.discard(staged.path)

    async def _register(
        self, staged: StagedFile, source_type: SourceType, pipeline_name: str
    ) -> UploadResponse:
        existing = await self._find_existing(staged.file_hash)
        if existing is not None:
            return existing

        document_id = uuid.uuid4()
        relative_path = await self.storage.promote(staged, document_id)
        try:
            async with self.session_factory.begin() as session:
                document = Document(
                    id=document_id,
                    file_name=staged.file_name,
                    file_hash=staged.file_hash,
                    file_path=relative_path,
                    source_type=source_type,
                    pipeline_name=pipeline_name,
                    extracted_data={},
                )
                session.add(document)
                task = await enqueue(session, document_id, pipeline_name, self.settings.max_retries)
                return UploadResponse(
                    document_id=document_id, task_id=task.id, status=TaskStatus.QUEUED
                )
        except IntegrityError as exc:
            # При гонке двух загрузок выигрывает UNIQUE(file_hash); лишний файл удаляем.
            await self.storage.discard_document_file(relative_path)
            existing = await self._find_existing(staged.file_hash)
            if existing is None:
                raise ConflictError("Не удалось зарегистрировать документ") from exc
            return existing

    async def _find_existing(self, file_hash: str) -> UploadResponse | None:
        async with self.session_factory() as session:
            document = await session.scalar(select(Document).where(Document.file_hash == file_hash))
            if document is None:
                return None
            task = await session.scalar(
                select(QueueTask).where(QueueTask.document_id == document.id)
            )
            if task is None:
                raise ConflictError("Документ существует без задачи обработки")
            return UploadResponse(
                document_id=document.id, task_id=task.id, status=task.status, duplicate=True
            )
