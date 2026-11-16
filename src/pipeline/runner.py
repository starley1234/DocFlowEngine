"""Исполнение пайплайна. Финальный результат коммитится только после всех узлов."""

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.errors import LeaseLostError, PermanentPipelineError
from src.models.db import Document
from src.pipeline.config import load_pipeline
from src.pipeline.context import PipelineContext
from src.pipeline.registry import PipelineRegistry
from src.queue.repository import QueueRepository, TaskClaim
from src.storage import LocalStorage


class PipelineRunner:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        storage: LocalStorage,
        http_client: httpx.AsyncClient,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.storage = storage
        self.registry = PipelineRegistry(settings, http_client)
        self.queue = QueueRepository(session_factory, settings.worker_lease_seconds)

    async def run(self, claim: TaskClaim) -> None:
        async with self.session_factory() as session:
            document = await session.get(Document, claim.document_id)
        if document is None:
            raise PermanentPipelineError("Документ задачи не найден")
        config = load_pipeline(claim.pipeline_name, self.settings.pipeline_dir)
        nodes = self.registry.build(config)
        artifact_dir = await self.storage.prepare_artifacts(
            self.storage.artifact_relative(claim.document_id, claim.task_id, claim.lock_version)
        )
        context = PipelineContext(
            document_id=claim.document_id,
            file_path=self.storage.resolve(document.file_path),
            artifact_dir=artifact_dir,
            schema_model=config.schema_model,
        )
        for runnable in nodes:
            if not await self.queue.update_status(claim, runnable.status):
                raise LeaseLostError
            context = await runnable.node.process(context)
        completed = await self.queue.complete(
            claim, context.ocr_text, context.extracted_data, context.embeddings
        )
        if not completed:
            raise LeaseLostError
