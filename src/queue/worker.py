"""Воркер очереди: один документ за раз, graceful shutdown через asyncio.Event."""

import asyncio
import logging
import uuid
from contextlib import suppress

import httpx
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.errors import LeaseLostError, PipelineError
from src.pipeline.runner import PipelineRunner
from src.queue.repository import QueueRepository, TaskClaim
from src.storage import LocalStorage

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        storage: LocalStorage,
        http_client: httpx.AsyncClient,
        worker_id: uuid.UUID | None = None,
    ) -> None:
        self.settings = settings
        self.worker_id = worker_id or uuid.uuid4()
        self.queue = QueueRepository(session_factory, settings.worker_lease_seconds)
        self.runner = PipelineRunner(settings, session_factory, storage, http_client)

    async def _heartbeat(self, claim: TaskClaim) -> None:
        while True:
            await asyncio.sleep(self.settings.worker_heartbeat_seconds)
            try:
                alive = await self.queue.heartbeat(claim)
            except SQLAlchemyError:
                # Временный сбой БД не отменяет работу сразу: lease ещё может быть жив.
                logger.warning("heartbeat failed for task_id=%s", claim.task_id, exc_info=True)
                continue
            if not alive:
                raise LeaseLostError

    async def process_next(self) -> bool:
        claim = await self.queue.claim(self.worker_id)
        if claim is None:
            return False
        runner_task = asyncio.create_task(self.runner.run(claim), name=f"task-{claim.task_id}")
        heartbeat_task = asyncio.create_task(self._heartbeat(claim), name=f"lease-{claim.task_id}")
        try:
            done, _ = await asyncio.wait(
                {runner_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat_task in done:
                runner_task.cancel()
                with suppress(asyncio.CancelledError):
                    await runner_task
                await heartbeat_task
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await runner_task
        except LeaseLostError:
            logger.warning("lease lost, result discarded task_id=%s", claim.task_id)
        except asyncio.CancelledError:
            runner_task.cancel()
            heartbeat_task.cancel()
            await asyncio.gather(runner_task, heartbeat_task, return_exceptions=True)
            raise
        except Exception as exc:
            retryable = exc.retryable if isinstance(exc, PipelineError) else True
            message = (
                str(exc)
                if isinstance(exc, PipelineError)
                else f"{type(exc).__name__}: внутренняя ошибка обработки"
            )
            recorded = await self.queue.fail(
                claim,
                message,
                retryable=retryable,
                base_delay=self.settings.retry_base_delay,
                max_delay=self.settings.retry_max_delay,
            )
            logger.warning(
                "task failed task_id=%s retryable=%s recorded=%s error=%s",
                claim.task_id,
                retryable,
                recorded,
                type(exc).__name__,
            )
        else:
            logger.info(
                "task completed task_id=%s document_id=%s", claim.task_id, claim.document_id
            )
        return True

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("worker started worker_id=%s", self.worker_id)
        while not stop.is_set():
            try:
                worked = await self.process_next()
            except Exception:
                logger.exception("worker iteration failed")
                worked = False
            if not worked:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self.settings.worker_poll_interval)
        logger.info("worker stopped worker_id=%s", self.worker_id)
