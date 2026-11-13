"""Контракт узла пайплайна. Узлы не управляют транзакциями и статусами очереди."""

from abc import ABC, abstractmethod
from typing import Any

import httpx

from config.settings import Settings
from src.pipeline.context import PipelineContext


class BaseNode(ABC):
    def __init__(
        self, params: dict[str, Any], settings: Settings, http_client: httpx.AsyncClient
    ) -> None:
        self.params = params
        self.settings = settings
        self.http_client = http_client

    @abstractmethod
    async def process(self, context: PipelineContext) -> PipelineContext:
        """Обновляет и возвращает типизированный контекст."""
