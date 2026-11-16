"""Контракт пакетного источника документов."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from src.models.schemas import ScanError, UploadResponse
from src.services.documents import DocumentService


@dataclass
class SourceBatch:
    results: list[UploadResponse] = field(default_factory=list)
    errors: list[ScanError] = field(default_factory=list)

    @property
    def scanned(self) -> int:
        return len(self.results) + len(self.errors)


class BaseSource(ABC):
    def __init__(self, service: DocumentService) -> None:
        self.service = service

    @abstractmethod
    async def collect(self, pipeline_name: str) -> SourceBatch:
        """Регистрирует найденные документы; ошибка одного файла не останавливает пакет."""
