"""Сканер настроенной локальной папки. Симлинки наружу не допускаются."""

from pathlib import Path

import anyio

from src.errors import DocFlowError
from src.models.schemas import ScanError
from src.services.documents import DocumentService
from src.sources.base import BaseSource, SourceBatch
from src.storage import ALLOWED_SUFFIXES


class FolderSource(BaseSource):
    def __init__(
        self, service: DocumentService, root: Path, limit: int, recursive: bool = False
    ) -> None:
        super().__init__(service)
        self.root = root.resolve()
        self.limit = limit
        self.recursive = recursive

    def _candidates(self) -> list[Path]:
        if not self.root.is_dir():
            return []
        iterator = self.root.rglob("*") if self.recursive else self.root.glob("*")
        result: list[Path] = []
        for path in sorted(iterator):
            if path.is_symlink() or not path.is_file():
                continue
            if path.suffix.lower() not in ALLOWED_SUFFIXES:
                continue
            if not path.resolve().is_relative_to(self.root):
                continue
            result.append(path)
            if len(result) >= self.limit:
                break
        return result

    async def collect(self, pipeline_name: str) -> SourceBatch:
        batch = SourceBatch()
        candidates = await anyio.to_thread.run_sync(self._candidates)
        for path in candidates:
            try:
                batch.results.append(await self.service.ingest_path(path, "folder", pipeline_name))
            except (DocFlowError, OSError, ValueError) as exc:
                batch.errors.append(ScanError(file_name=path.name, error=str(exc)[:500]))
        return batch
