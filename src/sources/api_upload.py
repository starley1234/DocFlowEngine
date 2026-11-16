"""Источник прямой HTTP-загрузки: файл читается потоково, без загрузки целиком в RAM."""

from collections.abc import AsyncIterator

from fastapi import UploadFile

from src.models.schemas import UploadResponse
from src.services.documents import DocumentService
from src.storage import CHUNK_BYTES


class ApiUploadSource:
    def __init__(self, service: DocumentService) -> None:
        self.service = service

    async def ingest(self, upload: UploadFile, pipeline_name: str) -> UploadResponse:
        async def chunks() -> AsyncIterator[bytes]:
            while chunk := await upload.read(CHUNK_BYTES):
                yield chunk

        try:
            return await self.service.ingest_stream(upload.filename, chunks(), "api", pipeline_name)
        finally:
            await upload.close()
