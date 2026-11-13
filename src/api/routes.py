"""Маршруты API v1."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Query, Response, UploadFile
from fastapi.responses import FileResponse

from src.api.dependencies import get_runtime, require_api_key
from src.api.state import Runtime
from src.errors import ConflictError, NotFoundError
from src.models.db import Document, QueueTask
from src.models.schemas import (
    DocumentResponse,
    ScanFolderResponse,
    SearchHit,
    SearchRequest,
    TaskResponse,
    UploadResponse,
)
from src.pipeline.config import load_pipeline
from src.pipeline.registry import PipelineRegistry
from src.queue.repository import QueueRepository
from src.sources.api_upload import ApiUploadSource
from src.sources.folder import FolderSource

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
RuntimeDep = Annotated[Runtime, Depends(get_runtime)]


@router.post("/documents/upload", response_model=UploadResponse, status_code=202)
async def upload_document(
    runtime: RuntimeDep,
    response: Response,
    file: Annotated[UploadFile, File()],
    pipeline_name: Annotated[str | None, Form()] = None,
) -> UploadResponse:
    pipeline = pipeline_name or runtime.settings.default_pipeline
    result = await ApiUploadSource(runtime.documents).ingest(file, pipeline)
    response.status_code = 200 if result.duplicate else 202
    return result


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, runtime: RuntimeDep) -> QueueTask:
    async with runtime.database.session_factory() as session:
        task = await session.get(QueueTask, task_id)
    if task is None:
        raise NotFoundError("Задача не найдена")
    return task


@router.post("/tasks/{task_id}/retry", response_model=TaskResponse)
async def retry_task(task_id: uuid.UUID, runtime: RuntimeDep) -> QueueTask:
    async with runtime.database.session_factory() as session:
        if await session.get(QueueTask, task_id) is None:
            raise NotFoundError("Задача не найдена")
    repository = QueueRepository(
        runtime.database.session_factory, runtime.settings.worker_lease_seconds
    )
    if not await repository.retry_failed(task_id):
        raise ConflictError("Повтор доступен только для задач в статусе FAILED")
    return await get_task(task_id, runtime)


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: uuid.UUID,
    runtime: RuntimeDep,
    include_text: Annotated[bool, Query()] = False,
) -> DocumentResponse:
    async with runtime.database.session_factory() as session:
        document = await session.get(Document, document_id)
    if document is None:
        raise NotFoundError("Документ не найден")
    result = DocumentResponse.model_validate(document)
    if not include_text:
        result.raw_ocr_text = None
    return result


@router.get("/documents/{document_id}/download")
async def download_document(document_id: uuid.UUID, runtime: RuntimeDep) -> FileResponse:
    async with runtime.database.session_factory() as session:
        document = await session.get(Document, document_id)
    if document is None:
        raise NotFoundError("Документ не найден")
    path = runtime.storage.resolve(document.file_path)
    if not path.is_file():
        raise NotFoundError("Файл документа отсутствует в хранилище")
    media_types = {
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }
    return FileResponse(path, filename=document.file_name, media_type=media_types[path.suffix])


@router.post("/scan-folder", response_model=ScanFolderResponse)
async def scan_folder(
    runtime: RuntimeDep,
    pipeline_name: Annotated[str | None, Form()] = None,
) -> ScanFolderResponse:
    pipeline = pipeline_name or runtime.settings.default_pipeline
    config = load_pipeline(pipeline, runtime.settings.pipeline_dir)
    PipelineRegistry.validate_config(config)
    source = FolderSource(
        runtime.documents, runtime.settings.inbox_dir, runtime.settings.folder_scan_limit
    )
    batch = await source.collect(pipeline)
    return ScanFolderResponse(
        scanned=batch.scanned,
        created=sum(not item.duplicate for item in batch.results),
        duplicates=sum(item.duplicate for item in batch.results),
        results=batch.results,
        errors=batch.errors,
    )


@router.post("/search", response_model=list[SearchHit])
async def search(body: SearchRequest, runtime: RuntimeDep) -> list[SearchHit]:
    return await runtime.search.search(body.query.strip(), body.limit)
