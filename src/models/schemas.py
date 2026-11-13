"""Pydantic-контракты API и результат извлечения реквизитов."""

import uuid
from datetime import date, datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from src.models.db import TaskStatus


class IncomingLetterSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    sender_org: str | None = Field(
        default=None, max_length=500, description="Организация-отправитель"
    )
    recipient_org: str | None = Field(
        default=None, max_length=500, description="Организация-получатель"
    )
    sender_person: str | None = Field(
        default=None, max_length=500, description="ФИО и должность отправителя"
    )
    recipient_person: str | None = Field(
        default=None, max_length=500, description="ФИО и должность адресата"
    )
    incoming_number: str | None = Field(
        default=None, max_length=100, description="Входящий регистрационный номер, если указан"
    )
    outgoing_number: str | None = Field(
        default=None, max_length=100, description="Исходящий номер отправителя"
    )
    deadline: date | None = Field(
        default=None, description="Срок исполнения документа, если указан"
    )
    tags: list[Annotated[str, Field(max_length=100)]] = Field(
        default_factory=list, max_length=30, description="Теги документа"
    )
    summary: str = Field(
        min_length=1, max_length=4000, description="Краткое содержание письма в 2-3 предложениях"
    )


SCHEMA_MODELS: dict[str, type[BaseModel]] = {"IncomingLetterSchema": IncomingLetterSchema}


class UploadResponse(BaseModel):
    document_id: uuid.UUID
    task_id: uuid.UUID
    status: TaskStatus
    duplicate: bool = False


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    document_id: uuid.UUID
    pipeline_name: str
    status: TaskStatus
    retry_count: int
    max_retries: int
    error_message: str | None
    available_at: datetime
    created_at: datetime
    updated_at: datetime


class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    file_name: str
    file_hash: str
    file_path: str
    source_type: str
    pipeline_name: str
    extracted_data: dict[str, Any] | None
    raw_ocr_text: str | None = None
    created_at: datetime
    updated_at: datetime


class ScanError(BaseModel):
    file_name: str
    error: str


class ScanFolderResponse(BaseModel):
    scanned: int
    created: int
    duplicates: int
    results: list[UploadResponse]
    errors: list[ScanError]


class SearchRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    query: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=10, ge=1, le=50)


class SearchHit(BaseModel):
    document_id: uuid.UUID
    chunk_index: int
    content: str
    score: float
