"""Таблицы PostgreSQL. UUID и временные метки создаются в UTC."""

import enum
import uuid
from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from config.settings import EMBEDDING_DIMENSIONS


class Base(DeclarativeBase):
    pass


class TaskStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    CONVERTING = "CONVERTING"
    OCR = "OCR"
    EXTRACTING = "EXTRACTING"
    INDEXING = "INDEXING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


PROCESSING_STATUSES = (
    TaskStatus.CONVERTING,
    TaskStatus.OCR,
    TaskStatus.EXTRACTING,
    TaskStatus.INDEXING,
)


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    file_name: Mapped[str] = mapped_column(String(255))
    file_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    file_path: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(50))
    pipeline_name: Mapped[str] = mapped_column(String(100))
    extracted_data: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=dict)
    raw_ocr_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    task: Mapped["QueueTask"] = relationship(back_populates="document", uselist=False)
    embeddings: Mapped[list["DocumentEmbedding"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("source_type IN ('api', 'folder', 'odata')", name="ck_documents_source"),
        CheckConstraint("char_length(file_hash) = 64", name="ck_documents_hash_length"),
    )


class QueueTask(Base):
    __tablename__ = "task_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), unique=True
    )
    pipeline_name: Mapped[str] = mapped_column(String(100))
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status", values_callable=lambda item: [e.value for e in item]),
        default=TaskStatus.QUEUED,
        server_default=TaskStatus.QUEUED.value,
    )
    # Число неудачных попыток, включая восстановленные после падения воркера.
    retry_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_retries: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    error_message: Mapped[str | None] = mapped_column(Text)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    worker_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    # Fencing token защищает от записи результатом устаревшего воркера.
    lock_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    document: Mapped[Document] = relationship(back_populates="task")

    __table_args__ = (
        CheckConstraint("retry_count >= 0", name="ck_task_retry_count"),
        CheckConstraint("max_retries >= 1", name="ck_task_max_retries"),
        CheckConstraint("retry_count <= max_retries", name="ck_task_retry_limit"),
        CheckConstraint("lock_version >= 0", name="ck_task_lock_version"),
        Index(
            "idx_task_queue_fetch",
            "status",
            "available_at",
            "created_at",
            postgresql_where=text("status = 'QUEUED'"),
        ),
        Index(
            "idx_task_queue_lease",
            "lease_expires_at",
            postgresql_where=text("status IN ('CONVERTING', 'OCR', 'EXTRACTING', 'INDEXING')"),
        ),
    )


class DocumentEmbedding(Base):
    __tablename__ = "document_embeddings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    document_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"))
    chunk_index: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(Text)
    embedding: Mapped[list[float]] = mapped_column(Vector(EMBEDDING_DIMENSIONS))

    document: Mapped[Document] = relationship(back_populates="embeddings")

    __table_args__ = (
        UniqueConstraint("document_id", "chunk_index", name="uq_embedding_document_chunk"),
        CheckConstraint("chunk_index >= 0", name="ck_embedding_chunk_index"),
        Index(
            "idx_document_embeddings_vector",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )
