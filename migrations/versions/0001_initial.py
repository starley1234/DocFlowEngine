"""Начальная схема: документы, очередь и векторные чанки."""

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

TASK_STATUS = postgresql.ENUM(
    "QUEUED",
    "CONVERTING",
    "OCR",
    "EXTRACTING",
    "INDEXING",
    "COMPLETED",
    "FAILED",
    name="task_status",
)


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "documents",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column("file_name", sa.String(255), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(50), nullable=False),
        sa.Column("pipeline_name", sa.String(100), nullable=False),
        sa.Column("extracted_data", postgresql.JSONB(), nullable=True),
        sa.Column("raw_ocr_text", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("source_type IN ('api', 'folder', 'odata')", name="ck_documents_source"),
        sa.CheckConstraint("char_length(file_hash) = 64", name="ck_documents_hash_length"),
    )
    op.create_index("ix_documents_file_hash", "documents", ["file_hash"], unique=True)
    op.create_table(
        "task_queue",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "document_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("pipeline_name", sa.String(100), nullable=False),
        sa.Column("status", TASK_STATUS, server_default="QUEUED", nullable=False),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_retries", sa.Integer(), server_default="3", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lock_version", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("document_id", name="uq_task_queue_document_id"),
        sa.CheckConstraint("retry_count >= 0", name="ck_task_retry_count"),
        sa.CheckConstraint("max_retries >= 1", name="ck_task_max_retries"),
        sa.CheckConstraint("retry_count <= max_retries", name="ck_task_retry_limit"),
        sa.CheckConstraint("lock_version >= 0", name="ck_task_lock_version"),
    )
    op.create_index(
        "idx_task_queue_fetch",
        "task_queue",
        ["status", "available_at", "created_at"],
        postgresql_where=sa.text("status = 'QUEUED'"),
    )
    op.create_index(
        "idx_task_queue_lease",
        "task_queue",
        ["lease_expires_at"],
        postgresql_where=sa.text("status IN ('CONVERTING', 'OCR', 'EXTRACTING', 'INDEXING')"),
    )
    op.create_table(
        "document_embeddings",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "document_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(1024), nullable=False),
        sa.UniqueConstraint("document_id", "chunk_index", name="uq_embedding_document_chunk"),
        sa.CheckConstraint("chunk_index >= 0", name="ck_embedding_chunk_index"),
    )
    op.create_index(
        "idx_document_embeddings_vector",
        "document_embeddings",
        ["embedding"],
        postgresql_using="hnsw",
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )


def downgrade() -> None:
    op.drop_index("idx_document_embeddings_vector", table_name="document_embeddings")
    op.drop_table("document_embeddings")
    op.drop_index("idx_task_queue_lease", table_name="task_queue")
    op.drop_index("idx_task_queue_fetch", table_name="task_queue")
    op.drop_table("task_queue")
    op.drop_index("ix_documents_file_hash", table_name="documents")
    op.drop_table("documents")
    TASK_STATUS.drop(op.get_bind(), checkfirst=True)
    # Расширения не удаляем: ими могут пользоваться другие объекты базы.
