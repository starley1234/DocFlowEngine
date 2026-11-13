"""Типизированный контекст, передаваемый между узлами пайплайна."""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class EmbeddingChunk:
    chunk_index: int
    content: str
    embedding: list[float]


@dataclass
class PipelineContext:
    document_id: uuid.UUID
    file_path: Path
    artifact_dir: Path
    schema_model: str
    images: list[Path] = field(default_factory=list)
    page_texts: list[str] = field(default_factory=list)
    ocr_text: str = ""
    extracted_data: dict[str, Any] = field(default_factory=dict)
    embeddings: list[EmbeddingChunk] = field(default_factory=list)
