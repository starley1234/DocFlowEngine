"""Нарезка текста и получение эмбеддингов. Запись в БД атомарно выполняет runner."""

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.errors import ProviderError
from src.nodes.base import BaseNode
from src.pipeline.chunking import chunk_text
from src.pipeline.context import EmbeddingChunk, PipelineContext
from src.providers.embeddings import request_embeddings


class VectorParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chunk_size: int = Field(default=1000, ge=100, le=10_000)
    overlap: int = Field(default=100, ge=0)
    batch_size: int = Field(default=32, ge=1, le=128)
    model: str | None = None

    @model_validator(mode="after")
    def validate_overlap(self) -> "VectorParams":
        if self.overlap >= self.chunk_size:
            raise ValueError("overlap must be smaller than chunk_size")
        return self


class VectorNode(BaseNode):
    async def process(self, context: PipelineContext) -> PipelineContext:
        params = VectorParams.model_validate(self.params)
        chunks = chunk_text(context.ocr_text, params.chunk_size, params.overlap)
        if not chunks:
            raise ProviderError("Нет текста для индексации", retryable=False)
        context.embeddings = []
        for offset in range(0, len(chunks), params.batch_size):
            batch = chunks[offset : offset + params.batch_size]
            vectors = await request_embeddings(self.http_client, self.settings, batch, params.model)
            context.embeddings.extend(
                EmbeddingChunk(offset + index, content, vector)
                for index, (content, vector) in enumerate(zip(batch, vectors, strict=True))
            )
        return context
