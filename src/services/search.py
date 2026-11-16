"""Семантический поиск по чанкам документов (косинусная близость, pgvector HNSW)."""

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.models.db import DocumentEmbedding
from src.models.schemas import SearchHit
from src.providers.embeddings import request_embeddings


class SearchService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        client: httpx.AsyncClient,
        settings: Settings,
    ) -> None:
        self.session_factory = session_factory
        self.client = client
        self.settings = settings

    async def search(self, query: str, limit: int) -> list[SearchHit]:
        vector = (await request_embeddings(self.client, self.settings, [query]))[0]
        distance = DocumentEmbedding.embedding.cosine_distance(vector)
        async with self.session_factory() as session:
            rows = await session.execute(
                select(
                    DocumentEmbedding.document_id,
                    DocumentEmbedding.chunk_index,
                    DocumentEmbedding.content,
                    (1 - distance).label("score"),
                )
                .order_by(distance)
                .limit(limit)
            )
            return [
                SearchHit(
                    document_id=row.document_id,
                    chunk_index=row.chunk_index,
                    content=row.content,
                    score=float(row.score),
                )
                for row in rows
            ]
