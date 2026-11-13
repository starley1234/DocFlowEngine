"""OpenAI-совместимый endpoint /embeddings и строгая проверка векторов."""

import math
from typing import Any

import httpx

from config.settings import Settings
from src.errors import ProviderError
from src.providers.http import bearer_headers, request_json


def parse_embeddings(payload: dict[str, Any], expected: int, dimensions: int) -> list[list[float]]:
    data = payload.get("data")
    if not isinstance(data, list) or len(data) != expected:
        raise ProviderError("Embedding-сервис вернул неверное число векторов", retryable=False)
    result: list[list[float] | None] = [None] * expected
    for item in data:
        if not isinstance(item, dict):
            raise ProviderError("Некорректный элемент ответа embeddings", retryable=False)
        index = item.get("index")
        vector = item.get("embedding")
        if type(index) is not int or not 0 <= index < expected or result[index] is not None:
            raise ProviderError("Некорректный индекс вектора", retryable=False)
        if not isinstance(vector, list) or len(vector) != dimensions:
            raise ProviderError("Неверная размерность эмбеддинга", retryable=False)
        if any(type(value) not in {int, float} or not math.isfinite(value) for value in vector):
            raise ProviderError("Эмбеддинг содержит нечисловые значения", retryable=False)
        if not any(vector):
            raise ProviderError("Эмбеддинг не должен быть нулевым", retryable=False)
        result[index] = [float(value) for value in vector]
    return [vector for vector in result if vector is not None]


async def request_embeddings(
    client: httpx.AsyncClient, settings: Settings, texts: list[str], model: str | None = None
) -> list[list[float]]:
    payload = await request_json(
        client,
        "POST",
        f"{settings.resolved_embedding_base_url}/embeddings",
        max_response_bytes=settings.provider_max_response_bytes,
        headers=bearer_headers(settings.resolved_embedding_api_key),
        json_body={"model": model or settings.embedding_model, "input": texts},
    )
    return parse_embeddings(payload, len(texts), settings.embedding_dimensions)
