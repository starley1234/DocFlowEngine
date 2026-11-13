"""Общий HTTP-транспорт провайдеров с лимитом ответа и классификацией ошибок."""

import json
from typing import Any

import httpx
from pydantic import SecretStr

from src.errors import ProviderError


def bearer_headers(api_key: SecretStr | None) -> dict[str, str]:
    if api_key is None:
        return {}
    return {"Authorization": f"Bearer {api_key.get_secret_value()}"}


async def request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_response_bytes: int,
    headers: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
    files: dict[str, Any] | None = None,
    data: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        async with client.stream(
            method, url, headers=headers, json=json_body, files=files, data=data
        ) as response:
            if not response.is_success:
                status = response.status_code
                retryable = status in {408, 429} or status >= 500
                raise ProviderError(f"Провайдер вернул HTTP {status}", retryable=retryable)
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > max_response_bytes:
                    raise ProviderError("Ответ провайдера слишком большой", retryable=False)
    except httpx.HTTPError as exc:
        raise ProviderError(f"Сбой соединения с провайдером: {type(exc).__name__}") from exc

    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise ProviderError("Провайдер вернул не JSON", retryable=False) from exc
    if not isinstance(payload, dict):
        raise ProviderError("Провайдер вернул не JSON-объект", retryable=False)
    return payload
