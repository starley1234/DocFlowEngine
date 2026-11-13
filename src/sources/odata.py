"""Узкий OData v4-адаптер: value[] с file_name и content_url, пагинация @odata.nextLink.

Это контракт для интеграции с 1С/шлюзом, а не универсальный клиент произвольных
сущностей 1С. Ссылки скачивания принимаются только с origin настроенного feed.
"""

import base64
from collections.abc import AsyncIterator
from typing import Any

import httpx
from pydantic import SecretStr

from src.errors import DocFlowError, ProviderError
from src.models.schemas import ScanError
from src.providers.http import request_json
from src.services.documents import DocumentService
from src.sources.base import BaseSource, SourceBatch


def basic_headers(username: str | None, password: SecretStr | None) -> dict[str, str]:
    if username is None or password is None:
        return {}
    token = base64.b64encode(f"{username}:{password.get_secret_value()}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


class ODataSource(BaseSource):
    def __init__(
        self,
        service: DocumentService,
        client: httpx.AsyncClient,
        feed_url: str,
        *,
        username: str | None = None,
        password: SecretStr | None = None,
        page_size: int = 50,
        max_documents: int = 1000,
        max_response_bytes: int = 5 * 1024 * 1024,
    ) -> None:
        super().__init__(service)
        self.client = client
        if httpx.URL(feed_url).userinfo:
            raise ValueError("credentials must be passed separately, not in feed URL")
        self.feed_url = feed_url
        self.origin = (
            httpx.URL(feed_url).scheme,
            httpx.URL(feed_url).host,
            httpx.URL(feed_url).port,
        )
        self.headers = basic_headers(username, password)
        self.page_size = page_size
        self.max_documents = max_documents
        self.max_response_bytes = max_response_bytes

    def _same_origin(self, url: str) -> bool:
        parsed = httpx.URL(url)
        return (
            parsed.scheme in {"http", "https"}
            and not parsed.userinfo
            and (parsed.scheme, parsed.host, parsed.port) == self.origin
        )

    async def collect(self, pipeline_name: str) -> SourceBatch:
        batch = SourceBatch()
        url: str | None = str(httpx.URL(self.feed_url).copy_add_param("$top", str(self.page_size)))
        seen_urls: set[str] = set()
        while url is not None and batch.scanned < self.max_documents:
            if url in seen_urls or not self._same_origin(url):
                raise ProviderError("Небезопасная ссылка пагинации OData", retryable=False)
            seen_urls.add(url)
            payload = await request_json(
                self.client,
                "GET",
                url,
                max_response_bytes=self.max_response_bytes,
                headers=self.headers,
            )
            entries = payload.get("value")
            if not isinstance(entries, list):
                raise ProviderError("OData feed не содержит value[]", retryable=False)
            for entry in entries:
                if batch.scanned >= self.max_documents:
                    break
                await self._ingest_entry(entry, pipeline_name, batch)
            next_link = payload.get("@odata.nextLink")
            url = str(next_link) if isinstance(next_link, str) else None
        return batch

    async def _ingest_entry(self, entry: Any, pipeline_name: str, batch: SourceBatch) -> None:
        name = entry.get("file_name") if isinstance(entry, dict) else None
        content_url = entry.get("content_url") if isinstance(entry, dict) else None
        if not isinstance(name, str) or not isinstance(content_url, str):
            batch.errors.append(ScanError(file_name="unknown", error="Нет file_name/content_url"))
            return
        try:
            if not self._same_origin(content_url):
                raise ProviderError("Ссылка вложения ведёт на другой origin", retryable=False)
            async with self.client.stream("GET", content_url, headers=self.headers) as response:
                if not response.is_success:
                    raise ProviderError(f"OData вложение вернуло HTTP {response.status_code}")

                async def chunks() -> AsyncIterator[bytes]:
                    async for chunk in response.aiter_bytes():
                        yield chunk

                result = await self.service.ingest_stream(name, chunks(), "odata", pipeline_name)
                batch.results.append(result)
        except (DocFlowError, ProviderError, httpx.HTTPError, OSError, ValueError) as exc:
            batch.errors.append(ScanError(file_name=name, error=str(exc)[:500]))
