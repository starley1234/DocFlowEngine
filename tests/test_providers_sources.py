"""OCR-адаптер, семантический поиск и OData-источник."""

import httpx
import pytest
from PIL import Image
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.queue.worker import Worker
from src.services.documents import DocumentService
from src.services.search import SearchService
from src.sources.odata import ODataSource
from src.storage import LocalStorage
from tests.helpers import ingest_png, provider_handler


async def test_ocr_service_contract(monkeypatch: pytest.MonkeyPatch, png_bytes: bytes) -> None:
    import ocr_service.main as ocr

    monkeypatch.setattr(ocr, "_ocr", lambda image, language: {"text": "Привет", "confidence": 0.9})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ocr.app), base_url="http://ocr"
    ) as client:
        response = await client.post(
            "/predict",
            files={"file": ("page.png", png_bytes, "image/png")},
            data={"language": "rus"},
        )
        assert response.status_code == 200
        assert response.json() == {"text": "Привет", "confidence": 0.9}
        bad = await client.post(
            "/predict",
            files={"file": ("page.png", png_bytes, "image/png")},
            data={"language": "bash"},
        )
        assert bad.status_code == 422


def test_ocr_line_reconstruction(monkeypatch: pytest.MonkeyPatch) -> None:
    import ocr_service.main as ocr

    data = {
        "text": ["Привет", "мир", "", "!", " "],
        "block_num": [1, 1, 1, 1, 1],
        "par_num": [1, 1, 1, 2, 2],
        "line_num": [1, 1, 1, 1, 1],
        "conf": [90, 80, -1, 70, -1],
    }
    monkeypatch.setattr(ocr.pytesseract, "image_to_data", lambda *args, **kwargs: data)
    result = ocr._ocr(Image.new("RGB", (10, 10)), "rus")
    assert result["text"] == "Привет мир\n!"
    assert result["confidence"] == pytest.approx(0.8)


async def test_semantic_search(
    settings: Settings,
    service: DocumentService,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    png_bytes: bytes,
) -> None:
    await ingest_png(service, png_bytes)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_handler())) as client:
        await Worker(settings, session_factory, storage, client).process_next()
        hits = await SearchService(session_factory, client, settings).search("оплата", 5)
    assert hits
    assert hits[0].score == pytest.approx(1.0)
    assert "оплатить" in hits[0].content


async def test_odata_source_origin_and_pagination(
    service: DocumentService, png_bytes: bytes
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"].startswith("Basic ")
        if request.url.path == "/feed":
            if request.url.params.get("page") == "2":
                return httpx.Response(
                    200,
                    json={
                        "value": [
                            {
                                "file_name": "two.png",
                                "content_url": "http://odata.test/files/two.png",
                            }
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "value": [
                        {"file_name": "one.png", "content_url": "http://odata.test/files/one.png"},
                        {"file_name": "bad.png", "content_url": "http://evil.test/steal.png"},
                    ],
                    "@odata.nextLink": "http://odata.test/feed?page=2",
                },
            )
        if request.url.path == "/files/one.png":
            return httpx.Response(200, content=png_bytes)
        if request.url.path == "/files/two.png":
            return httpx.Response(200, content=png_bytes + b"second")
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = ODataSource(
            service,
            client,
            "http://odata.test/feed",
            username="user",
            password=__import__("pydantic").SecretStr("pass"),
            max_documents=3,
        )
        batch = await source.collect("incoming_letter")
    assert len(batch.results) == 2
    assert len(batch.errors) == 1
    assert batch.errors[0].file_name == "bad.png"
