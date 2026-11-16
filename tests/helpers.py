"""Общие помощники тестов."""

import json
from collections.abc import AsyncIterator, Callable

import httpx

from src.models.schemas import UploadResponse
from src.services.documents import DocumentService


async def bytes_iter(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def ingest_png(
    service: DocumentService, data: bytes, name: str = "letter.png"
) -> UploadResponse:
    return await service.ingest_stream(name, bytes_iter(data), "api", "incoming_letter")


def provider_handler(
    *, ocr_status: int = 200, llm_content: str | None = None
) -> Callable[[httpx.Request], httpx.Response]:
    letter = {
        "sender_org": "ООО Ромашка",
        "recipient_org": "ООО Василёк",
        "sender_person": None,
        "recipient_person": None,
        "incoming_number": None,
        "outgoing_number": "12/3",
        "deadline": None,
        "tags": ["оплата"],
        "summary": "Письмо об оплате счёта.",
    }

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/predict":
            if ocr_status != 200:
                return httpx.Response(ocr_status, json={"detail": "temporary failure"})
            return httpx.Response(
                200, json={"text": "ООО Ромашка. Просим оплатить счёт.", "confidence": 0.97}
            )
        if request.url.path.endswith("/chat/completions"):
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": llm_content or json.dumps(letter, ensure_ascii=False)
                            },
                            "finish_reason": "stop",
                        }
                    ]
                },
            )
        if request.url.path.endswith("/embeddings"):
            body = json.loads(request.content)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"index": index, "embedding": [1.0] + [0.0] * 1023}
                        for index, _ in enumerate(body["input"])
                    ]
                },
            )
        raise AssertionError(f"unexpected provider request: {request.url}")

    return handle
