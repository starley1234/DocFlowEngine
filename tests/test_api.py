"""HTTP-контракты API."""

import uuid

import httpx
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.api.main import create_app
from src.queue.repository import QueueRepository


async def test_upload_dedup_cards_and_download(client: httpx.AsyncClient, png_bytes: bytes) -> None:
    files = {"file": ("letter.png", png_bytes, "image/png")}
    response = await client.post("/api/v1/documents/upload", files=files)
    assert response.status_code == 202
    created = response.json()
    duplicate = await client.post(
        "/api/v1/documents/upload", files={"file": ("copy.png", png_bytes, "image/png")}
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["document_id"] == created["document_id"]
    assert duplicate.json()["duplicate"] is True

    task = await client.get(f"/api/v1/tasks/{created['task_id']}")
    assert task.status_code == 200
    assert task.json()["status"] == "QUEUED"
    card = await client.get(f"/api/v1/documents/{created['document_id']}")
    assert card.status_code == 200
    assert card.json()["file_path"].startswith("raw/")
    assert card.json()["raw_ocr_text"] is None
    download = await client.get(f"/api/v1/documents/{created['document_id']}/download")
    assert download.status_code == 200
    assert download.content == png_bytes
    assert (await client.get(f"/api/v1/tasks/{uuid.uuid4()}")).status_code == 404


async def test_upload_validation(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/documents/upload", files={"file": ("virus.exe", b"MZ", "application/octet-stream")}
    )
    assert response.status_code == 415
    response = await client.post(
        "/api/v1/documents/upload", files={"file": ("fake.pdf", b"not pdf", "application/pdf")}
    )
    assert response.status_code == 415
    response = await client.post(
        "/api/v1/documents/upload",
        files={"file": ("big.pdf", b"%PDF-" + b"x" * 2000, "application/pdf")},
    )
    assert response.status_code == 413
    response = await client.post(
        "/api/v1/documents/upload",
        files={"file": ("x.png", b"\x89PNG\r\n\x1a\n", "image/png")},
        data={"pipeline_name": "../secret"},
    )
    assert response.status_code == 422


async def test_scan_folder(client: httpx.AsyncClient, settings: Settings, png_bytes: bytes) -> None:
    (settings.inbox_dir / "one.png").write_bytes(png_bytes)
    (settings.inbox_dir / "ignore.txt").write_text("skip")
    response = await client.post("/api/v1/scan-folder")
    assert response.status_code == 200
    assert response.json()["created"] == 1
    again = await client.post("/api/v1/scan-folder")
    assert again.json()["duplicates"] == 1


async def test_retry_endpoint(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    png_bytes: bytes,
) -> None:
    response = await client.post(
        "/api/v1/documents/upload", files={"file": ("x.png", png_bytes, "image/png")}
    )
    task_id = uuid.UUID(response.json()["task_id"])
    repository = QueueRepository(session_factory, 30)
    claim = await repository.claim(uuid.uuid4())
    assert claim is not None
    await repository.fail(claim, "bad data", retryable=False, base_delay=0, max_delay=0)
    retry = await client.post(f"/api/v1/tasks/{task_id}/retry")
    assert retry.status_code == 200
    assert retry.json()["status"] == "QUEUED"
    assert retry.json()["retry_count"] == 0
    assert (await client.post(f"/api/v1/tasks/{task_id}/retry")).status_code == 409


async def test_bearer_auth(settings: Settings, database, png_bytes: bytes) -> None:
    app = create_app(settings.model_copy(update={"api_key": SecretStr("test-secret")}))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        assert (await client.get("/health/live")).status_code == 200
        files = {"file": ("x.png", png_bytes, "image/png")}
        assert (await client.post("/api/v1/documents/upload", files=files)).status_code == 401
        assert (
            await client.post(
                "/api/v1/documents/upload",
                files=files,
                headers={"Authorization": "Bearer wrong"},
            )
        ).status_code == 401
        assert (
            await client.post(
                "/api/v1/documents/upload",
                files=files,
                headers={"Authorization": "Bearer test-secret"},
            )
        ).status_code == 202


async def test_health(client: httpx.AsyncClient) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).json() == {"status": "ready"}
