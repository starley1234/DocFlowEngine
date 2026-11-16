"""Реальные системные бинарники: выполняются в CI, локально пропускаются при отсутствии."""

import shutil
from pathlib import Path

import httpx
import pytest
from PIL import Image, ImageDraw, ImageFont
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.models.db import QueueTask, TaskStatus
from src.queue.worker import Worker
from src.services.documents import DocumentService
from src.storage import LocalStorage
from tests.helpers import provider_handler


@pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="Poppler is not installed")
async def test_real_pdf_conversion_pipeline(
    settings: Settings,
    service: DocumentService,
    session_factory: async_sessionmaker[AsyncSession],
    storage: LocalStorage,
    tmp_path: Path,
) -> None:
    pdf_path = tmp_path / "sample.pdf"
    Image.new("RGB", (300, 200), "white").save(pdf_path, format="PDF")
    uploaded = await service.ingest_path(pdf_path, "folder", "incoming_letter")
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_handler())) as client:
        assert await Worker(settings, session_factory, storage, client).process_next()
    async with session_factory() as session:
        task = await session.get(QueueTask, uploaded.task_id)
    assert task is not None and task.status == TaskStatus.COMPLETED


@pytest.mark.skipif(shutil.which("tesseract") is None, reason="Tesseract is not installed")
def test_real_tesseract_ocr() -> None:
    import ocr_service.main as ocr

    image = Image.new("RGB", (600, 120), "white")
    draw = ImageDraw.Draw(image)
    draw.text((10, 30), "HELLO DOCFLOW", fill="black", font=ImageFont.load_default(size=40))
    result = ocr._ocr(image, "eng")
    assert "HELLO" in result["text"].upper()
    assert result["confidence"] is not None and result["confidence"] > 0.5
