"""OCR-сервис: POST /predict (multipart file + language) -> text и confidence."""

import io
import os
from collections import OrderedDict
from typing import Annotated, Any

import anyio
import pytesseract
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from PIL import Image, UnidentifiedImageError
from pytesseract import Output

MAX_BYTES = int(os.getenv("OCR_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))
MAX_PIXELS = int(os.getenv("OCR_MAX_IMAGE_PIXELS", "40000000"))
TIMEOUT = int(os.getenv("OCR_TIMEOUT_SECONDS", "120"))
ALLOWED_LANGUAGES = set(os.getenv("OCR_ALLOWED_LANGUAGES", "rus,eng").split(","))
CONCURRENCY = int(os.getenv("OCR_CONCURRENCY", "2"))
_semaphore = anyio.Semaphore(CONCURRENCY)

app = FastAPI(title="DocFlow OCR", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _ocr(image: Image.Image, language: str) -> dict[str, Any]:
    data = pytesseract.image_to_data(
        image, lang=language, config="--psm 3", output_type=Output.DICT, timeout=TIMEOUT
    )
    lines: OrderedDict[tuple[int, int, int], list[str]] = OrderedDict()
    confidences: list[float] = []
    for index, word in enumerate(data["text"]):
        if not str(word).strip():
            continue
        key = (data["block_num"][index], data["par_num"][index], data["line_num"][index])
        lines.setdefault(key, []).append(str(word))
        confidence = float(data["conf"][index])
        if confidence >= 0:
            confidences.append(confidence)
    text = "\n".join(" ".join(words) for words in lines.values())
    mean_confidence = sum(confidences) / len(confidences) / 100 if confidences else None
    return {"text": text, "confidence": mean_confidence}


@app.post("/predict")
async def predict(
    file: Annotated[UploadFile, File()],
    language: Annotated[str, Form()] = "rus+eng",
) -> dict[str, Any]:
    if not language or any(part not in ALLOWED_LANGUAGES for part in language.split("+")):
        raise HTTPException(status_code=422, detail="Unsupported OCR language")
    try:
        content = await file.read(MAX_BYTES + 1)
        if len(content) > MAX_BYTES:
            raise HTTPException(status_code=413, detail="Image is too large")
        with Image.open(io.BytesIO(content)) as checked:
            width, height = checked.size
            if width * height > MAX_PIXELS:
                raise HTTPException(status_code=413, detail="Image has too many pixels")
            checked.verify()
        with Image.open(io.BytesIO(content)) as source:
            image = source.convert("RGB")
        try:
            async with _semaphore:
                return await anyio.to_thread.run_sync(_ocr, image, language)
        except pytesseract.TesseractNotFoundError as exc:
            raise HTTPException(status_code=503, detail="Tesseract is unavailable") from exc
        except pytesseract.TesseractError as exc:
            raise HTTPException(status_code=502, detail="OCR processing failed") from exc
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Invalid image") from exc
    finally:
        await file.close()
