"""Локальное хранилище. Наружу отдаются только относительные пути."""

import hashlib
import json
import re
import shutil
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import anyio

from src.errors import PayloadTooLargeError, UnsupportedMediaTypeError

CHUNK_BYTES = 1024 * 1024
ALLOWED_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg"}
MAGIC_BY_SUFFIX = {
    ".pdf": b"%PDF-",
    ".png": b"\x89PNG\r\n\x1a\n",
    ".jpg": b"\xff\xd8\xff",
    ".jpeg": b"\xff\xd8\xff",
}


@dataclass(frozen=True)
class StagedFile:
    path: Path
    file_name: str
    file_hash: str
    size: int
    suffix: str


def safe_file_name(file_name: str | None) -> str:
    name = Path((file_name or "document").replace("\\", "/")).name
    name = re.sub(r"[\x00-\x1f\x7f]", "", name).strip()
    if name in {"", ".", ".."}:
        name = "document"
    suffix = Path(name).suffix
    if len(name) > 255:
        name = name[: 255 - len(suffix)] + suffix
    return name


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def ensure_directories(self) -> None:
        for name in ("raw", "artifacts", "inbox", "tmp"):
            (self.root / name).mkdir(parents=True, exist_ok=True)

    def resolve(self, relative_path: str | Path) -> Path:
        path = Path(relative_path)
        if path.is_absolute():
            raise ValueError("absolute paths are not allowed")
        resolved = (self.root / path).resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError("path escapes storage root")
        return resolved

    async def stage_stream(
        self, file_name: str | None, chunks: AsyncIterator[bytes], max_bytes: int
    ) -> StagedFile:
        """Пишет во временный файл, проверяет размер и сигнатуру, считает SHA-256."""
        name = safe_file_name(file_name)
        suffix = Path(name).suffix.lower()
        if suffix not in ALLOWED_SUFFIXES:
            raise UnsupportedMediaTypeError("Поддерживаются только PDF, PNG и JPEG")

        temporary = self.root / "tmp" / f"{uuid.uuid4()}.part"
        digest = hashlib.sha256()
        header = bytearray()
        size = 0
        try:
            async with await anyio.open_file(temporary, "wb") as output:
                async for chunk in chunks:
                    size += len(chunk)
                    if size > max_bytes:
                        raise PayloadTooLargeError(f"Файл превышает лимит {max_bytes} байт")
                    if len(header) < 8:
                        header.extend(chunk[: 8 - len(header)])
                    digest.update(chunk)
                    await output.write(chunk)
            if size == 0:
                raise UnsupportedMediaTypeError("Пустой файл")
            magic = MAGIC_BY_SUFFIX[suffix]
            if not bytes(header).startswith(magic):
                raise UnsupportedMediaTypeError("Содержимое не соответствует расширению файла")
            return StagedFile(temporary, name, digest.hexdigest(), size, suffix)
        except BaseException:
            await self.discard(temporary)
            raise

    async def stage_path(self, path: Path, max_bytes: int) -> StagedFile:
        async def chunks() -> AsyncIterator[bytes]:
            async with await anyio.open_file(path, "rb") as source:
                while chunk := await source.read(CHUNK_BYTES):
                    yield chunk

        return await self.stage_stream(path.name, chunks(), max_bytes)

    async def promote(self, staged: StagedFile, document_id: uuid.UUID) -> str:
        relative = Path("raw") / str(document_id) / f"{staged.file_hash}{staged.suffix}"
        destination = self.resolve(relative)
        await anyio.to_thread.run_sync(
            partial(destination.parent.mkdir, parents=True, exist_ok=True)
        )
        await anyio.to_thread.run_sync(staged.path.replace, destination)
        return relative.as_posix()

    async def discard(self, path: Path) -> None:
        await anyio.to_thread.run_sync(partial(path.unlink, missing_ok=True))

    async def discard_document_file(self, relative_path: str) -> None:
        """Удаляет лишний файл проигравшей гонки и её пустой каталог документа."""
        path = self.resolve(relative_path)
        await self.discard(path)
        raw_root = self.root / "raw"
        if path.parent != raw_root and path.parent.is_relative_to(raw_root):
            with suppress(OSError):
                await anyio.to_thread.run_sync(path.parent.rmdir)

    def artifact_relative(self, document_id: uuid.UUID, task_id: uuid.UUID, version: int) -> Path:
        return Path("artifacts") / str(document_id) / str(task_id) / f"attempt-{version}"

    async def prepare_artifacts(self, relative: Path) -> Path:
        path = self.resolve(relative)
        if not path.is_relative_to(self.root / "artifacts"):
            raise ValueError("artifacts must stay inside storage/artifacts")
        await anyio.to_thread.run_sync(partial(shutil.rmtree, path, ignore_errors=True))
        await anyio.to_thread.run_sync(partial(path.mkdir, parents=True, exist_ok=True))
        return path

    async def write_json(self, path: Path, data: Any) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        async with await anyio.open_file(path, "w", encoding="utf-8") as output:
            await output.write(payload)
