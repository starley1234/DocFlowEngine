"""PDF/изображение -> PNG. Poppler вызывается в отдельном потоке, не блокируя event loop."""

import subprocess
from pathlib import Path
from typing import cast

import anyio
from pdf2image import convert_from_path, pdfinfo_from_path
from pdf2image.exceptions import PDFInfoNotInstalledError, PDFPageCountError, PDFSyntaxError
from PIL import Image, ImageOps, UnidentifiedImageError
from pydantic import BaseModel, ConfigDict, Field

from src.errors import PermanentPipelineError
from src.nodes.base import BaseNode
from src.pipeline.context import PipelineContext


class PdfConverterParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dpi: int = Field(default=200, ge=72, le=300)
    first_page_only: bool = False


class PdfConverterNode(BaseNode):
    async def process(self, context: PipelineContext) -> PipelineContext:
        params = PdfConverterParams.model_validate(self.params)
        try:
            if context.file_path.suffix.lower() == ".pdf":
                images = await anyio.to_thread.run_sync(self._convert_pdf, context, params)
            else:
                images = await anyio.to_thread.run_sync(self._convert_image, context)
        except (
            PDFInfoNotInstalledError,
            PDFPageCountError,
            PDFSyntaxError,
            UnidentifiedImageError,
            Image.DecompressionBombError,
            subprocess.TimeoutExpired,
            OSError,
            ValueError,
        ) as exc:
            raise PermanentPipelineError(f"Не удалось подготовить страницы: {exc}") from exc
        if not images:
            raise PermanentPipelineError("Документ не содержит страниц")
        context.images = images
        return context

    def _convert_pdf(self, context: PipelineContext, params: PdfConverterParams) -> list[Path]:
        info = pdfinfo_from_path(str(context.file_path), timeout=self.settings.conversion_timeout)
        pages = int(info["Pages"])
        if pages < 1 or pages > self.settings.max_pdf_pages:
            raise PermanentPipelineError(
                f"Число страниц вне лимита 1..{self.settings.max_pdf_pages}"
            )
        last_page = 1 if params.first_page_only else pages
        paths = convert_from_path(
            str(context.file_path),
            dpi=params.dpi,
            first_page=1,
            last_page=last_page,
            fmt="png",
            output_folder=str(context.artifact_dir),
            output_file="page",
            paths_only=True,
            thread_count=1,
            timeout=self.settings.conversion_timeout,
        )
        images = sorted(Path(path) for path in cast(list[str], paths))
        if len(images) != last_page:
            raise PermanentPipelineError("Конвертер вернул неполный набор страниц")
        return images

    def _convert_image(self, context: PipelineContext) -> list[Path]:
        with Image.open(context.file_path) as source:
            width, height = source.size
            if width * height > self.settings.max_image_pixels:
                raise PermanentPipelineError("Изображение превышает лимит пикселей")
            source.load()
            image = ImageOps.exif_transpose(source).convert("RGB")
        destination = context.artifact_dir / "page-0001.png"
        image.save(destination, format="PNG")
        return [destination]
