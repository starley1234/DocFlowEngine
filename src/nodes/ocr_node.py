"""Вызов внешнего OCR-сервиса. Контракт ответа: {"text": "...", "confidence": 0.97}."""

import json
import math

import anyio
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from src.errors import PermanentPipelineError, ProviderError
from src.nodes.base import BaseNode
from src.pipeline.context import PipelineContext
from src.providers.http import request_json


class OCRParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_url: HttpUrl | None = None
    language: str = Field(default="rus+eng", pattern=r"^[a-z_]{3,20}(\+[a-z_]{3,20}){0,3}$")


class OCRNode(BaseNode):
    async def process(self, context: PipelineContext) -> PipelineContext:
        params = OCRParams.model_validate(self.params)
        if not context.images:
            raise PermanentPipelineError("OCR получил документ без страниц")
        url = str(params.service_url or self.settings.ocr_url)
        pages: list[dict[str, object]] = []
        context.page_texts = []
        for index, image_path in enumerate(context.images, start=1):
            async with await anyio.open_file(image_path, "rb") as image:
                content = await image.read()
            payload = await request_json(
                self.http_client,
                "POST",
                url,
                max_response_bytes=self.settings.provider_max_response_bytes,
                files={"file": (image_path.name, content, "image/png")},
                data={"language": params.language},
            )
            text = payload.get("text")
            if not isinstance(text, str):
                raise ProviderError("OCR вернул ответ без строкового поля text", retryable=False)
            confidence = payload.get("confidence")
            confidence_value: float | None = None
            if (
                isinstance(confidence, (int, float))
                and not isinstance(confidence, bool)
                and math.isfinite(confidence)
            ):
                confidence_value = float(confidence)
            context.page_texts.append(text)
            pages.append({"page": index, "text": text, "confidence": confidence_value})
        context.ocr_text = "\n\n".join(
            f"[Страница {index}]\n{text}" for index, text in enumerate(context.page_texts, start=1)
        )
        await self._write_artifact(context, pages)
        if not context.ocr_text.strip() or not any(text.strip() for text in context.page_texts):
            raise PermanentPipelineError("OCR не распознал текст")
        return context

    async def _write_artifact(
        self, context: PipelineContext, pages: list[dict[str, object]]
    ) -> None:
        async with await anyio.open_file(
            context.artifact_dir / "ocr.json", "w", encoding="utf-8"
        ) as output:
            await output.write(json.dumps({"pages": pages}, ensure_ascii=False, indent=2))
