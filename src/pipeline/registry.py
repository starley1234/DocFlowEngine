"""Реестр известных узлов и проверка параметров до постановки документа в очередь."""

from dataclasses import dataclass

import httpx
from pydantic import BaseModel, ValidationError

from config.settings import Settings
from src.errors import PermanentPipelineError
from src.models.db import TaskStatus
from src.nodes.base import BaseNode
from src.nodes.llm_extractor import LLMExtractorNode, LLMExtractorParams
from src.nodes.ocr_node import OCRNode, OCRParams
from src.nodes.pdf_converter import PdfConverterNode, PdfConverterParams
from src.nodes.vector_node import VectorNode, VectorParams
from src.pipeline.config import PipelineConfig


@dataclass(frozen=True)
class NodeDefinition:
    node_class: type[BaseNode]
    params_model: type[BaseModel]
    status: TaskStatus


NODE_DEFINITIONS: dict[str, NodeDefinition] = {
    "pdf_converter": NodeDefinition(PdfConverterNode, PdfConverterParams, TaskStatus.CONVERTING),
    "ocr_node": NodeDefinition(OCRNode, OCRParams, TaskStatus.OCR),
    "llm_extractor": NodeDefinition(LLMExtractorNode, LLMExtractorParams, TaskStatus.EXTRACTING),
    "vector_node": NodeDefinition(VectorNode, VectorParams, TaskStatus.INDEXING),
}


@dataclass(frozen=True)
class RunnableNode:
    node: BaseNode
    status: TaskStatus


class PipelineRegistry:
    def __init__(self, settings: Settings, http_client: httpx.AsyncClient) -> None:
        self.settings = settings
        self.http_client = http_client

    @staticmethod
    def validate_config(config: PipelineConfig) -> None:
        try:
            for item in config.nodes:
                definition = NODE_DEFINITIONS[item.node_name]
                definition.params_model.model_validate(item.params)
        except ValidationError as exc:
            raise PermanentPipelineError(f"Некорректные параметры узлов: {exc}") from exc

    def build(self, config: PipelineConfig) -> list[RunnableNode]:
        self.validate_config(config)
        result: list[RunnableNode] = []
        for item in config.nodes:
            definition = NODE_DEFINITIONS[item.node_name]
            params = dict(item.params)
            result.append(
                RunnableNode(
                    definition.node_class(params, self.settings, self.http_client),
                    definition.status,
                )
            )
        return result
