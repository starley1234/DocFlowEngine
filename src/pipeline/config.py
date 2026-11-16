"""Загрузка JSON-конфигураций пайплайнов без возможности path traversal."""

import json
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from src.errors import PermanentPipelineError
from src.models.schemas import SCHEMA_MODELS

PIPELINE_NAME = re.compile(r"[a-z][a-z0-9_]{0,99}")
NodeName = Literal["pdf_converter", "ocr_node", "llm_extractor", "vector_node"]


class NodeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_name: NodeName
    params: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    version: str = Field(min_length=1, max_length=20)
    schema_model: str
    nodes: list[NodeConfig] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def validate_pipeline(self) -> "PipelineConfig":
        if self.schema_model not in SCHEMA_MODELS:
            raise ValueError(f"unknown schema_model: {self.schema_model}")
        names = [node.node_name for node in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError("duplicate nodes are not allowed")
        return self


def load_pipeline(name: str, directory: Path) -> PipelineConfig:
    if not PIPELINE_NAME.fullmatch(name):
        raise PermanentPipelineError("Некорректное имя пайплайна")
    root = directory.resolve()
    path = (root / f"{name}.json").resolve()
    if not path.is_relative_to(root):
        raise PermanentPipelineError("Некорректный путь пайплайна")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        config = PipelineConfig.model_validate(data)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise PermanentPipelineError(f"Ошибка конфигурации пайплайна {name}: {exc}") from exc
    if config.pipeline_name != name:
        raise PermanentPipelineError("Имя пайплайна не совпадает с именем файла")
    return config
