"""Быстрые тесты без базы данных."""

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from config.settings import Settings
from src.errors import PermanentPipelineError, ProviderError, UnsupportedMediaTypeError
from src.models.schemas import IncomingLetterSchema
from src.nodes.llm_extractor import parse_llm_content
from src.pipeline.chunking import chunk_text
from src.pipeline.config import PipelineConfig, load_pipeline
from src.pipeline.registry import PipelineRegistry
from src.providers.embeddings import parse_embeddings
from src.storage import LocalStorage, safe_file_name


def test_chunking_overlap_and_progress() -> None:
    text = " ".join(f"word{i}" for i in range(300))
    chunks = chunk_text(text, 100, 20)
    assert len(chunks) > 1
    assert all(len(chunk) <= 100 for chunk in chunks)
    assert chunks[0][-20:] in chunks[1] or chunks[1].startswith(chunks[0][-20:])
    assert chunk_text("   ", 100, 20) == []
    with pytest.raises(ValueError):
        chunk_text("text", 100, 100)


def test_llm_json_validation() -> None:
    payload = {"summary": "Краткое содержание.", "tags": ["оплата"]}
    parsed = parse_llm_content(json.dumps(payload), "IncomingLetterSchema")
    assert parsed["sender_org"] is None
    fenced = f"```json\n{json.dumps(payload)}\n```"
    assert parse_llm_content(fenced, "IncomingLetterSchema")["summary"] == payload["summary"]
    with pytest.raises(ProviderError, match="схеме"):
        parse_llm_content('{"summary": "ok", "invented": true}', "IncomingLetterSchema")
    with pytest.raises(ProviderError):
        parse_llm_content("not json", "IncomingLetterSchema")
    with pytest.raises(ValidationError):
        IncomingLetterSchema.model_validate({"summary": "ok", "tags": [{}]})


def test_embedding_validation() -> None:
    payload = {"data": [{"index": 0, "embedding": [1.0, 0.0]}]}
    assert parse_embeddings(payload, 1, 2) == [[1.0, 0.0]]
    for bad in [
        {"data": [{"index": 1, "embedding": [1.0]}]},
        {"data": [{"index": 0, "embedding": [float("nan")]}]},
        {"data": [{"index": 0, "embedding": [0.0]}]},
        {"data": [{"index": 0, "embedding": [1.0]}, {"index": 0, "embedding": [1.0]}]},
    ]:
        with pytest.raises(ProviderError):
            parse_embeddings(bad, 1 if len(bad["data"]) == 1 else 2, 1)


def test_pipeline_config_and_registry(tmp_path: Path) -> None:
    config = load_pipeline("incoming_letter", Path("config/pipelines"))
    PipelineRegistry.validate_config(config)
    with pytest.raises(PermanentPipelineError):
        load_pipeline("../secret", tmp_path)
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(
            {
                "pipeline_name": "x",
                "version": "1",
                "schema_model": "IncomingLetterSchema",
                "nodes": [{"node_name": "unknown"}],
            }
        )
    bad = config.model_copy(deep=True)
    bad.nodes[0].params["dpi"] = 10
    with pytest.raises(PermanentPipelineError):
        PipelineRegistry.validate_config(bad)


async def test_storage_signatures_and_cleanup(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    storage.ensure_directories()
    assert safe_file_name("../../etc/passwd") == "passwd"
    assert safe_file_name("..\\..\\secret.pdf") == "secret.pdf"

    async def chunks():
        yield b"not a pdf"

    with pytest.raises(UnsupportedMediaTypeError):
        await storage.stage_stream("fake.pdf", chunks(), 100)
    assert list((tmp_path / "tmp").iterdir()) == []
    with pytest.raises(ValueError):
        storage.resolve("../outside")


def test_settings_safety_constraints() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, environment="production", api_key=None)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, worker_lease_seconds=30, worker_heartbeat_seconds=20)
    settings = Settings(_env_file=None, environment="production", api_key=SecretStr("x" * 16))
    assert settings.environment == "production"
