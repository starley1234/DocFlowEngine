"""Фабрика FastAPI-приложения и управление ресурсами lifespan."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from config.settings import Settings, get_settings
from src import __version__
from src.api.routes import router
from src.api.state import Runtime
from src.database import Database
from src.errors import DocFlowError, PermanentPipelineError, ProviderError
from src.logging import configure_logging
from src.pipeline.config import load_pipeline
from src.pipeline.registry import PipelineRegistry
from src.services.documents import DocumentService
from src.services.search import SearchService
from src.storage import LocalStorage

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        storage = LocalStorage(settings.storage_root)
        storage.ensure_directories()
        config = load_pipeline(settings.default_pipeline, settings.pipeline_dir)
        PipelineRegistry.validate_config(config)
        if settings.api_key is None:
            logger.warning("API key is not configured; do not expose this instance publicly")
        database = Database(settings)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.provider_timeout, connect=10),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=False,
        ) as client:
            app.state.runtime = Runtime(
                settings=settings,
                database=database,
                storage=storage,
                http_client=client,
                documents=DocumentService(database.session_factory, storage, settings),
                search=SearchService(database.session_factory, client, settings),
            )
            try:
                yield
            finally:
                await database.dispose()

    app = FastAPI(title="DocFlow Engine", version=__version__, lifespan=lifespan)
    app.include_router(router)

    @app.exception_handler(DocFlowError)
    async def domain_error(request: Request, exc: Exception) -> JSONResponse:
        error = exc if isinstance(exc, DocFlowError) else DocFlowError()
        return JSONResponse(
            status_code=error.status_code,
            content={"detail": str(error) or "Внутренняя ошибка", "code": error.code},
        )

    @app.exception_handler(PermanentPipelineError)
    async def invalid_pipeline(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=422, content={"detail": str(exc), "code": "invalid_pipeline"}
        )

    @app.exception_handler(ProviderError)
    async def provider_error(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": str(exc), "code": "provider_error"})

    @app.get("/health/live", tags=["health"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    async def ready(request: Request) -> JSONResponse:
        runtime: Runtime = request.app.state.runtime
        try:
            await runtime.database.check_connection()
        except SQLAlchemyError:
            return JSONResponse(status_code=503, content={"status": "not_ready"})
        return JSONResponse(content={"status": "ready"})

    return app


app = create_app()
