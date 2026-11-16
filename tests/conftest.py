"""Интеграционные тесты используют настоящий PostgreSQL 16 + pgvector.

Без TEST_DATABASE_URL поднимается временный pgserver из dev-зависимостей.
Внешний DSN допускается только в базу с именем *_test: fixture выполняет TRUNCATE.
"""

import io
import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pgserver
import pytest
from alembic import command
from alembic.config import Config
from PIL import Image
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from src.database import Database
from src.services.documents import DocumentService
from src.storage import LocalStorage

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    external = os.getenv("TEST_DATABASE_URL")
    if external:
        url = make_url(external)
        if url.drivername != "postgresql+asyncpg" or not (url.database or "").endswith("_test"):
            raise pytest.UsageError(
                "TEST_DATABASE_URL must be postgresql+asyncpg and database *_test"
            )
        yield external
        return
    with tempfile.TemporaryDirectory(prefix="docflow-pg-") as directory:
        server = pgserver.get_server(directory, cleanup_mode="stop")
        try:
            yield server.get_uri().replace("postgresql://", "postgresql+asyncpg://", 1)
        finally:
            server.cleanup()


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    command.upgrade(config, "head")
    return database_url


@pytest.fixture
def settings(migrated_database: str, tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_url=migrated_database,
        storage_root=tmp_path / "storage",
        pipeline_dir=ROOT / "config/pipelines",
        api_key=None,
        max_upload_bytes=1024,
        worker_lease_seconds=30,
        worker_heartbeat_seconds=0.05,
        retry_base_delay=0,
        retry_max_delay=0,
        provider_timeout=5,
    )


@pytest.fixture
async def database(settings: Settings) -> AsyncIterator[Database]:
    instance = Database(settings)
    async with instance.engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE documents CASCADE"))
    try:
        yield instance
    finally:
        await instance.dispose()


@pytest.fixture
def session_factory(database: Database) -> async_sessionmaker[AsyncSession]:
    return database.session_factory


@pytest.fixture
def storage(settings: Settings) -> LocalStorage:
    instance = LocalStorage(settings.storage_root)
    instance.ensure_directories()
    return instance


@pytest.fixture
def service(
    session_factory: async_sessionmaker[AsyncSession], storage: LocalStorage, settings: Settings
) -> DocumentService:
    return DocumentService(session_factory, storage, settings)


@pytest.fixture
def png_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (20, 20), "white").save(output, format="PNG")
    return output.getvalue()


@pytest.fixture
async def client(settings: Settings, database: Database) -> AsyncIterator[httpx.AsyncClient]:
    from src.api.main import create_app

    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as instance:
            yield instance
