"""Точка входа воркера: python run_worker.py."""

import asyncio
import signal

import httpx

from config.settings import get_settings
from src.database import Database
from src.logging import configure_logging
from src.queue.worker import Worker
from src.storage import LocalStorage


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    storage = LocalStorage(settings.storage_root)
    storage.ensure_directories()
    database = Database(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.provider_timeout, connect=10),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=False,
        ) as client:
            worker = Worker(settings, database.session_factory, storage, client)
            await worker.run(stop)
    finally:
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(main())
