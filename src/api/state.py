"""Ресурсы приложения, создаваемые на время lifespan."""

from dataclasses import dataclass

import httpx

from config.settings import Settings
from src.database import Database
from src.services.documents import DocumentService
from src.services.search import SearchService
from src.storage import LocalStorage


@dataclass
class Runtime:
    settings: Settings
    database: Database
    storage: LocalStorage
    http_client: httpx.AsyncClient
    documents: DocumentService
    search: SearchService
