"""Точка входа API: uvicorn main:app --host 0.0.0.0 --port 8000."""

from src.api.main import app

__all__ = ["app"]
