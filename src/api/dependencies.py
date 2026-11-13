"""Зависимости FastAPI и необязательная Bearer-аутентификация."""

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.api.state import Runtime

_bearer = HTTPBearer(auto_error=False)


def get_runtime(request: Request) -> Runtime:
    runtime: Runtime = request.app.state.runtime
    return runtime


async def require_api_key(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    expected = get_runtime(request).settings.api_key
    if expected is None:
        return
    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or not secrets.compare_digest(
            credentials.credentials.encode(), expected.get_secret_value().encode()
        )
    ):
        raise HTTPException(
            status_code=401,
            detail="Требуется Bearer-токен",
            headers={"WWW-Authenticate": "Bearer"},
        )
