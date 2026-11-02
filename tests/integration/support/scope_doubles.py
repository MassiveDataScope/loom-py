"""Use-case doubles shared by the invocation-scope integration tests (ADR 0008)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from loom.core.use_case.use_case import UseCase

Body = Callable[[], Awaitable[Any]]


class BusinessError(Exception):
    """A business failure a body raises."""


class Run(UseCase[Any, Any]):
    """Runs whatever body the test hands it."""

    def __init__(self, body: Body) -> None:
        self._body = body

    async def execute(self) -> Any:
        return await self._body()
