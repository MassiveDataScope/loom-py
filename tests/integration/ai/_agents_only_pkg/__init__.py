"""The package an agents-only application scans: one model-less use case."""

from __future__ import annotations

from typing import Any

from loom.core.use_case.keys import use_case_key
from loom.core.use_case.use_case import UseCase

PINGS: list[str] = []


@use_case_key("ping")
class PingUseCase(UseCase[Any, str]):
    """Model-less use case recording one side effect per execution."""

    async def execute(self, **kwargs: Any) -> str:
        del kwargs
        PINGS.append("pong")
        return "pong"
