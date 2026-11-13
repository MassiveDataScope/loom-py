"""Stand-ins the ``create_kernel`` tests share."""

from __future__ import annotations

from typing import Any

from loom.core.logger import get_logger
from loom.core.use_case.use_case import UseCase

LOG = get_logger("tests.acme.kernel")
"""The logger the internal kernel steps take."""


class AcmeConfig:
    """The application's configuration."""


class AcmePing(UseCase[Any, str]):
    async def execute(self) -> str:
        return "pong"
