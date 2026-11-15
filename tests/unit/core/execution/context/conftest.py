"""Fixtures of the execution context tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from loom.core.execution import ExecutionContext, context
from loom.core.identity import Identity
from loom.core.identity import context as identity_context
from tests.helpers.loggers import RecordingLogger
from tests.helpers.outliving import outlive
from tests.unit.core.execution.context.doubles import ACME_ALICE


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> RecordingLogger:
    """What the execution context logs during the test, its delegation refusal included."""
    logger = RecordingLogger()
    monkeypatch.setattr(context, "_log", logger)
    monkeypatch.setattr(identity_context, "_log", logger)
    return logger


@pytest.fixture
def outliving() -> Callable[..., Awaitable[asyncio.Task[object]]]:
    """Start a task inside an execution that calls ``late`` once that execution has ended."""

    async def start(
        late: Callable[[], object],
        *,
        identity: Identity | None = ACME_ALICE,
        inside: Callable[[], object] = lambda: None,
    ) -> asyncio.Task[object]:
        async def opens(body: Callable[[], Awaitable[None]]) -> None:
            token = ExecutionContext.entered(identity)
            try:
                await body()
            finally:
                token.close()

        return await outlive(opens, late, inside=inside)

    return start
