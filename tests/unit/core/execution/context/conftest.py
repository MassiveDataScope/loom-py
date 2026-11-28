"""Fixtures of the execution context tests."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import pytest

from loom.core.execution import ExecutionContext, context
from loom.core.identity import Identity
from loom.core.identity import context as identity_context
from tests.helpers.loggers import CapturedLog
from tests.helpers.outliving import outlive
from tests.unit.core.execution.context.doubles import ACME_ALICE


@pytest.fixture
def recorded(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    """What the execution context logs during the test, its delegation refusal included."""
    for module in (context, identity_context):
        caplog.set_level(logging.DEBUG, logger=module.__name__)
    return CapturedLog(caplog)


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
