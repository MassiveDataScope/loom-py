"""Fixtures of the execution context tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from loom.core.execution import context, enter, leave
from loom.core.identity import Identity
from tests.helpers.loggers import RecordingLogger
from tests.unit.core.execution.context.doubles import ACME_ALICE


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> RecordingLogger:
    """What the execution context logs during the test."""
    logger = RecordingLogger()
    monkeypatch.setattr(context, "_log", logger)
    return logger


@pytest.fixture
def outliving() -> Callable[..., Awaitable[asyncio.Task[object]]]:
    """Start a task inside an execution that calls ``read`` once that execution has ended."""

    async def start(
        read: Callable[[], object], *, identity: Identity | None = ACME_ALICE
    ) -> asyncio.Task[object]:
        release = asyncio.Event()

        async def late() -> object:
            await release.wait()
            return read()

        token = enter(identity)
        try:
            task = asyncio.create_task(late())
        finally:
            leave(token)
        release.set()
        return task

    return start
