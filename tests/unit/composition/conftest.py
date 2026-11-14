"""Fixtures shared by the tests that compose an application."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest_asyncio

from tests.helpers.composition import application_openings, fixture_project
from tests.helpers.loops import event_loops

__all__ = ["application_openings", "event_loops", "fixture_project"]


@pytest_asyncio.fixture
async def loop_errors() -> AsyncIterator[list[dict[str, object]]]:
    """Capture loop exception reports and restore the previous handler."""
    loop = asyncio.get_running_loop()
    handler = loop.get_exception_handler()
    reported: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _, context: reported.append(context))
    try:
        yield reported
    finally:
        loop.set_exception_handler(handler)
