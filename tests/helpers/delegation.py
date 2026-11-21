"""The delegation window, the clock past it, acting as a caller, and the switches logged.

Shared by every test of the acting identity (spec 011, T09a); the root
conftest registers the fixtures.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest

from loom.core.identity import Identity, configure_delegation, reset_identity, set_identity
from loom.core.identity import context as identity_context
from tests.helpers.loggers import CapturedLog


@pytest.fixture
def window(monkeypatch: pytest.MonkeyPatch) -> float:
    """A delegation window of one minute, for this test only.

    The window is process-global, as the composition sets it: *monkeypatch*
    gives the previous value back, even when the test composes an
    application that sets its own.
    """
    previous = identity_context._max_delegation_seconds  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(identity_context, "_max_delegation_seconds", previous)
    configure_delegation(60.0)
    return 60.0


@pytest.fixture
def past_the_window(window: float, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    """Move the clock past the window of a delegation starting now, when called."""
    late = time.time() + window + 1

    def move() -> None:
        monkeypatch.setattr(time, "time", lambda: late)

    return move


@pytest.fixture
def switches(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    """Every identity switch reported during the test."""
    caplog.set_level(logging.DEBUG, logger=identity_context.__name__)
    return CapturedLog(caplog, identity_context.__name__)


@contextmanager
def acting_as(caller: Identity | None, *, until: float | None = None) -> Iterator[None]:
    """Install *caller* as a transport does around the body: a new delegation, or *until*."""
    token = set_identity(caller, until=until)
    try:
        yield
    finally:
        reset_identity(token)


def assert_new_delegation(deadline: float | None, before: float, window: float) -> None:
    """*deadline* is a delegation of *window* started between *before* and now."""
    assert deadline is not None
    assert before + window <= deadline <= time.time() + window


__all__ = [
    "acting_as",
    "assert_new_delegation",
    "past_the_window",
    "switches",
    "window",
]
