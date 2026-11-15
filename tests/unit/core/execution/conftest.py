"""Every execution test starts with no execution and the anonymous caller.

Synchronous tests share the main :mod:`contextvars` context, so one test that
leaves an execution open would hand its caller to the next. The fixture starts
each test clean, fails the test that leaks, and restores the state anyway.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.identity.context import _NO_ONE, _acting  # pyright: ignore[reportPrivateUsage]


@pytest.fixture(autouse=True)
def _no_execution_leaks() -> Iterator[None]:
    token = _acting.set(_NO_ONE)
    try:
        yield
        assert current_execution() is None, "an execution was left open"
        assert current_identity() is ANONYMOUS, "an ambient identity was left set"
    finally:
        _acting.reset(token)
