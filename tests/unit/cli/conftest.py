"""The ``loom`` command prints a failure's public form; a test reads the failure in process."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.helpers.handoffs import EchoedFailures, echoed_failures


@pytest.fixture
def echoed() -> Iterator[EchoedFailures]:
    """Record each failure a command prints, so a test can match its message."""
    with echoed_failures() as failures:
        yield failures
