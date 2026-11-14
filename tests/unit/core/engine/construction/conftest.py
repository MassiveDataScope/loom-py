"""Each test starts with an empty journal, no execution and the anonymous caller."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from tests.unit.core.engine.construction.acme import JOURNAL


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    JOURNAL.entries.clear()
    yield
    assert current_execution() is None, "an execution was left open"
    assert current_identity() is ANONYMOUS, "an ambient identity was left set"
