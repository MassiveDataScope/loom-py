"""Fixtures shared by the runtime unit tests."""

from __future__ import annotations

import pytest

from tests.integration.ai.conftest import StubDepsFactory


@pytest.fixture
def deps() -> StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""
    return StubDepsFactory()
