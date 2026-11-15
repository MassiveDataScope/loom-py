"""Fixtures of the documentation tests."""

from __future__ import annotations

import pytest

from loom.core.identity import Identity


@pytest.fixture
def ada() -> Identity:
    """An authenticated caller."""
    return Identity(subject="ada", roles=("billing",), mechanism="jwt")
