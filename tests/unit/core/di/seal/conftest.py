"""Fixtures of the seal tests."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from tests.unit.core.di.seal.doubles import AcmeBoundLedger


@pytest.fixture
def sealed() -> LoomContainer:
    """A sealed container with one singleton, ``AcmeBoundLedger``."""
    container = LoomContainer()
    container.register(AcmeBoundLedger, AcmeBoundLedger, scope=Scope.APPLICATION)
    container.seal()
    return container
