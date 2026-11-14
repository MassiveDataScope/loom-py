"""Fixtures of the seal tests."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from tests.unit.core.di.seal.doubles import AcmeLedger


@pytest.fixture
def sealed() -> LoomContainer:
    """A sealed container with one singleton, ``AcmeLedger``."""
    container = LoomContainer()
    container.register(AcmeLedger, AcmeLedger, scope=Scope.APPLICATION)
    container.seal()
    return container
