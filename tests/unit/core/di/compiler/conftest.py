"""Fixtures of the compiler tests."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from tests.unit.core.di.compiler.doubles import (
    AcmeAudit,
    AcmeClock,
    AcmeQuotes,
    AcmeRates,
    AcmeTenant,
)


@pytest.fixture
def bound() -> LoomContainer:
    """A clock singleton, rates and quotes per execution over it, and two unrelated singletons."""
    container = LoomContainer()
    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)
    container.register(AcmeRates, AcmeRates)
    container.register(AcmeQuotes, AcmeQuotes)
    container.register(AcmeAudit, AcmeAudit, scope=Scope.APPLICATION)
    container.register_instance(AcmeTenant, AcmeTenant())
    return container
