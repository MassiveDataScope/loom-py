"""F8: re-registering a key recompiles what depends on it; after seal, built singletons stay."""

from __future__ import annotations

import warnings

import pytest

from loom.core.di import ContainerSealedWarning
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.repository.abc import RepoFor
from loom.core.repository.dependency import classify_key


class AcmeClock:
    pass


class AcmeFrozenClock(AcmeClock):
    pass


class AcmeRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeQuotes:
    def __init__(self, rates: AcmeRates) -> None:
        self.rates = rates


class AcmeAudit:
    """Unrelated to the clock."""


class AcmeTenant:
    """Registered as an instance."""


def _bound() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)
    container.register(AcmeRates, AcmeRates)
    container.register(AcmeQuotes, AcmeQuotes)
    container.register(AcmeAudit, AcmeAudit, scope=Scope.APPLICATION)
    container.register_instance(AcmeTenant, AcmeTenant())
    return container


def test_before_seal_a_re_registered_key_reaches_its_dependents_transitively() -> None:
    container = _bound()
    container.resolve(AcmeQuotes)

    container.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

    assert type(container.resolve(AcmeQuotes).rates.clock) is AcmeFrozenClock
    assert type(container.resolve(AcmeRates).clock) is AcmeFrozenClock


def test_before_seal_a_re_registered_key_drops_its_own_instance() -> None:
    container = _bound()
    clock = container.resolve(AcmeClock)

    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)

    assert container.resolve(AcmeClock) is not clock


def test_before_seal_nothing_unrelated_is_dropped() -> None:
    container = _bound()
    audit, tenant = container.resolve(AcmeAudit), container.resolve(AcmeTenant)

    container.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)
    container.register(AcmeRates, AcmeRates)

    assert container.resolve(AcmeAudit) is audit
    assert container.resolve(AcmeTenant) is tenant


class AcmeAuditReport:
    def __init__(self, audits: RepoFor[AcmeAudit]) -> None:
        self.audits = audits


def test_before_seal_a_remapped_repository_reaches_its_dependents() -> None:
    container = LoomContainer(classify_key=classify_key)
    container.register(AcmeClock, AcmeClock)
    container.register(AcmeFrozenClock, AcmeFrozenClock)
    container.register_repo(AcmeAudit, AcmeClock)
    container.register(AcmeAuditReport, AcmeAuditReport)
    assert type(container.resolve(AcmeAuditReport).audits) is AcmeClock

    container.register_repo(AcmeAudit, AcmeFrozenClock)

    assert type(container.resolve(AcmeAuditReport).audits) is AcmeFrozenClock


def test_after_seal_dependents_see_the_new_key_and_built_singletons_stay() -> None:
    container = _bound()
    container.register(AcmeRates, AcmeRates, scope=Scope.APPLICATION)
    container.seal()
    rates, audit, tenant = (
        container.resolve(AcmeRates),
        container.resolve(AcmeAudit),
        container.resolve(AcmeTenant),
    )
    container.resolve(AcmeQuotes)

    with pytest.warns(ContainerSealedWarning):
        container.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

    assert type(container.resolve(AcmeClock)) is AcmeFrozenClock
    assert container.resolve(AcmeRates) is rates
    assert type(rates.clock) is AcmeClock
    assert container.resolve(AcmeAudit) is audit
    assert container.resolve(AcmeTenant) is tenant


def test_after_seal_a_per_execution_dependent_sees_the_new_key() -> None:
    container = _bound()
    container.seal()
    container.resolve(AcmeRates)

    with pytest.warns(ContainerSealedWarning):
        container.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

    assert type(container.resolve(AcmeRates).clock) is AcmeFrozenClock
    assert type(container.resolve(AcmeQuotes).rates.clock) is AcmeFrozenClock


def test_after_seal_a_registered_instance_is_served_at_once() -> None:
    container = _bound()
    container.seal()
    frozen = AcmeFrozenClock()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ContainerSealedWarning)
        container.register_instance(AcmeClock, frozen)

    assert container.resolve(AcmeClock) is frozen


def test_after_seal_a_built_dependent_is_served_as_a_constant_again() -> None:
    container = _bound()
    container.register(AcmeRates, AcmeRates, scope=Scope.APPLICATION)
    container.seal()
    rates = container.resolve(AcmeRates)

    with pytest.warns(ContainerSealedWarning):
        container.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)
    assert container.resolve(AcmeRates) is rates

    served = container._compiled[AcmeRates]  # pyright: ignore[reportPrivateUsage]
    assert type(served).__name__ == "method-wrapper"  # repeat(rates).__next__, no Python frame
