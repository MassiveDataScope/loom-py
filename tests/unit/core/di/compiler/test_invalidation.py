"""F8: re-registering a key recompiles what depends on it; a sealed container refuses it."""

from __future__ import annotations

import sys

import pytest

from loom.core.di import ContainerSealed
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.repository.registration import classify_key
from tests.unit.core.di.compiler.doubles import (
    AcmeAudit,
    AcmeAuditReport,
    AcmeClock,
    AcmeClockRates,
    AcmeFrozenClock,
    AcmeQuotes,
    AcmeTenant,
)


class TestRegisteringBeforeSeal:
    def test_a_re_registered_key_reaches_its_dependents_transitively(
        self, bound: LoomContainer
    ) -> None:
        bound.resolve(AcmeQuotes)

        bound.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

        assert type(bound.resolve(AcmeQuotes).rates.clock) is AcmeFrozenClock
        assert type(bound.resolve(AcmeClockRates).clock) is AcmeFrozenClock

    def test_a_re_registered_key_drops_its_own_instance(self, bound: LoomContainer) -> None:
        clock = bound.resolve(AcmeClock)

        bound.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)

        assert bound.resolve(AcmeClock) is not clock

    def test_nothing_unrelated_is_dropped(self, bound: LoomContainer) -> None:
        audit, tenant = bound.resolve(AcmeAudit), bound.resolve(AcmeTenant)

        bound.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)
        bound.register(AcmeClockRates, AcmeClockRates)

        assert bound.resolve(AcmeAudit) is audit
        assert bound.resolve(AcmeTenant) is tenant

    def test_a_remapped_repository_reaches_its_dependents(self) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(AcmeClock, AcmeClock)
        container.register(AcmeFrozenClock, AcmeFrozenClock)
        container.register_repo(AcmeAudit, AcmeClock)
        container.register(AcmeAuditReport, AcmeAuditReport)
        assert type(container.resolve(AcmeAuditReport).audits) is AcmeClock

        container.register_repo(AcmeAudit, AcmeFrozenClock)

        assert type(container.resolve(AcmeAuditReport).audits) is AcmeFrozenClock


class TestRegisteringAfterSeal:
    def test_is_refused_and_dependents_and_built_singletons_stay(
        self, bound: LoomContainer
    ) -> None:
        bound.register(AcmeClockRates, AcmeClockRates, scope=Scope.APPLICATION)
        bound.seal()
        rates, audit, tenant = (
            bound.resolve(AcmeClockRates),
            bound.resolve(AcmeAudit),
            bound.resolve(AcmeTenant),
        )
        bound.resolve(AcmeQuotes)

        with pytest.raises(ContainerSealed):
            bound.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

        assert type(bound.resolve(AcmeClock)) is AcmeClock
        assert bound.resolve(AcmeClockRates) is rates
        assert type(rates.clock) is AcmeClock
        assert bound.resolve(AcmeAudit) is audit
        assert bound.resolve(AcmeTenant) is tenant

    def test_a_per_execution_dependent_keeps_the_sealed_key(self, bound: LoomContainer) -> None:
        bound.seal()
        bound.resolve(AcmeClockRates)

        with pytest.raises(ContainerSealed):
            bound.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)

        assert type(bound.resolve(AcmeClockRates).clock) is AcmeClock
        assert type(bound.resolve(AcmeQuotes).rates.clock) is AcmeClock

    def test_a_registered_instance_is_refused(self, bound: LoomContainer) -> None:
        bound.seal()

        with pytest.raises(ContainerSealed):
            bound.register_instance(AcmeClock, AcmeFrozenClock())

        assert type(bound.resolve(AcmeClock)) is AcmeClock

    def test_a_built_dependent_is_still_served_as_a_constant(self, bound: LoomContainer) -> None:
        bound.register(AcmeClockRates, AcmeClockRates, scope=Scope.APPLICATION)
        bound.seal()
        rates = bound.resolve(AcmeClockRates)

        with pytest.raises(ContainerSealed):
            bound.register(AcmeClock, AcmeFrozenClock, scope=Scope.APPLICATION)
        assert bound.resolve(AcmeClockRates) is rates

        frames: list[str] = []
        sys.setprofile(lambda frame, event, arg: frames.append(event))
        bound.resolve(AcmeClockRates)
        sys.setprofile(None)
        assert frames.count("call") == 1
