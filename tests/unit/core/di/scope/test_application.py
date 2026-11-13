"""``APPLICATION``: built once, outside any execution, at seal or on its first resolve."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, current_execution
from loom.core.identity import ANONYMOUS


class AcmeClock:
    built: list[object] = []

    def __init__(self) -> None:
        AcmeClock.built.append(current_execution())


class AcmeRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


@pytest.fixture(autouse=True)
def _reset() -> None:
    AcmeClock.built = []


def _bound() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)
    container.register(AcmeRates, AcmeRates, scope=Scope.APPLICATION)
    return container


def test_a_singleton_first_resolved_inside_an_execution_is_built_outside_it() -> None:
    container = _bound()

    with ExecutionScope(ANONYMOUS):
        clock = container.resolve(AcmeClock)
        back_inside = current_execution()

    assert AcmeClock.built == [None]
    assert back_inside is not None
    assert container.resolve(AcmeClock) is clock


def test_a_singleton_is_built_once_and_shared_by_its_dependents() -> None:
    container = _bound()

    rates = container.resolve(AcmeRates)

    assert rates.clock is container.resolve(AcmeClock)
    assert container.resolve(AcmeRates) is rates
    assert len(AcmeClock.built) == 1


def test_seal_builds_the_singletons_with_no_execution() -> None:
    container = _bound()

    with ExecutionScope(ANONYMOUS):
        container.seal()

    assert AcmeClock.built == [None]


def test_a_failing_singleton_is_retried_on_the_next_resolve() -> None:
    attempts: list[int] = []

    def flaky() -> AcmeClock:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("acme clock offline")
        return AcmeClock()

    container = LoomContainer()
    container.register(AcmeClock, flaky, scope=Scope.APPLICATION)

    with pytest.raises(RuntimeError, match="acme clock offline"):
        container.resolve(AcmeClock)

    assert container.resolve(AcmeClock) is container.resolve(AcmeClock)
    assert len(attempts) == 2


def test_a_singleton_provider_returning_none_is_built_once() -> None:
    calls: list[int] = []
    container = LoomContainer()
    container.register(AcmeClock, lambda: calls.append(1), scope=Scope.APPLICATION)

    container.resolve(AcmeClock)
    container.resolve(AcmeClock)

    assert calls == [1]
