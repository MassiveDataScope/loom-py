"""``APPLICATION``: built once, outside any execution, at seal or on its first resolve."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, current_execution
from loom.core.identity import ANONYMOUS


class AcmeApplicationClock:
    built: list[object] = []

    def __init__(self) -> None:
        AcmeApplicationClock.built.append(current_execution())


class AcmeApplicationRates:
    def __init__(self, clock: AcmeApplicationClock) -> None:
        self.clock = clock


@pytest.fixture(autouse=True)
def _reset() -> None:
    AcmeApplicationClock.built = []


def _bound() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeApplicationClock, AcmeApplicationClock, scope=Scope.APPLICATION)
    container.register(AcmeApplicationRates, AcmeApplicationRates, scope=Scope.APPLICATION)
    return container


class TestApplicationScope:
    def test_a_singleton_first_resolved_inside_an_execution_is_built_outside_it(self) -> None:
        container = _bound()

        with ExecutionScope(ANONYMOUS):
            clock = container.resolve(AcmeApplicationClock)
            back_inside = current_execution()

        assert AcmeApplicationClock.built == [None]
        assert back_inside is not None
        assert container.resolve(AcmeApplicationClock) is clock

    def test_a_singleton_is_built_once_and_shared_by_its_dependents(self) -> None:
        container = _bound()

        rates = container.resolve(AcmeApplicationRates)

        assert rates.clock is container.resolve(AcmeApplicationClock)
        assert container.resolve(AcmeApplicationRates) is rates
        assert len(AcmeApplicationClock.built) == 1

    def test_seal_builds_the_singletons_with_no_execution(self) -> None:
        container = _bound()

        with ExecutionScope(ANONYMOUS):
            container.seal()

        assert AcmeApplicationClock.built == [None]

    def test_a_failing_singleton_is_retried_on_the_next_resolve(self) -> None:
        attempts: list[int] = []

        def flaky() -> AcmeApplicationClock:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("acme clock offline")
            return AcmeApplicationClock()

        container = LoomContainer()
        container.register(AcmeApplicationClock, flaky, scope=Scope.APPLICATION)

        with pytest.raises(RuntimeError, match="acme clock offline"):
            container.resolve(AcmeApplicationClock)

        assert container.resolve(AcmeApplicationClock) is container.resolve(AcmeApplicationClock)
        assert len(attempts) == 2

    def test_a_singleton_provider_returning_none_is_built_once(self) -> None:
        calls: list[int] = []
        container = LoomContainer()
        container.register(AcmeApplicationClock, lambda: calls.append(1), scope=Scope.APPLICATION)

        container.resolve(AcmeApplicationClock)
        container.resolve(AcmeApplicationClock)

        assert calls == [1]
