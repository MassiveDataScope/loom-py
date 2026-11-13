"""Reachable singletons are built at seal with no execution; the hot path then serves them."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraph, DependencyGraphError, analyse
from loom.core.di.introspection import RepoDependency
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, current_execution
from loom.core.identity import ANONYMOUS
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case import UseCase
from loom.core.use_case.factory import UseCaseFactory
from tests.unit.core.di.graph import acme


class AcmeOrder(BaseModel):
    __tablename__ = "di_seal_acme_orders"

    id: int = ColumnField(primary_key=True)


class AcmeListsOrders(UseCase[AcmeOrder, int]):
    async def execute(self) -> int:
        return 0


class AcmeClock:
    seen: list[object] = []

    def __init__(self) -> None:
        AcmeClock.seen.append(current_execution())


class AcmeRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeQuote:
    def __init__(self, rates: AcmeRates) -> None:
        self.rates = rates


def _bound() -> LoomContainer:
    AcmeClock.seen = []
    container = LoomContainer()
    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)
    container.register(AcmeRates, AcmeRates, scope=Scope.APPLICATION)
    return container


def test_reachable_singletons_are_built_at_seal_outside_any_execution() -> None:
    container = _bound()
    analysis = acme.analysed(container, AcmeQuote)

    with ExecutionScope(ANONYMOUS):
        container.seal(analysis)

    assert AcmeClock.seen == [None]
    rates = container.resolve(AcmeRates)
    assert rates.clock is container.resolve(AcmeClock)
    assert AcmeClock.seen == [None]


def test_seal_refuses_the_analysis_of_another_graph() -> None:
    container = _bound()
    analysis = analyse(DependencyGraph(container), [ClassNode(AcmeQuote)])

    with pytest.raises(ValueError, match="another dependency graph"):
        container.seal(analysis)


class AcmeOldRates:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeLedger:
    """What the new rates need instead of a clock."""


class AcmeNewRates:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


def test_a_key_registered_again_after_analysis_is_sealed_with_its_new_constructor() -> None:
    container = _bound()
    container.register(AcmeRates, AcmeOldRates, scope=Scope.APPLICATION)
    container.register(AcmeLedger, AcmeLedger, scope=Scope.APPLICATION)
    analysis = acme.analysed(container, AcmeQuote)

    container.register(AcmeRates, AcmeNewRates, scope=Scope.APPLICATION)
    container.seal(analysis)

    assert container.resolve(AcmeRates).ledger is container.resolve(AcmeLedger)


def test_the_container_and_the_factory_read_one_graph() -> None:
    container = LoomContainer()
    UseCaseFactory(container)

    assert container.graph.constructor(ClassNode(AcmeListsOrders)) == (
        ("main_repo", RepoDependency(AcmeOrder, None)),
    )


class AcmeUnboundRates:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


def test_a_strict_seal_surfaces_a_reached_binding_that_does_not_compile() -> None:
    container = _bound()
    container.register(AcmeRates, AcmeOldRates)
    analysis = acme.analysed(container, AcmeQuote)
    container.register(AcmeRates, AcmeUnboundRates)

    with pytest.raises(DependencyGraphError, match="ledger: AcmeLedger: no binding"):
        container.seal(analysis)


def test_a_second_factory_reads_the_one_graph() -> None:
    container = LoomContainer()
    first = UseCaseFactory(container)
    second = UseCaseFactory(container)

    assert first.graph is second.graph is container.graph


def test_reading_constructors_another_way_is_refused() -> None:
    container = LoomContainer()
    UseCaseFactory(container)

    with pytest.raises(ValueError, match="already reads constructors"):
        container.read_with(classify_key=lambda annotation: None, implicit=lambda cls: ())
