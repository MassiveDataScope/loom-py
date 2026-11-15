"""Reachable singletons are built at seal with no execution; the hot path then serves them."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraph, DependencyGraphError
from loom.core.di.introspection import RepoDependency, SignatureReader
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, current_execution
from loom.core.identity import ANONYMOUS
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.registration import classify_key
from loom.core.use_case import UseCase
from loom.core.use_case.factory import UseCaseFactory, main_repository
from tests.unit.core.di.graph import acme


class AcmeSealOrder(BaseModel):
    __tablename__ = "di_seal_acme_orders"

    id: int = ColumnField(primary_key=True)


class AcmeListsOrders(UseCase[AcmeSealOrder, int]):
    async def execute(self) -> int:
        return 0


class AcmeExecutionClock:
    seen: list[object] = []

    def __init__(self) -> None:
        AcmeExecutionClock.seen.append(current_execution())


class AcmeExecutionRates:
    def __init__(self, clock: AcmeExecutionClock) -> None:
        self.clock = clock


class AcmeExecutionQuote:
    def __init__(self, rates: AcmeExecutionRates) -> None:
        self.rates = rates


@pytest.fixture
def container() -> LoomContainer:
    AcmeExecutionClock.seen = []
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    container.register(AcmeExecutionClock, AcmeExecutionClock, scope=Scope.APPLICATION)
    container.register(AcmeExecutionRates, AcmeExecutionRates, scope=Scope.APPLICATION)
    return container


class TestSealBuildsReachableSingletons:
    def test_reachable_singletons_are_built_at_seal_outside_any_execution(
        self, container: LoomContainer
    ) -> None:
        analysis = acme.analysed(container, AcmeExecutionQuote)

        with ExecutionScope(ANONYMOUS):
            container.seal(analysis)

        assert AcmeExecutionClock.seen == [None]
        rates = container.resolve(AcmeExecutionRates)
        assert rates.clock is container.resolve(AcmeExecutionClock)
        assert AcmeExecutionClock.seen == [None]

    def test_seal_refuses_the_analysis_of_another_graph(self, container: LoomContainer) -> None:
        graph = DependencyGraph(container.bindings(), {}, SignatureReader(container.kinds))
        analysis = graph.analyse([ClassNode(AcmeExecutionQuote)])

        with pytest.raises(ValueError, match="another dependency graph"):
            container.seal(analysis)


class AcmeOldRates:
    def __init__(self, clock: AcmeExecutionClock) -> None:
        self.clock = clock


class AcmeReplacementLedger:
    """What the new rates need instead of a clock."""


class AcmeNewRates:
    def __init__(self, ledger: AcmeReplacementLedger) -> None:
        self.ledger = ledger


class TestSealUsesTheRegisteredConstructor:
    def test_a_key_registered_again_after_analysis_is_sealed_with_its_new_constructor(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeExecutionRates, AcmeOldRates, scope=Scope.APPLICATION)
        container.register(AcmeReplacementLedger, AcmeReplacementLedger, scope=Scope.APPLICATION)
        analysis = acme.analysed(container, AcmeExecutionQuote)

        container.register(AcmeExecutionRates, AcmeNewRates, scope=Scope.APPLICATION)
        container.seal(analysis)

        assert container.resolve(AcmeExecutionRates).ledger is container.resolve(
            AcmeReplacementLedger
        )

    def test_the_container_and_the_factory_read_one_graph(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        UseCaseFactory(container)

        assert container.graph.constructor(ClassNode(AcmeListsOrders)) == (
            ("main_repo", RepoDependency(AcmeSealOrder, None)),
        )


class AcmeUnboundRates:
    def __init__(self, ledger: AcmeReplacementLedger) -> None:
        self.ledger = ledger


class TestSealRefusesAnUncompilableBinding:
    def test_a_strict_seal_surfaces_a_reached_binding_that_does_not_compile(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeExecutionRates, AcmeOldRates)
        analysis = acme.analysed(container, AcmeExecutionQuote)
        container.register(AcmeExecutionRates, AcmeUnboundRates)

        with pytest.raises(DependencyGraphError, match="ledger: AcmeReplacementLedger: no binding"):
            container.seal(analysis)

    def test_a_second_factory_reads_the_one_graph(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        first = UseCaseFactory(container)
        second = UseCaseFactory(container)

        assert first.graph is second.graph is container.graph

    def test_a_factory_over_a_container_reading_another_way_is_refused(self) -> None:
        container = LoomContainer(classify_key=lambda annotation: None)

        with pytest.raises(ValueError, match="already reads constructors"):
            UseCaseFactory(container)
