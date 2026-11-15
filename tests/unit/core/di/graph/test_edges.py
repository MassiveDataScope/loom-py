"""Call edges (N3): closures cross them; cycle and singleton refusals stop at them."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode
from loom.core.di.scope import Scope
from tests.unit.core.di.doubles import AcmeSession
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCall, AcmeCaller


class AcmeValuate:
    def __init__(self, session: AcmeSession, caller: object = AcmeCaller()) -> None:
        self.session = session
        self.caller = caller


class AcmeMarket:
    def __init__(self, valuate: AcmeCall[AcmeValuate]) -> None:
        self.valuate = valuate


class AcmeInvoking:
    """A singleton calling :class:`AcmeValuate`, like ``Invoking[Valuate]``."""

    def __init__(self, target: AcmeCall[AcmeValuate]) -> None:
        self.target = target


class AcmePricing:
    def __init__(self, invoking: AcmeInvoking) -> None:
        self.invoking = invoking


class AcmeIdentityCache:
    def __init__(self, caller: object = AcmeCaller()) -> None:
        self.caller = caller


class AcmeCachedPricing:
    """A singleton holding :class:`AcmeIdentityCache` through a plain key."""

    def __init__(self, cache: AcmeIdentityCache) -> None:
        self.cache = cache


class AcmeCachedQuote:
    def __init__(self, pricing: AcmeCachedPricing) -> None:
        self.pricing = pricing


class AcmeLoop:
    def __init__(self, back: AcmeCall[AcmeLoopBack]) -> None:
        self.back = back


class AcmeLoopBack:
    def __init__(self, loop: AcmeCall[AcmeLoop]) -> None:
        self.loop = loop


class AcmeRingFirst:
    def __init__(self, second: AcmeRingSecond) -> None:
        self.second = second


class AcmeRingSecond:
    def __init__(self, first: AcmeRingFirst) -> None:
        self.first = first


class AcmeRing:
    """Calls :class:`AcmeRingFirst`, whose bindings hold each other."""

    def __init__(self, first: AcmeCall[AcmeRingFirst]) -> None:
        self.first = first


class AcmeBook:
    """Holds :class:`AcmeDesk`, then :class:`AcmeTrader`, which holds it back."""

    def __init__(self, desk: AcmeDesk, trader: AcmeTrader) -> None:
        self.desk = desk
        self.trader = trader


class AcmeDesk:
    def __init__(self, hedge: AcmeCall[AcmeHedge]) -> None:
        self.hedge = hedge


class AcmeHedge:
    def __init__(self, trader: AcmeTrader) -> None:
        self.trader = trader


class AcmeTrader:
    def __init__(self, book: AcmeBook) -> None:
        self.book = book


class AcmeTrading:
    def __init__(self, book: AcmeBook) -> None:
        self.book = book


@pytest.fixture
def container() -> LoomContainer:
    built = acme.container()
    built.register(AcmeSession, AcmeSession, scope=Scope.REQUEST)
    return built


class TestCallEdges:
    def test_a_called_class_is_wrapped_as_a_call_edge_to_its_class_node(
        self, container: LoomContainer
    ) -> None:
        graph = acme.analysed(container, AcmeMarket).graph

        assert graph.edges(ClassNode(AcmeMarket)) == (
            Edge(ClassNode(AcmeValuate), "valuate", call_edge=True),
        )

    def test_a_closure_walk_crosses_call_edges(self, container: LoomContainer) -> None:
        analysis = acme.analysed(container, AcmeMarket)

        assert {ClassNode(AcmeValuate), KeyNode(AcmeSession)} <= analysis.reachable

    def test_a_loop_through_call_edges_builds_and_its_analysis_ends(
        self, container: LoomContainer
    ) -> None:
        analysis = acme.analysed(container, AcmeLoop)

        assert {ClassNode(AcmeLoop), ClassNode(AcmeLoopBack)} <= analysis.reachable

    def test_a_construction_cycle_after_a_call_edge_is_refused(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeRingFirst, AcmeRingFirst)
        container.register(AcmeRingSecond, AcmeRingSecond)

        with pytest.raises(
            DependencyGraphError,
            match=(
                "AcmeRing -> first: AcmeRingFirst -> second: AcmeRingSecond -> first: AcmeRingFirst"
                " -> second: AcmeRingSecond: dependency cycle"
            ),
        ):
            acme.analysed(container, AcmeRing)

    def test_a_construction_cycle_first_reached_across_a_call_loop_is_refused(
        self, container: LoomContainer
    ) -> None:
        for key in (AcmeBook, AcmeDesk, AcmeTrader):
            container.register(key, key)

        with pytest.raises(DependencyGraphError, match="trader: AcmeTrader: dependency cycle"):
            acme.analysed(container, AcmeTrading)

    def test_a_singleton_calling_a_class_with_per_execution_dependencies_is_not_captive(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeInvoking, AcmeInvoking, scope=Scope.APPLICATION)

        analysis = acme.analysed(container, AcmePricing)

        assert KeyNode(AcmeSession) in analysis.reachable

    def test_a_singleton_reaching_a_caller_through_a_plain_edge_is_refused(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeCachedPricing, AcmeCachedPricing, scope=Scope.APPLICATION)
        container.register(AcmeIdentityCache, AcmeIdentityCache, scope=Scope.APPLICATION)

        with pytest.raises(DependencyGraphError, match="cache: AcmeIdentityCache: caller"):
            acme.analysed(container, AcmeCachedQuote)

    def test_a_class_node_target_is_never_a_missing_binding(self, container: LoomContainer) -> None:
        analysis = acme.analysed(container, AcmeMarket)

        assert not container.is_registered(AcmeValuate)
        assert ClassNode(AcmeValuate) in analysis.reachable
