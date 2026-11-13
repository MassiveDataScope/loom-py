"""Call edges (N3): closures cross them; cycle and singleton refusals stop at them."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCall, AcmeCaller


class AcmeSession:
    """Bound per execution."""


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


class AcmeQuote:
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


def _bound() -> LoomContainer:
    built = acme.container()
    built.register(AcmeSession, AcmeSession, scope=Scope.REQUEST)
    return built


def test_a_called_class_is_wrapped_as_a_call_edge_to_its_class_node() -> None:
    graph = acme.analysed(_bound(), AcmeMarket).graph

    assert graph.edges(ClassNode(AcmeMarket)) == (
        Edge(ClassNode(AcmeValuate), "valuate", call_edge=True),
    )


def test_a_closure_walk_crosses_call_edges() -> None:
    analysis = acme.analysed(_bound(), AcmeMarket)

    assert {ClassNode(AcmeValuate), KeyNode(AcmeSession)} <= analysis.reachable


def test_a_loop_through_call_edges_builds_and_its_analysis_ends() -> None:
    analysis = acme.analysed(_bound(), AcmeLoop)

    assert {ClassNode(AcmeLoop), ClassNode(AcmeLoopBack)} <= analysis.reachable


def test_a_construction_cycle_after_a_call_edge_is_refused() -> None:
    built = _bound()
    built.register(AcmeRingFirst, AcmeRingFirst)
    built.register(AcmeRingSecond, AcmeRingSecond)

    with pytest.raises(
        DependencyGraphError,
        match=(
            "AcmeRing -> first: AcmeRingFirst -> second: AcmeRingSecond -> first: AcmeRingFirst"
            " -> second: AcmeRingSecond: dependency cycle"
        ),
    ):
        acme.analysed(built, AcmeRing)


def test_a_construction_cycle_first_reached_across_a_call_loop_is_refused() -> None:
    built = _bound()
    for key in (AcmeBook, AcmeDesk, AcmeTrader):
        built.register(key, key)

    with pytest.raises(DependencyGraphError, match="trader: AcmeTrader: dependency cycle"):
        acme.analysed(built, AcmeTrading)


def test_a_singleton_calling_a_class_with_per_execution_dependencies_is_not_captive() -> None:
    built = _bound()
    built.register(AcmeInvoking, AcmeInvoking, scope=Scope.APPLICATION)

    analysis = acme.analysed(built, AcmePricing)

    assert KeyNode(AcmeSession) in analysis.reachable


def test_a_singleton_reaching_a_caller_through_a_plain_edge_is_refused() -> None:
    built = _bound()
    built.register(AcmeCachedPricing, AcmeCachedPricing, scope=Scope.APPLICATION)
    built.register(AcmeIdentityCache, AcmeIdentityCache, scope=Scope.APPLICATION)

    with pytest.raises(DependencyGraphError, match="cache: AcmeIdentityCache: caller"):
        acme.analysed(built, AcmeQuote)


def test_a_class_node_target_is_never_a_missing_binding() -> None:
    built = _bound()

    analysis = acme.analysed(built, AcmeMarket)

    assert not built.is_registered(AcmeValuate)
    assert ClassNode(AcmeValuate) in analysis.reachable
