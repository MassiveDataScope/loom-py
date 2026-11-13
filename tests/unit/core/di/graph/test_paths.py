"""A refusal names the path from its root, one ``-> parameter: node`` step per edge."""

from __future__ import annotations

from typing import Generic, TypeVar

import pytest

from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode, Path
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme

T = TypeVar("T")


class AcmeClock:
    pass


class AcmeLedger:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeRates:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


class AcmeQuote:
    def __init__(self, rates: AcmeRates) -> None:
        self.rates = rates


def test_a_missing_binding_names_every_step_from_the_root() -> None:
    built = acme.container()
    built.register(AcmeRates, AcmeRates)
    built.register(AcmeLedger, AcmeLedger, scope=Scope.REQUEST)

    with pytest.raises(DependencyGraphError) as caught:
        acme.analysed(built, AcmeQuote)

    assert str(caught.value) == (
        "AcmeQuote -> rates: AcmeRates -> ledger: AcmeLedger -> clock: AcmeClock: "
        "no binding is registered for it"
    )


def test_a_cycle_ends_on_the_node_it_returns_to() -> None:
    built = acme.container()
    built.register(AcmeRates, AcmeRates)
    built.register(AcmeLedger, AcmeQuote)

    with pytest.raises(DependencyGraphError) as caught:
        acme.analysed(built, AcmeQuote)

    assert str(caught.value) == (
        "AcmeQuote -> rates: AcmeRates -> ledger: AcmeLedger -> rates: AcmeRates: dependency cycle"
    )


class AcmeSeries(Generic[T]):
    pass


def test_a_parametrised_key_is_named_by_its_origin_and_arguments() -> None:
    path = Path(ClassNode(AcmeQuote)).to(Edge(KeyNode(AcmeSeries[AcmeClock]), "clocks"))

    assert str(path) == "AcmeQuote -> clocks: AcmeSeries[AcmeClock]"
