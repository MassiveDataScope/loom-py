"""A refusal names the path from its root, one ``-> parameter: node`` step per edge."""

from __future__ import annotations

from typing import Generic, TypeVar

import pytest

from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode, Path
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.doubles import AcmeClock, AcmeLedger, AcmeQuote, AcmeRates

T = TypeVar("T")


class TestDependencyPaths:
    def test_a_missing_binding_names_every_step_from_the_root(self) -> None:
        built = acme.container()
        built.register(AcmeRates, AcmeRates)
        built.register(AcmeLedger, AcmeLedger, scope=Scope.REQUEST)

        with pytest.raises(DependencyGraphError) as caught:
            acme.analysed(built, AcmeQuote)

        assert str(caught.value) == (
            "AcmeQuote -> rates: AcmeRates -> ledger: AcmeLedger -> clock: AcmeClock: "
            "no binding is registered for it"
        )

    def test_a_cycle_ends_on_the_node_it_returns_to(self) -> None:
        built = acme.container()
        built.register(AcmeRates, AcmeRates)
        built.register(AcmeLedger, AcmeQuote)

        with pytest.raises(DependencyGraphError) as caught:
            acme.analysed(built, AcmeQuote)

        assert str(caught.value) == (
            "AcmeQuote -> rates: AcmeRates -> ledger: AcmeLedger -> "
            "rates: AcmeRates: dependency cycle"
        )


class AcmeSeries(Generic[T]):
    pass


class TestParametrisedKeys:
    def test_a_parametrised_key_is_named_by_its_origin_and_arguments(self) -> None:
        path = Path(ClassNode(AcmeQuote)).to(Edge(KeyNode(AcmeSeries[AcmeClock]), "clocks"))

        assert str(path) == "AcmeQuote -> clocks: AcmeSeries[AcmeClock]"
