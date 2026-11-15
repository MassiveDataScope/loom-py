"""A factory provider's annotated parameters are its dependencies, read like a constructor's."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.errors import DependencyWarning
from loom.core.di.graph import DependencyGraphError, Edge, KeyNode
from loom.core.di.scope import Scope
from tests.unit.core.di.doubles import AcmeSession
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.doubles import AcmeLedger
from tests.unit.core.doubles import AcmeClock


class AcmeGateway:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


class AcmeDesk:
    """A root reaching the gateway."""

    def __init__(self, gateway: AcmeGateway) -> None:
        self.gateway = gateway


class AcmeLoop:
    def __init__(self, gateway: AcmeGateway) -> None:
        self.gateway = gateway


def make_gateway(ledger: AcmeLedger) -> AcmeGateway:
    return AcmeGateway(ledger)


def make_looping_gateway(loop: AcmeLoop) -> AcmeGateway:
    return AcmeGateway(AcmeLedger(AcmeClock()))


def make_session_gateway(session: AcmeSession) -> AcmeGateway:
    return AcmeGateway(AcmeLedger(AcmeClock()))


def make_unannotated_gateway(ledger):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN201
    return AcmeGateway(ledger)


def make_plain_gateway() -> AcmeGateway:
    return AcmeGateway(AcmeLedger(AcmeClock()))


@pytest.fixture
def container() -> LoomContainer:
    return acme.container()


class TestFactoryEdges:
    def test_a_factory_parameter_is_an_edge_named_after_it(self, container: LoomContainer) -> None:
        container.register(AcmeGateway, make_gateway)

        assert container.graph.edges(KeyNode(AcmeGateway)) == (Edge(KeyNode(AcmeLedger), "ledger"),)

    def test_a_zero_argument_factory_stays_opaque(self, container: LoomContainer) -> None:
        container.register(AcmeGateway, make_plain_gateway)

        assert container.graph.edges(KeyNode(AcmeGateway)) == ()


class TestFactoryRefusals:
    def test_refuses_a_missing_parameter_binding_with_its_path(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeGateway, make_gateway)

        with pytest.raises(DependencyGraphError) as refused:
            acme.analysed(container, AcmeDesk)

        assert str(refused.value) == (
            "AcmeDesk -> gateway: AcmeGateway -> ledger: AcmeLedger: "
            "no binding is registered for it"
        )

    def test_refuses_a_singleton_factory_taking_a_per_execution_binding(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeSession, AcmeSession)
        container.register(AcmeGateway, make_session_gateway, scope=Scope.APPLICATION)

        with pytest.raises(DependencyGraphError) as refused:
            acme.analysed(container, AcmeDesk)

        assert str(refused.value) == (
            "AcmeDesk -> gateway: AcmeGateway -> session: AcmeSession: "
            "an APPLICATION binding cannot hold a per-execution one"
        )

    def test_refuses_a_cycle_through_a_factory(self, container: LoomContainer) -> None:
        container.register(AcmeLoop, AcmeLoop)
        container.register(AcmeGateway, make_looping_gateway)

        with pytest.raises(DependencyGraphError) as refused:
            acme.analysed(container, AcmeDesk)

        assert str(refused.value) == (
            "AcmeDesk -> gateway: AcmeGateway -> loop: AcmeLoop -> gateway: AcmeGateway: "
            "dependency cycle"
        )

    def test_refuses_an_unannotated_factory_parameter(self, container: LoomContainer) -> None:
        container.register(AcmeLedger, lambda: AcmeLedger(AcmeClock()))
        container.register(AcmeGateway, make_unannotated_gateway)

        with pytest.raises(DependencyGraphError, match="AcmeGateway: ledger has no annotation"):
            acme.analysed(container, AcmeDesk)


class TestFactorySeal:
    def test_an_unreached_singleton_factory_taking_a_per_execution_binding_is_not_built(
        self, container: LoomContainer
    ) -> None:
        container.register(AcmeSession, AcmeSession)
        container.register(AcmeGateway, make_session_gateway, scope=Scope.APPLICATION)

        with pytest.warns(DependencyWarning, match="AcmeGateway is not built"):
            container.seal(acme.analysed(container))
        with pytest.raises(DependencyGraphError, match="an APPLICATION binding cannot hold"):
            container.resolve(AcmeGateway)


class TestFactoryInjection:
    def test_the_factory_receives_the_bound_argument(self, container: LoomContainer) -> None:
        ledger = AcmeLedger(AcmeClock())
        container.register_instance(AcmeLedger, ledger)
        container.register(AcmeGateway, make_gateway)

        assert container.resolve(AcmeGateway).ledger is ledger
