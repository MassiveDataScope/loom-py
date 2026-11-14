"""What cuts the walk: an instance or a zero-argument factory; a factory's parameters do not."""

from __future__ import annotations

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.walk import reached
from tests.unit.core.use_case.walk.doubles import (
    AcmeFakeQueries,
    AcmeGatewayQueries,
    AcmeQueries,
    AcmeQuiet,
    queries_through,
)


def _where(container: LoomContainer, compiler: UseCaseCompiler) -> list[str]:
    root = ClassNode(AcmeQuiet)
    return [r.where for r in reached([root], container.graph, compiler)[root]]


class TestInstanceCut:
    def test_an_instance_binding_removes_the_adapter_declaration(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register(AcmeQueries, AcmeGatewayQueries)
        assert _where(container, compiler) == ["AcmeQuiet -> queries: AcmeQueries: gateway"]

        container.register_instance(AcmeQueries, AcmeFakeQueries())

        assert _where(container, compiler) == []


class TestFactoryCut:
    def test_a_zero_argument_factory_hides_what_it_builds(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register(AcmeQueries, lambda: AcmeGatewayQueries())

        assert _where(container, compiler) == []

    def test_a_factory_parameter_is_walked_through(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register(AcmeGatewayQueries, AcmeGatewayQueries)
        container.register(AcmeQueries, queries_through)

        assert _where(container, compiler) == [
            "AcmeQuiet -> queries: AcmeQueries -> gateway: AcmeGatewayQueries: gateway"
        ]
