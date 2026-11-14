"""The walk over several roots: one walk per root, each declaration attributed to its root."""

from __future__ import annotations

from loom.composition._ai import DeclarationWalk
from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode
from loom.core.engine.compiler import UseCaseCompiler
from tests.unit.composition.agents.doubles import (
    AcmeDelegating,
    AcmeFakeQueries,
    AcmeGatewayQueries,
    AcmeMarket,
    AcmeQueries,
    AcmeQuiet,
)


class TestDeclarationsPerRoot:
    def test_each_root_reaching_one_adapter_keeps_its_own_path(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register(AcmeQueries, AcmeGatewayQueries)
        roots = [ClassNode(AcmeMarket), ClassNode(AcmeQuiet)]

        walked = DeclarationWalk(container.graph, compiler).reached(roots)

        assert {root: [r.where for r in found] for root, found in walked.items()} == {
            ClassNode(AcmeMarket): ["analyst", "AcmeMarket -> queries: AcmeQueries: gateway"],
            ClassNode(AcmeQuiet): ["AcmeQuiet -> queries: AcmeQueries: gateway"],
        }

    def test_a_root_reaching_nothing_maps_to_no_declaration(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register_instance(AcmeQueries, AcmeFakeQueries())
        roots = [ClassNode(AcmeQuiet), ClassNode(AcmeDelegating)]

        walked = DeclarationWalk(container.graph, compiler).reached(roots)

        assert list(walked) == roots
        assert walked[ClassNode(AcmeQuiet)] == ()
        assert len(walked[ClassNode(AcmeDelegating)]) == 3
