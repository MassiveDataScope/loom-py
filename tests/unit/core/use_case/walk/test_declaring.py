"""``reached`` over several roots: one walk per root, each declaration attributed to its root."""

from __future__ import annotations

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.walk import reached
from tests.unit.core.use_case.walk.doubles import (
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

        walked = reached(roots, container.graph, compiler)

        assert {root: [r.where for r in found] for root, found in walked.items()} == {
            ClassNode(AcmeMarket): ["analyst", "AcmeMarket -> queries: AcmeQueries: gateway"],
            ClassNode(AcmeQuiet): ["AcmeQuiet -> queries: AcmeQueries: gateway"],
        }

    def test_a_root_reaching_nothing_maps_to_no_declaration(
        self, container: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        container.register_instance(AcmeQueries, AcmeFakeQueries())
        roots = [ClassNode(AcmeQuiet), ClassNode(AcmeDelegating)]

        walked = reached(roots, container.graph, compiler)

        assert list(walked) == roots
        assert walked[ClassNode(AcmeQuiet)] == ()
        assert len(walked[ClassNode(AcmeDelegating)]) == 3
