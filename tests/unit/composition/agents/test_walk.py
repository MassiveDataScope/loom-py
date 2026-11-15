"""The walk: every constructor, factory and ``execute()`` declaration a root reaches."""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

import pytest

from loom.composition._ai import DeclarationWalk
from loom.core.bootstrap import create_kernel
from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, Node
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.execution import Kind
from loom.core.identity import ANONYMOUS
from loom.core.use_case.kinds import AGENT, MCP
from tests.unit.composition.agents.doubles import (
    AcmeDelegating,
    AcmeGatewayQueries,
    AcmeMarket,
    AcmeQueries,
    AcmeQuiet,
    AcmeValuate,
)
from tests.unit.core.di.graph.acme import CALL


class TestReachedDeclarations:
    @pytest.mark.parametrize(
        ("root", "declared"),
        [
            (
                AcmeMarket,
                [(AGENT, "analyst"), (MCP, "AcmeMarket -> queries: AcmeQueries: gateway")],
            ),
            (AcmeValuate, [(MCP, "gateway"), (AGENT, "valuer")]),
            (
                AcmeDelegating,
                [
                    (CALL, "valuate"),
                    (MCP, "AcmeDelegating -> valuate: AcmeValuate: gateway"),
                    (AGENT, "AcmeDelegating -> valuate: AcmeValuate: valuer"),
                ],
            ),
        ],
        ids=["through-an-adapter", "on-the-root-itself", "through-a-call-edge"],
    )
    def test_lists_each_declaration_where_it_is_declared(
        self,
        container: LoomContainer,
        compiler: UseCaseCompiler,
        root: type[Any],
        declared: list[tuple[Kind, str]],
    ) -> None:
        container.register(AcmeQueries, AcmeGatewayQueries)

        walked = DeclarationWalk(container.graph, compiler).reached([ClassNode(root)])

        assert [(r.binding.kind, r.where) for r in walked[ClassNode(root)]] == declared


class TestReachedAtStartupOnly:
    def test_an_execution_reads_no_edge_while_the_startup_walk_does(
        self, edge_reads: Counter[Node]
    ) -> None:
        kernel = create_kernel(
            config=object(),
            use_cases=[AcmeQuiet],
            modules=[lambda container: container.register(AcmeQueries, AcmeGatewayQueries)],
            mcp_resolver=lambda server, include, identity: f"handle:{server}",
        )
        root = ClassNode(AcmeQuiet)
        walked = DeclarationWalk(kernel.container.graph, kernel.compiler).reached([root])
        reads_at_startup = sum(edge_reads.values())
        edge_reads.clear()

        result = asyncio.run(kernel.executor.run(AcmeQuiet, identity=ANONYMOUS))

        assert result == "quiet"
        assert [r.binding.name for r in walked[root]] == ["gateway"]
        assert reads_at_startup > 0
        assert sum(edge_reads.values()) == 0
