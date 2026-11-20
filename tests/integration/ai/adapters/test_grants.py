"""H2: an adapter and an ``execute()`` marker on one server share its one session.

Each keeps its own filter: two views over one session, opened and listed
once, both resolved per execution through the resolver ``wire_ai`` bound.
The graph is walked once, at startup; an execution reads none of it.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.composition import _ai
from loom.core.di.graph import DependencyGraph, Node
from loom.core.identity import Identity
from tests.integration.ai.adapters.conftest import Compose
from tests.integration.ai.adapters.doubles import (
    SERVER,
    AcmeFakeAdvice,
    AcmeHistory,
    AcmeMarket,
)
from tests.integration.ai.conftest import CountingMcpClientFactory, RecordingMcpSession


class TestOneSessionTwoViews:
    async def test_an_adapter_and_an_execute_marker_share_one_session(
        self,
        compose: Compose,
        sessions: CountingMcpClientFactory,
        session: RecordingMcpSession,
        identity: Identity,
    ) -> None:
        application = compose(AcmeMarket, AcmeHistory, advice=AcmeFakeAdvice())
        host, executor = application.ai.agent_host, application.kernel.executor
        assert host is not None

        async with host:
            market = await executor.run(AcmeMarket, identity=identity)
            history = await executor.run(AcmeHistory, identity=identity)

        assert sessions.calls == [SERVER]
        assert session.listed == 1
        assert [name for name, _ in session.calls] == ["quote", "history"]
        assert (market[0], history[0]) == (("quote",), ("history",))


class TestGraphReadAtStartupOnly:
    async def test_startup_walks_once_and_executions_read_no_edge(
        self, compose: Compose, identity: Identity, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        walks: list[object] = []
        edge_reads: list[Node] = []
        walk, edges = _ai.DeclarationWalk.reached, DependencyGraph.edges

        def counted_walk(*args: Any) -> Any:
            walks.append(args)
            return walk(*args)

        def counted_edges(graph: DependencyGraph, node: Node) -> Any:
            edge_reads.append(node)
            return edges(graph, node)

        monkeypatch.setattr(_ai.DeclarationWalk, "reached", counted_walk)
        monkeypatch.setattr(DependencyGraph, "edges", counted_edges)
        application = compose(AcmeMarket, AcmeHistory, advice=AcmeFakeAdvice())
        host = application.ai.agent_host
        assert host is not None
        reads_at_startup = len(edge_reads)
        edge_reads.clear()

        async with host:
            for _ in range(3):
                await application.kernel.executor.run(AcmeMarket, identity=identity)

        assert len(walks) == 1
        assert reads_at_startup > 0
        assert edge_reads == []
