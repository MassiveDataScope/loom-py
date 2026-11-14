"""A container declaring the AI and Acme kinds, and a compiler holding every plan."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import DependencyGraph, Edge, Node
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.kinds import declare_ai_kinds
from tests.unit.core.di.graph import acme
from tests.unit.core.use_case.walk.doubles import (
    AcmeDelegating,
    AcmeDiamond,
    AcmeLeft,
    AcmeMarket,
    AcmeQuiet,
    AcmeRight,
    AcmeShared,
    AcmeValuate,
)


@pytest.fixture
def container() -> LoomContainer:
    container = acme.container()
    declare_ai_kinds(container.kinds)
    return container


@pytest.fixture
def diamond(container: LoomContainer) -> LoomContainer:
    for cls in (AcmeLeft, AcmeRight, AcmeShared):
        container.register(cls, cls)
    return container


@pytest.fixture
def compiler() -> UseCaseCompiler:
    compiler = UseCaseCompiler()
    for use_case in (AcmeMarket, AcmeQuiet, AcmeDiamond, AcmeValuate, AcmeDelegating):
        compiler.compile(use_case)
    return compiler


@pytest.fixture
def edge_reads(monkeypatch: pytest.MonkeyPatch) -> Iterator[Counter[Node]]:
    """How many times each node's edges are read, by anyone, while the test runs."""
    reads: Counter[Node] = Counter()
    edges = DependencyGraph.edges

    def counting(graph: DependencyGraph, node: Node) -> tuple[Edge, ...]:
        reads[node] += 1
        return edges(graph, node)

    monkeypatch.setattr(DependencyGraph, "edges", counting)
    yield reads
