"""The walk keeps a visited set (NB1): a diamond reads each node once."""

from __future__ import annotations

from collections import Counter

from loom.composition._ai import DeclarationWalk
from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, KeyNode, Node
from loom.core.engine.compiler import UseCaseCompiler
from tests.unit.composition.agents.doubles import AcmeDiamond, AcmeLeft, AcmeRight, AcmeShared

_ROOT = ClassNode(AcmeDiamond)


class TestVisitedSet:
    def test_a_diamond_reads_the_edges_of_each_node_once(
        self, diamond: LoomContainer, compiler: UseCaseCompiler, edge_reads: Counter[Node]
    ) -> None:
        DeclarationWalk(diamond.graph, compiler).reached([_ROOT])

        assert edge_reads == {
            _ROOT: 1,
            KeyNode(AcmeLeft): 1,
            KeyNode(AcmeRight): 1,
            KeyNode(AcmeShared): 1,
        }

    def test_a_node_reached_twice_yields_its_declaration_once_under_the_first_path(
        self, diamond: LoomContainer, compiler: UseCaseCompiler
    ) -> None:
        walked = DeclarationWalk(diamond.graph, compiler).reached([_ROOT])

        assert [r.where for r in walked[_ROOT]] == [
            "AcmeDiamond -> left: AcmeLeft -> shared: AcmeShared: gateway"
        ]
