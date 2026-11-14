"""The walk starts at the roots: what they reach is analysed, nothing else is."""

from __future__ import annotations

import pytest

from loom.core.di.graph import (
    LENIENT_REFUSALS,
    ClassNode,
    DependencyGraphError,
    KeyNode,
)
from loom.core.di.scope import Scope
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCall
from tests.unit.core.di.graph.doubles import AcmeClock, AcmeLedger, AcmeReport


class AcmeOrphan:
    """Bound, needs a missing key, and no root reaches it."""

    def __init__(self, missing: AcmeLedger) -> None:
        self.missing = missing


class AcmeExport:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class TestReachableRoots:
    def test_the_roots_and_what_they_reach_are_the_reachable_nodes(self) -> None:
        built = acme.container()
        built.register(AcmeLedger, AcmeLedger)
        built.register(AcmeClock, AcmeClock)

        analysis = acme.analysed(built, AcmeReport, AcmeExport, AcmeReport)

        assert analysis.roots == (ClassNode(AcmeReport), ClassNode(AcmeExport))
        assert analysis.reachable == {
            ClassNode(AcmeReport),
            ClassNode(AcmeExport),
            KeyNode(AcmeLedger),
            KeyNode(AcmeClock),
        }

    def test_a_binding_no_root_reaches_is_not_analysed(self) -> None:
        built = acme.container()
        built.register(AcmeClock, AcmeClock)
        built.register(AcmeOrphan, AcmeOrphan, scope=Scope.APPLICATION)

        analysis = acme.analysed(built, AcmeExport)

        assert KeyNode(AcmeOrphan) not in analysis.reachable

    def test_an_instance_binding_has_no_edges(self) -> None:
        built = acme.container()
        built.register_instance(AcmeLedger, AcmeLedger(AcmeClock()))
        analysis = acme.analysed(built, AcmeReport)

        assert analysis.graph.edges(KeyNode(AcmeLedger)) == ()
        assert KeyNode(AcmeClock) not in analysis.reachable


class AcmeSettle:
    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names


class AcmeClose:
    def __init__(self, settle: AcmeCall[AcmeSettle]) -> None:
        self.settle = settle


class TestCallTargetRoots:
    def test_a_root_first_reached_as_a_call_target_is_still_checked_as_a_root(self) -> None:
        """Under the lenient rules refusal 6 is a root's only, so the root check must still run."""
        roots = [ClassNode(AcmeClose), ClassNode(AcmeSettle)]

        with pytest.raises(DependencyGraphError, match=r"^AcmeSettle: names: tuple\[str, \.\.\.\]"):
            acme.container().graph.analyse(roots, LENIENT_REFUSALS)
