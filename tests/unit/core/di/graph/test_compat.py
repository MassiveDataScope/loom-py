"""A class ``Impl()`` builds today is never refused and never autowired.

Each row binds a class the container builds today with no argument; the
analysis must accept it and give it no edge, whatever its parameters are
annotated with, under the classifier the composition passes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraph, KeyNode, analyse
from loom.core.di.scope import Scope
from loom.core.model import LoomStruct
from loom.core.repository.abc import RepoFor
from loom.core.repository.dependency import classify_key
from tests.unit.core.doubles import AcmeClock


class AcmePort:
    pass


class AcmeConsumer:
    def __init__(self, port: AcmePort) -> None:
        self.port = port


class AcmeBare:
    pass


_CLOCK = AcmeClock()


class AcmeDefaulted:
    def __init__(self, clock: AcmeClock = _CLOCK, retries: int = 3) -> None:
        self.clock = clock
        self.retries = retries


class AcmeVariadic:
    def __init__(self, *clocks: AcmeClock, **options: AcmeClock) -> None:
        self.clocks = clocks
        self.options = options


class AcmeForwardReference:
    def __init__(self, clock: AcmeNowhere | None = None) -> None:  # type: ignore[name-defined]  # noqa: F821
        self.clock = clock


class AcmeRecord(LoomStruct):
    pass


class AcmeDefaultedRepository:
    def __init__(self, records: RepoFor[AcmeRecord] | None = None) -> None:
        self.records = records


@dataclass
class AcmeRegionSettings:
    region: str = "eu-west-1"


def _unreadable(self: object) -> None:
    del self


_unreadable.__signature__ = "unreadable"  # type: ignore[attr-defined]


class AcmeUnreadable:
    __init__ = _unreadable


class AcmeBuildingMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        return super().__call__(AcmeClock())


class AcmeMetaBuilt(metaclass=AcmeBuildingMeta):
    """Built by its metaclass, which supplies the clock its constructor requires."""

    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeUnbound:
    pass


class AcmeNewBuilt:
    """Built by its ``__new__``, which returns a clock, so its constructor never runs."""

    def __new__(cls) -> Any:
        return AcmeClock()

    def __init__(self, unbound: AcmeUnbound) -> None:
        self.unbound = unbound


_BUILT_TODAY: dict[str, Callable[[], Any]] = {
    "no-constructor": AcmeBare,
    "defaults": AcmeDefaulted,
    "variadic": AcmeVariadic,
    "unresolvable-hints": AcmeForwardReference,
    "defaulted-repository": AcmeDefaultedRepository,
    "dataclass": AcmeRegionSettings,
    "unreadable-signature": AcmeUnreadable,
    "metaclass-call": AcmeMetaBuilt,
    "custom-new": AcmeNewBuilt,
    "builtin": dict,
    "provider": lambda: AcmeDefaulted(retries=1),
}


class TestGraphCompatibility:
    @pytest.mark.parametrize("scope", [Scope.APPLICATION, Scope.REQUEST])
    @pytest.mark.parametrize("built_today", list(_BUILT_TODAY))
    def test_a_class_built_today_is_accepted_and_given_no_edge(
        self, built_today: str, scope: Scope
    ) -> None:
        container = LoomContainer(classify_key=classify_key)
        container.register(AcmeClock, AcmeClock)
        container.register(AcmePort, _BUILT_TODAY[built_today], scope=Scope.APPLICATION)
        graph = DependencyGraph(container)

        analysis = analyse(graph, [ClassNode(AcmeConsumer)])

        assert graph.edges(KeyNode(AcmePort)) == ()
        assert analysis.warnings == ()
        assert container.resolve(AcmePort) is not None
