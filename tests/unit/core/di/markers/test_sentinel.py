"""A marker loom resolves is replaced; one an opaque provider leaks fails on first use."""

from __future__ import annotations

import re
from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, MarkerNotResolved
from loom.core.identity import ANONYMOUS
from loom.core.repository.registration import classify_key
from loom.core.use_case.kinds import MCP, declare_kinds
from loom.core.use_case.markers import Agent, Caller, Mcp

GATEWAY = object()


class Queries:
    """What the use case asks for."""


class GatewayQueries(Queries):
    def __init__(self, gateway: Any = Mcp("acme-crm", include=["find_*"])) -> None:
        self.gateway = gateway


class GatewayResolver:
    def resolve(self, declaration: object, annotation: object) -> object:
        return GATEWAY


def _container() -> LoomContainer:
    container = LoomContainer(classify_key=classify_key)
    declare_kinds(container.kinds)
    container.kinds.bind(MCP, GatewayResolver())
    return container


class TestMarkerSentinel:
    def test_a_class_binding_gets_the_resolvers_object_for_its_constructor_marker(self) -> None:
        container = _container()
        container.register(Queries, GatewayQueries)

        with ExecutionScope(ANONYMOUS):
            queries: Any = container.resolve(Queries)

        assert queries.gateway is GATEWAY

    def test_a_marker_leaked_by_an_opaque_provider_fails_on_first_use(self) -> None:
        container = _container()
        container.register(Queries, lambda: GatewayQueries(), scope=Scope.APPLICATION)
        queries: Any = container.resolve(Queries)

        with pytest.raises(MarkerNotResolved) as caught:
            queries.gateway.call_tool("find_customer")

        message = str(caught.value)
        assert "Mcp('acme-crm', include=['find_*']).call_tool" in message
        assert "register(Port, Impl)" in message

    @pytest.mark.parametrize(
        ("marker", "named"),
        [(Agent("acme-triage"), "Agent('acme-triage')"), (Caller(), "Caller()")],
    )
    def test_every_constructor_marker_names_itself_when_used(self, marker: Any, named: str) -> None:
        with pytest.raises(MarkerNotResolved, match=f"^{re.escape(named + '.id')}"):
            marker.id  # noqa: B018 - attribute access is the behaviour under test

    def test_a_markers_own_fields_read_normally(self) -> None:
        mcp: Any = Mcp("acme-crm", include=["find_*"])
        agent: Any = Agent("acme-triage")

        assert (mcp.server, mcp.include, agent.name) == ("acme-crm", ("find_*",), "acme-triage")
