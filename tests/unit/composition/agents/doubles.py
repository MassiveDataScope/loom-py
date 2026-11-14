"""Doubles shared by the graph-walk tests: ports, adapters and use cases declaring markers.

``AcmeMarket`` reaches ``Mcp('gateway')`` only through its ``AcmeQueries``
port, whose ``AcmeGatewayQueries`` adapter declares it. The diamond reaches
``AcmeShared`` through two paths. ``AcmeDelegating`` calls ``AcmeValuate``
through the graph tests' generic ``AcmeCall`` kind, so the walk is shown to
follow call edges without naming one.
"""

from __future__ import annotations

from typing import Any

from loom.core.use_case.markers import Agent, Mcp
from loom.core.use_case.use_case import UseCase
from tests.unit.core.di.graph.acme import AcmeCall


class AcmeQueries:
    """The port a use case asks for."""


class AcmeGatewayQueries(AcmeQueries):
    def __init__(self, gateway: Any = Mcp("gateway", include=["run_query"])) -> None:
        self.gateway = gateway


class AcmeFakeQueries(AcmeQueries):
    pass


def queries_through(gateway: AcmeGatewayQueries) -> AcmeQueries:
    """A factory whose parameter the walk goes through."""
    return gateway


class AcmeMarket(UseCase[object, str]):
    def __init__(self, queries: AcmeQueries) -> None:
        self.queries = queries

    async def execute(self, analyst: Any = Agent("analyst")) -> str:
        return "market"


class AcmeQuiet(UseCase[object, str]):
    """Reaches the gateway through its port and declares nothing in ``execute()``."""

    def __init__(self, queries: AcmeQueries) -> None:
        self.queries = queries

    async def execute(self) -> str:
        return "quiet"


class AcmeShared:
    def __init__(self, gateway: Any = Mcp("gateway", include=["run_query"])) -> None:
        self.gateway = gateway


class AcmeLeft:
    def __init__(self, shared: AcmeShared) -> None:
        self.shared = shared


class AcmeRight:
    def __init__(self, shared: AcmeShared) -> None:
        self.shared = shared


class AcmeDiamond(UseCase[object, str]):
    def __init__(self, left: AcmeLeft, right: AcmeRight) -> None:
        self.left, self.right = left, right

    async def execute(self) -> str:
        return "diamond"


class AcmeValuate(UseCase[object, str]):
    """Declares a marker in its constructor and one in ``execute()``."""

    def __init__(self, gateway: Any = Mcp("pricing", include=["quote"])) -> None:
        self.gateway = gateway

    async def execute(self, valuer: Any = Agent("valuer")) -> str:
        return "valuate"


class AcmeDelegating(UseCase[object, str]):
    def __init__(self, valuate: AcmeCall[AcmeValuate]) -> None:
        self.valuate = valuate

    async def execute(self) -> str:
        return "delegating"
