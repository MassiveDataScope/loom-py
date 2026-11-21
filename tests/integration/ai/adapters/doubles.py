"""Ports, adapters and use cases whose AI declarations live on an adapter, not in ``execute()``.

``AcmeMarket`` names two ports. ``AcmeGatewayQuotes`` implements ``AcmeQuotes``
over ``Mcp('pricing')``; ``AcmeAnalystAdvice`` implements ``AcmeAdvice`` over
``Agent('analyst')``. Bound to their ``AcmeFake*`` instances instead, the
use case reaches neither. ``AcmeHistory`` declares the same server in its
own ``execute()`` with another filter, so two views share one session.
"""

from __future__ import annotations

from typing import Any

from loom.core.job.callback import JobCallback
from loom.core.use_case import Agent, Mcp, UseCase

SERVER = "pricing"
AGENT = "analyst"
UNUSED_AGENT = "auditor"
PATH_TO_GATEWAY = "AcmeMarket -> quotes: AcmeQuotes: gateway"
PATH_TO_ANALYST = "AcmeMarket -> advice: AcmeAdvice: analyst"
PATH_FROM_CALLBACK = "AcmeAuditCallback -> quotes: AcmeQuotes: gateway"


class AcmeQuotes:
    """The port ``AcmeMarket`` asks quotes from."""

    async def quote(self) -> object:
        raise NotImplementedError


class AcmeGatewayQuotes(AcmeQuotes):
    def __init__(self, gateway: Any = Mcp(SERVER, include=("quote",))) -> None:
        self.gateway = gateway

    async def quote(self) -> object:
        return self.gateway.tools(), await self.gateway.call_untyped("quote", {})


class AcmeTypoQuotes(AcmeQuotes):
    def __init__(self, gateway: Any = Mcp("pricng", include=("quote",))) -> None:
        self.gateway = gateway


class AcmeNarrowQuotes(AcmeQuotes):
    def __init__(self, gateway: Any = Mcp(SERVER, include=("no_such_tool",))) -> None:
        self.gateway = gateway


class AcmeFakeQuotes(AcmeQuotes):
    async def quote(self) -> object:
        return "fake"


class AcmeAdvice:
    """The port ``AcmeMarket`` asks advice from."""


class AcmeAnalystAdvice(AcmeAdvice):
    def __init__(self, analyst: Any = Agent(AGENT)) -> None:
        self.analyst = analyst


class AcmeTypoAdvice(AcmeAdvice):
    def __init__(self, analyst: Any = Agent("analist")) -> None:
        self.analyst = analyst


class AcmeFakeAdvice(AcmeAdvice):
    pass


class AcmeMarket(UseCase[object, object]):
    def __init__(self, quotes: AcmeQuotes, advice: AcmeAdvice) -> None:
        self.quotes, self.advice = quotes, advice

    async def execute(self) -> object:
        return await self.quotes.quote()


class AcmeHistory(UseCase[object, object]):
    async def execute(self, gateway: Any = Mcp(SERVER, include=("history",))) -> object:
        return gateway.tools(), await gateway.call_untyped("history", {})


class AcmeAuditCallback(JobCallback):
    """A job callback built with the quotes port: a root of its own, not a use case."""

    def __init__(self, quotes: AcmeQuotes) -> None:
        self.quotes = quotes

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context
