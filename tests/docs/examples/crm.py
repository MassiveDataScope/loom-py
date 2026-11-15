"""An adapter that declares an MCP server and an agent in its own constructor."""

from __future__ import annotations

from typing import Any, Protocol

import msgspec

from loom.ai.abc import AgentHandle, McpHandle
from loom.core.di.container import LoomContainer
from loom.core.use_case import Agent, Mcp, UseCase


class Customer(msgspec.Struct, frozen=True):
    """What the CRM returns for a customer."""

    email: str
    tier: str


class CustomerDirectory(Protocol):
    """The port: the use case knows nothing about MCP or agents."""

    async def find(self, email: str) -> Customer: ...

    async def summarise(self, text: str) -> str: ...


class CrmDirectory:
    """The adapter: declares the MCP server and the agent it uses."""

    def __init__(
        self,
        crm: McpHandle = Mcp("acme-crm", include=["find_*"]),
        summariser: AgentHandle[str] = Agent("acme-summariser"),
    ) -> None:
        self._crm = crm
        self._summariser = summariser

    async def find(self, email: str) -> Customer:
        return await self._crm.call("find_customer", {"email": email}, expect=Customer)

    async def summarise(self, text: str) -> str:
        answer = await self._summariser.run(text)
        return answer.output


class TriageTicket(UseCase[Any, str]):
    """Depends on the port only."""

    def __init__(self, directory: CustomerDirectory) -> None:
        super().__init__()
        self._directory = directory

    async def execute(self, email: str, text: str) -> str:
        customer = await self._directory.find(email)
        summary = await self._directory.summarise(text)
        return f"{customer.tier}: {summary}"


def crm_module(container: LoomContainer) -> None:
    """Bind the adapter behind the port."""
    container.register(CustomerDirectory, CrmDirectory)
