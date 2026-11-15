"""Acme shipping's adapters and composition roots.

Each adapter declares what it needs in its own constructor: ``Mcp()`` for the
tracking gateway, ``Agent()`` for the notice writer, an ``httpx`` client for
the tracking HTTP API. Each composition root binds one adapter per port.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx

from loom.ai.abc import AgentHandle, McpHandle
from loom.core.di.container import LoomContainer
from loom.core.use_case import Agent, Mcp
from tests.integration.hexagonal.shipping import CustomerNotifier, ShipmentQueries

TRACKING_SERVER = "acme-tracking"
TRACK_TOOL = "track_shipment"
NOTICE_WRITER = "acme-notice-writer"


class GatewayShipmentQueries(ShipmentQueries):
    """Asks the tracking MCP gateway."""

    def __init__(self, gateway: McpHandle = Mcp(TRACKING_SERVER, include=(TRACK_TOOL,))) -> None:
        self._gateway = gateway

    async def status_of(self, shipment_id: str) -> str:
        reply = await self._gateway.call_untyped(TRACK_TOOL, {"shipment_id": shipment_id})
        return str(reply["status"])


class HttpShipmentQueries(ShipmentQueries):
    """Asks the tracking HTTP API."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def status_of(self, shipment_id: str) -> str:
        response = await self._client.get(f"/shipments/{shipment_id}")
        response.raise_for_status()
        return str(response.json()["status"])


class AgentCustomerNotifier(CustomerNotifier):
    """Has the notice-writer agent word the notice."""

    def __init__(self, writer: AgentHandle[str] = Agent(NOTICE_WRITER)) -> None:
        self._writer = writer

    async def notice_for(self, shipment_id: str, status: str) -> str:
        answer = await self._writer.run(f"Tell the customer that {shipment_id} is {status}.")
        return answer.output


def gateway_root(container: LoomContainer) -> None:
    """Bind the ports to the MCP gateway and the notice-writer agent."""
    container.register(ShipmentQueries, GatewayShipmentQueries)
    container.register(CustomerNotifier, AgentCustomerNotifier)


def http_root(client: httpx.AsyncClient) -> Callable[[LoomContainer], None]:
    """Bind the ports to the tracking HTTP API over *client* and the notice-writer agent."""

    def root(container: LoomContainer) -> None:
        container.register_instance(httpx.AsyncClient, client)
        container.register(ShipmentQueries, HttpShipmentQueries)
        container.register(CustomerNotifier, AgentCustomerNotifier)

    return root
