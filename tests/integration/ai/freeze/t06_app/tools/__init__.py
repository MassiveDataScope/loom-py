"""Use cases and routes calling the tool server through an ``Mcp()`` marker."""

from __future__ import annotations

import asyncio
from typing import Any

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentRunError
from loom.core.use_case import Input, Mcp, UseCase
from loom.rest.model import RestInterface, RestRoute
from tests.integration.ai.freeze.t06_app.shared import GAP_TAKEN, SERVER, TOOLS, Say, noted


class CallTool(UseCase[object, dict[str, Any]]):
    """Calls one tool through an ``Mcp()`` marker: no agent, no model."""

    async def execute(
        self, cmd: Say = Input(), orders: McpHandle = Mcp(SERVER, include=TOOLS)
    ) -> dict[str, Any]:
        try:
            return dict(await orders.call_untyped(cmd.tool, {"text": cmd.text}))
        except AgentRunError as error:
            raise noted(error) from None


class CallTwice(UseCase[object, int]):
    """Calls ``echo`` twice at once through an ``Mcp()`` marker."""

    async def execute(self, orders: McpHandle = Mcp(SERVER, include=TOOLS)) -> int:
        calls = (orders.call_untyped("echo", {"text": str(n)}) for n in range(2))
        return len(await asyncio.gather(*calls))


class EchoDuringSlow(UseCase[object, int]):
    """Calls ``echo`` while a ``slow`` call is in flight, once the test moved the clock."""

    async def execute(self, orders: McpHandle = Mcp(SERVER, include=TOOLS)) -> int:
        slow = asyncio.ensure_future(orders.call_untyped("slow", {"text": "slow"}))
        while not GAP_TAKEN.is_set():
            await asyncio.sleep(0.01)
        await orders.call_untyped("echo", {"text": "during"})
        await slow
        return 2


class ToolsInterface(RestInterface[object]):
    prefix = "/mcp"
    routes = (
        RestRoute(use_case=CallTool, method="POST", path="/call"),
        RestRoute(use_case=CallTwice, method="POST", path="/twice"),
        RestRoute(use_case=EchoDuringSlow, method="POST", path="/inflight"),
    )
