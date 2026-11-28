"""Jobs calling the tool server through an ``Mcp()`` marker."""

from __future__ import annotations

import asyncio

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentRunError
from loom.core.job.job import Job
from loom.core.use_case import Mcp
from tests.integration.ai.freeze.t06_app.shared import SERVER, TOOLS, noted


class CallToolJob(Job[None]):
    """The ``Mcp()`` marker, from a job of an SQS batch."""

    async def execute(
        self, tool: str, text: str, orders: McpHandle = Mcp(SERVER, include=TOOLS)
    ) -> None:
        try:
            await orders.call_untyped(tool, {"text": text})
        except AgentRunError as error:
            raise noted(error) from None


class CallTwiceJob(Job[None]):
    """Two ``echo`` calls at once, from a job of an SQS batch."""

    async def execute(self, orders: McpHandle = Mcp(SERVER, include=TOOLS)) -> None:
        await asyncio.gather(*(orders.call_untyped("echo", {"text": str(n)}) for n in range(2)))
