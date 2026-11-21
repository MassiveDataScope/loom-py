"""The use case and route running the agent."""

from __future__ import annotations

from loom.ai.abc import AgentHandle
from loom.ai.errors import AgentRunError
from loom.core.use_case import Agent, Input, UseCase
from loom.rest.model import RestInterface, RestRoute
from tests.integration.ai.freeze.t06_app.shared import AGENT, Say, noted


class AskClerk(UseCase[object, str]):
    """Runs the agent, whose model calls the tool the prompt names."""

    async def execute(self, cmd: Say = Input(), clerk: AgentHandle = Agent(AGENT)) -> str:
        try:
            await clerk.run(cmd.tool)
        except AgentRunError as error:
            raise noted(error) from None
        return cmd.tool


class ClerkInterface(RestInterface[object]):
    prefix = "/mcp"
    routes = (RestRoute(use_case=AskClerk, method="POST", path="/ask"),)
