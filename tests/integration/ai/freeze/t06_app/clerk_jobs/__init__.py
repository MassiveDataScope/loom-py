"""The job running the agent."""

from __future__ import annotations

from loom.ai.abc import AgentHandle
from loom.ai.errors import AgentRunError
from loom.core.job.job import Job
from loom.core.use_case import Agent
from tests.integration.ai.freeze.t06_app.shared import AGENT, noted


class AskClerkJob(Job[None]):
    """The agent, from a job of an SQS batch."""

    async def execute(self, tool: str, clerk: AgentHandle = Agent(AGENT)) -> None:
        try:
            await clerk.run(tool)
        except AgentRunError as error:
            raise noted(error) from None
