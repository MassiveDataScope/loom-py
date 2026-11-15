"""A port, its adapters and the use case depending on it, shared by the doubles tests."""

from __future__ import annotations

from typing import Any, Protocol

from loom.ai.abc import AgentHandle, McpHandle
from loom.core.use_case.markers import Agent, Mcp
from loom.core.use_case.use_case import UseCase


class Triage(Protocol):
    async def triage(self, incident: str) -> str: ...


class AgentTriage:
    built = 0

    def __init__(self, agent: AgentHandle[str] = Agent("triage")) -> None:
        type(self).built += 1
        self._agent = agent

    async def triage(self, incident: str) -> str:
        answer = await self._agent.run(incident)
        return str(answer.output)


class McpTriage:
    def __init__(self, runbooks: McpHandle = Mcp("runbooks", include=("search",))) -> None:
        self._runbooks = runbooks

    async def triage(self, incident: str) -> str:
        result = await self._runbooks.call_untyped("search", {"incident": incident})
        return str(result["title"])


class FakeTriage:
    def __init__(self) -> None:
        self.incidents: list[str] = []

    async def triage(self, incident: str) -> str:
        self.incidents.append(incident)
        return "fake"


class TriageUseCase(UseCase[Any, str]):
    def __init__(self, triage: Triage) -> None:
        self._triage = triage

    async def execute(self, incident: str) -> str:
        return await self._triage.triage(incident)


class AgentUseCase(UseCase[Any, object]):
    async def execute(self, triage: AgentHandle[str] = Agent("triage")) -> object:
        return triage


class McpUseCase(UseCase[Any, object]):
    async def execute(self, runbooks: McpHandle = Mcp("runbooks", include=("*",))) -> object:
        return runbooks


class SearchOnlyUseCase(UseCase[Any, tuple[str, ...]]):
    async def execute(
        self, runbooks: McpHandle = Mcp("runbooks", include=("search",))
    ) -> tuple[str, ...]:
        return runbooks.tools()
