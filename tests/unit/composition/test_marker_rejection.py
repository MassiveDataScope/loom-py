"""Start-up refusal of the ``Agent()`` and ``Mcp()`` markers on an entry point without agents.

An entry point whose recipe declares ``agents=False`` (a Celery task worker,
for instance) never builds an AI runtime, so a use case declaring either
marker is refused when the application is composed, not on its first
execution, with nothing compiled and nothing configured.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition import Application, Project, Recipe
from loom.composition._ai import MarkerDeclarations
from loom.core.stores.profile import SERVER_PROFILE
from loom.core.use_case import Agent, Mcp
from loom.core.use_case.use_case import UseCase
from tests.helpers.ai import declared_markers
from tests.helpers.composition import ProjectFactory
from tests.unit.composition.doubles import SendReceiptUseCase


class AcmeNotifyIncident(UseCase[object, object]):
    """Declares an agent marker."""

    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class AcmeLookUpKnowledge(UseCase[object, object]):
    """Declares a server marker."""

    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> object:
        return gateway


class AcmeAgentAndServer(UseCase[object, object]):
    """Declares both markers."""

    async def execute(
        self,
        triage: AgentHandle[dict] = Agent("triage"),
        gateway: McpHandle = Mcp("knowledge", include=["search_*"]),
    ) -> object:
        return triage, gateway


class _WithoutAgents:
    """An entry point hosting no agents, compiling a use case that declares one."""

    def recipe(self, source: Project) -> Recipe:
        discovered = source.discover()
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=dataclasses.replace(
                discovered, use_cases=(*discovered.use_cases, AcmeNotifyIncident)
            ),
            default_store_backend="sqlalchemy",
            agents=False,
        )

    def bind(self, application: Application) -> Application:
        return application


class TestMarkerRejection:
    def test_no_marker_does_not_raise(self) -> None:
        declared = declared_markers(SendReceiptUseCase)

        declared.verify({})
        assert declared.mcp_grants({}) == ()

    @pytest.mark.parametrize(
        ("use_case", "refuse", "code", "names"),
        [
            (
                AcmeNotifyIncident,
                MarkerDeclarations.verify,
                AgentErrorCode.AGENT_MARKER_UNKNOWN,
                ("triage",),
            ),
            (
                AcmeLookUpKnowledge,
                MarkerDeclarations.mcp_grants,
                AgentErrorCode.MCP_MARKER_UNKNOWN,
                ("gateway", "knowledge"),
            ),
        ],
        ids=["agent", "mcp"],
    )
    def test_a_marker_is_refused_naming_the_use_case_and_parameter(
        self,
        use_case: type[UseCase[object, object]],
        refuse: Callable[[MarkerDeclarations, dict[str, Any]], object],
        code: AgentErrorCode,
        names: tuple[str, ...],
    ) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            refuse(declared_markers(use_case), {})

        message = str(excinfo.value)
        assert {issue.code for issue in excinfo.value.issues} == {code}
        assert use_case.__name__ in message
        assert all(name in message for name in names)
        assert "none" in message

    def test_each_marker_is_refused_by_its_own_check(self) -> None:
        declared = declared_markers(AcmeAgentAndServer)

        with pytest.raises(AgentCompilationError) as agents:
            declared.verify({})
        with pytest.raises(AgentCompilationError) as servers:
            declared.mcp_grants({})

        assert [issue.code for issue in agents.value.issues] == [
            AgentErrorCode.AGENT_MARKER_UNKNOWN
        ]
        assert [issue.code for issue in servers.value.issues] == [AgentErrorCode.MCP_MARKER_UNKNOWN]

    def test_composition_refuses_the_marker_for_any_adapter(
        self, fixture_project: ProjectFactory
    ) -> None:
        project = fixture_project()
        adapter = _WithoutAgents()

        with pytest.raises(AgentCompilationError, match="AcmeNotifyIncident"):
            Project.load(project, environment="test").compose(adapter)
