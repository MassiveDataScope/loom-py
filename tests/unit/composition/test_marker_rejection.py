"""Start-up refusal of the ``Agent()`` and ``Mcp()`` markers on an entry point without agents.

An entry point whose recipe declares ``agents=False`` (a Celery task worker,
for instance) never builds an AI runtime, so a use case declaring either
marker is refused when the application is composed, not on its first
execution, with nothing compiled and nothing configured.
"""

from __future__ import annotations

import dataclasses

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition import Application, CompositionSource, Recipe, build_application
from loom.core.persistence.profile import SERVER_PROFILE
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

    def recipe(self, source: CompositionSource) -> Recipe:
        discovered = source.discover()
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=dataclasses.replace(
                discovered, use_cases=(*discovered.use_cases, AcmeNotifyIncident)
            ),
            default_backend="sqlalchemy",
            agents=False,
        )

    def bind(self, application: Application) -> Application:
        return application


class TestMarkerRejection:
    def test_no_marker_does_not_raise(self) -> None:
        declared_markers(SendReceiptUseCase).verify({}, {})

    @pytest.mark.parametrize(
        ("use_case", "code", "names"),
        [
            (AcmeNotifyIncident, AgentErrorCode.AGENT_MARKER_UNKNOWN, ("triage",)),
            (AcmeLookUpKnowledge, AgentErrorCode.MCP_MARKER_UNKNOWN, ("gateway", "knowledge")),
        ],
        ids=["agent", "mcp"],
    )
    def test_a_marker_is_refused_naming_the_use_case_and_parameter(
        self, use_case: type[UseCase[object, object]], code: AgentErrorCode, names: tuple[str, ...]
    ) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            declared_markers(use_case).verify({}, {})

        message = str(excinfo.value)
        assert {issue.code for issue in excinfo.value.issues} == {code}
        assert use_case.__name__ in message
        assert all(name in message for name in names)
        assert "none" in message

    def test_both_markers_are_refused_in_one_error_agent_first(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            declared_markers(AcmeAgentAndServer).verify({}, {})

        assert [issue.code for issue in excinfo.value.issues] == [
            AgentErrorCode.AGENT_MARKER_UNKNOWN,
            AgentErrorCode.MCP_MARKER_UNKNOWN,
        ]

    def test_composition_refuses_the_marker_for_any_adapter(
        self, fixture_project: ProjectFactory
    ) -> None:
        with pytest.raises(AgentCompilationError, match="AcmeNotifyIncident"):
            build_application(fixture_project(), adapter=_WithoutAgents(), environment="test")
