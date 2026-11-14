"""Start-up refusal of the ``Agent()`` marker without agents (spec 014, T402).

An entry point whose recipe declares ``agents=False`` (a Celery task worker,
for instance) never builds an AI runtime, so a use case declaring
``Agent()`` must be refused when the application is composed, not on its
first execution. ``reject_ai_markers`` reuses the check an agent-hosting
entry point runs, wired here with no compiled agent plans at all.
"""

from __future__ import annotations

import dataclasses

import pytest

from loom.ai.abc import AgentHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition import Application, CompositionSource, Recipe, build_application
from loom.composition._ai import reject_ai_markers
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.use_case import Agent
from loom.core.use_case.use_case import UseCase
from tests.helpers.composition import ProjectFactory
from tests.unit.composition.doubles import SendReceiptUseCase, kernel_runtime


class NotifyIncidentUseCase(UseCase[object, object]):
    """Declares an agent marker; unreachable from any worker in this suite."""

    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class TestNoUseCaseDeclaresTheMarker:
    def test_no_marker_does_not_raise(self) -> None:
        kernel = kernel_runtime(SendReceiptUseCase)

        reject_ai_markers((SendReceiptUseCase,), kernel.compiler, kernel.registry)  # no raise


class TestOneUseCaseDeclaresTheMarker:
    def test_worker_startup_is_rejected_naming_use_case_and_parameter(self) -> None:
        kernel = kernel_runtime(NotifyIncidentUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            reject_ai_markers((NotifyIncidentUseCase,), kernel.compiler, kernel.registry)

        message = str(excinfo.value)
        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.AGENT_MARKER_UNKNOWN}
        assert "NotifyIncidentUseCase" in message
        assert "triage" in message

    def test_no_compiled_plan_ever_reports_every_agent_as_unknown(self) -> None:
        """No plan is ever compiled here (T402): every declared agent is unknown."""
        kernel = kernel_runtime(NotifyIncidentUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            reject_ai_markers((NotifyIncidentUseCase,), kernel.compiler, kernel.registry)

        assert "none" in str(excinfo.value)


class _WithoutAgents:
    """An entry point hosting no agents, compiling a use case that declares one."""

    def recipe(self, source: CompositionSource) -> Recipe:
        discovered = source.discover()
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=dataclasses.replace(
                discovered, use_cases=(*discovered.use_cases, NotifyIncidentUseCase)
            ),
            default_backend="sqlalchemy",
            agents=False,
        )

    def bind(self, application: Application) -> Application:
        return application


class TestEveryEntryPointWithoutAgents:
    def test_composition_refuses_the_marker_for_any_adapter(
        self, fixture_project: ProjectFactory
    ) -> None:
        config = fixture_project()
        adapter = _WithoutAgents()

        with pytest.raises(AgentCompilationError, match="NotifyIncidentUseCase"):
            build_application(config, adapter=adapter, environment="test")
