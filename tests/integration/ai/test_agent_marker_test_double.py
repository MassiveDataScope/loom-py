"""No-network, no-model, no-database coverage of the ``Agent()`` marker (spec 014, PR4).

Covers point 7 of the standing integration requirement in
``specs/014-model-as-actor/tasks.md`` and is T401's own closing criterion: the
complete example use case from ``markers.md`` — a query through the granted
``sql`` view, a tool call through the granted ``mcp`` view, a model run, and a
branch on the decoded answer — compiled by a real ``UseCaseCompiler`` and run
by a real ``RuntimeExecutor``, with the ``Agent()`` marker resolved to an
:class:`~loom.testing.runner.AgentHandleDouble` instead of a real
``AgentRuntime``. Nothing here imports ``loom.ai.execution``, opens a socket, or
touches a database — the double is the entire "engine" this test exercises.

This is also the example ``docs/rest/use-case-dsl.md`` (section "Agent marker
— reaching a named agent") shows verbatim: editing one without the other is a
gap the next review will catch.
"""

from __future__ import annotations

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.core.identity import Identity
from loom.core.use_case import Agent, Caller, UseCase
from loom.testing.runner import AgentHandleDouble, UseCaseTest

_AGENT_NAME = "incident-triage"
_CALLER = Identity(subject="on-call-ada", roles=("responder",), mechanism="test")


class SeverityAssessment(msgspec.Struct, frozen=True):
    """The artefact's declared output shape, mirroring ``markers.md``."""

    severity: int


class IncidentReport(msgspec.Struct, frozen=True):
    """What the example use case returns."""

    incident_id: str
    caller: str
    runbook_title: str
    severity: int
    escalated: bool


class TriageIncidentUseCase(UseCase[object, IncidentReport]):
    """The complete example from ``markers.md``: query, tool call, model run, branch."""

    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        triage: AgentHandle[SeverityAssessment] = Agent(_AGENT_NAME),
    ) -> IncidentReport:
        obs = triage.sql("observability_readonly")
        deploys = await obs.query(
            "select service, status from deploys where incident = :id",
            parameters={"id": incident_id},
        )

        runbooks = triage.mcp("runbooks")
        runbook = await runbooks.call(
            "search_incident",
            {"incident_id": incident_id, "recent_deploy": deploys[0]["service"]},
            expect=dict,
        )

        assessment = await triage.run(f"Assess {incident_id}.")

        return IncidentReport(
            incident_id=incident_id,
            caller=caller.require_subject(),
            runbook_title=str(runbook["title"]),
            severity=assessment.output.severity,
            escalated=assessment.output.severity >= 4,
        )


def _triage_double(severity: int) -> AgentHandleDouble:
    """Build a fully scripted double: no answer this use case needs is left unset."""
    double = AgentHandleDouble(_AGENT_NAME).on_run(SeverityAssessment(severity=severity))
    double.sql("observability_readonly").on_query([{"service": "checkout", "status": "unhealthy"}])
    double.mcp("runbooks").on_call("search_incident", {"title": "checkout rollback runbook"})
    return double


class TestTheFullUseCaseRunsWithoutNetworkModelOrDatabase:
    async def test_the_result_reflects_the_scripted_responses(self) -> None:
        result = await (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(_CALLER)
            .with_agent(_AGENT_NAME, _triage_double(severity=5))
            .with_params(incident_id="INC-100")
            .run()
        )

        assert result == IncidentReport(
            incident_id="INC-100",
            caller="on-call-ada",
            runbook_title="checkout rollback runbook",
            severity=5,
            escalated=True,
        )

    async def test_a_low_severity_does_not_escalate(self) -> None:
        result = await (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(_CALLER)
            .with_agent(_AGENT_NAME, _triage_double(severity=1))
            .with_params(incident_id="INC-101")
            .run()
        )

        assert result.escalated is False

    async def test_the_double_records_the_sql_query_and_the_tool_call(self) -> None:
        double = _triage_double(severity=2)

        await (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(_CALLER)
            .with_agent(_AGENT_NAME, double)
            .with_params(incident_id="INC-102")
            .run()
        )

        sql_calls = double.sql("observability_readonly").calls
        assert sql_calls[0].parameters == {"id": "INC-102"}

        tool_calls = double.mcp("runbooks").calls
        assert tool_calls[0].tool == "search_incident"
        assert tool_calls[0].arguments["incident_id"] == "INC-102"

    async def test_the_double_records_the_models_run_prompt(self) -> None:
        double = _triage_double(severity=3)

        await (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(_CALLER)
            .with_agent(_AGENT_NAME, double)
            .with_params(incident_id="INC-103")
            .run()
        )

        assert double.run_calls[0].prompt == "Assess INC-103."


class TestWithoutADoubleRegisteredStartupFailsClosed:
    async def test_declaring_the_marker_without_a_double_fails_with_a_clear_error(self) -> None:
        runner = (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(_CALLER)
            .with_params(incident_id="INC-104")
        )
        with pytest.raises(RuntimeError, match=_AGENT_NAME):
            await runner.run()
