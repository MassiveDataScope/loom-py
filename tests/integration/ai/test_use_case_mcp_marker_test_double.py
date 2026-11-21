"""No-network coverage of the ``Mcp()`` marker's own doc example (spec 015, T601/T604).

Mirrors ``test_agent_marker_test_double.py``: the exact use case
``docs/rest/use-case-dsl.md`` (section "Mcp marker — reaching an MCP server
directly") and ``docs/ai/mcp.md`` ("Reach a server directly, with no agent in
the middle") show verbatim, compiled by a real ``UseCaseCompiler`` and run by
a real ``UseCaseExecutor``, with the ``Mcp()`` marker resolved to a
:class:`~loom.testing.runner.McpHandleDouble` instead of a real
``AgentHost``. Editing either doc's example without this test is a gap
the next review will catch.
"""

from __future__ import annotations

import msgspec
import pytest

from loom.ai.abc import McpHandle
from loom.core.identity import Identity
from loom.core.use_case import Caller, Mcp, UseCase
from loom.testing.runner import McpHandleDouble, UseCaseTest

_SERVER = "runbooks"
_CALLER = Identity(subject="on-call-ada", roles=("responder",), mechanism="test")


class RunbookLookup(msgspec.Struct, frozen=True):
    """What the example use case returns."""

    incident_id: str
    requested_by: str
    runbook_title: str


class LookUpRunbookUseCase(UseCase[object, RunbookLookup]):
    """The complete example ``use-case-dsl.md`` and ``mcp.md`` show verbatim."""

    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        runbooks: McpHandle = Mcp(_SERVER, include=("search_incident",)),
    ) -> RunbookLookup:
        result = await runbooks.call_untyped("search_incident", {"incident_id": incident_id})
        return RunbookLookup(
            incident_id=incident_id,
            requested_by=caller.require_subject(),
            runbook_title=str(result["title"]),
        )


class TestTheMarkerExampleRunsWithoutNetworkOrMcpServer:
    async def test_the_result_reflects_the_scripted_response(self) -> None:
        double = McpHandleDouble(_SERVER).with_tools("search_incident")
        double.on_call_untyped("search_incident", {"title": "checkout rollback runbook"})

        result = await (
            UseCaseTest(LookUpRunbookUseCase())
            .with_caller(_CALLER)
            .with_mcp(_SERVER, double)
            .with_params(incident_id="INC-100")
            .run()
        )

        assert result == RunbookLookup(
            incident_id="INC-100",
            requested_by="on-call-ada",
            runbook_title="checkout rollback runbook",
        )

    async def test_the_double_records_the_call(self) -> None:
        double = McpHandleDouble(_SERVER).with_tools("search_incident")
        double.on_call_untyped("search_incident", {"title": "checkout rollback runbook"})

        await (
            UseCaseTest(LookUpRunbookUseCase())
            .with_caller(_CALLER)
            .with_mcp(_SERVER, double)
            .with_params(incident_id="INC-101")
            .run()
        )

        assert double.calls[0].tool == "search_incident"
        assert double.calls[0].arguments["incident_id"] == "INC-101"


class TestWithoutADoubleRegisteredStartupFailsClosed:
    async def test_declaring_the_marker_without_a_double_fails_with_a_clear_error(self) -> None:
        # The docs promise the refusal names both, so pin both.
        runner = (
            UseCaseTest(LookUpRunbookUseCase())
            .with_caller(_CALLER)
            .with_params(incident_id="INC-102")
        )
        with pytest.raises(RuntimeError) as refusal:
            await runner.run()
        assert "LookUpRunbookUseCase" in str(refusal.value)
        assert _SERVER in str(refusal.value)
