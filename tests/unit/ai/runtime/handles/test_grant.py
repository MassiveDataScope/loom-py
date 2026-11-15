"""The ``Mcp()`` grant is looked up per call, and no handle serves a caller with no subject."""

from __future__ import annotations

from typing import Any

import pytest

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import McpMarkerResolver
from loom.core.execution import ExecutionScope
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case import Mcp

from .doubles import ALICE, HANDLE_CALLS, NO_SUBJECT, SERVER, TOOL, AcmeSqlService


@pytest.fixture
def lookups(runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every server the runtime was asked the ``Mcp()`` grant of."""
    asked: list[str] = []
    lookup = runtime.use_case_mcp_grant

    def recording(server: str) -> Any:
        asked.append(server)
        return lookup(server)

    monkeypatch.setattr(runtime, "use_case_mcp_grant", recording)
    return asked


class TestTheMcpGrant:
    async def test_is_looked_up_on_every_call_and_never_on_resolve(
        self, runtime: AgentRuntime, lookups: list[str]
    ) -> None:
        crm = McpMarkerResolver(runtime, observability=None).resolve(
            Mcp(SERVER, include=["find_*"]), McpHandle
        )
        resolved = list(lookups)

        async with runtime:
            with ExecutionScope(ALICE):
                await crm.call_untyped(TOOL, {})
                await crm.call_untyped(TOOL, {})

        assert resolved == []
        assert lookups == [SERVER, SERVER]

    async def test_a_server_no_use_case_was_granted_is_refused_with_no_call(
        self, make_runtime: Any, backend_calls: Any
    ) -> None:
        runtime = make_runtime(use_case_servers=())
        crm = McpMarkerResolver(runtime, observability=None).resolve(
            Mcp(SERVER, include=["find_*"]), McpHandle
        )

        async with runtime:
            with ExecutionScope(ALICE), pytest.raises(AgentRunError) as refused:
                await crm.call_untyped(TOOL, {})

        assert refused.value.code is AgentRunErrorCode.MCP_SERVER_NOT_GRANTED
        assert SERVER in str(refused.value)
        assert backend_calls() == 0

    async def test_a_granted_server_that_never_connected_keeps_tool_unavailable(
        self, make_runtime: Any
    ) -> None:
        runtime = make_runtime(reachable=False)
        crm = McpMarkerResolver(runtime, observability=None).resolve(
            Mcp(SERVER, include=["find_*"]), McpHandle
        )

        async with runtime:
            with ExecutionScope(ALICE):
                assert crm.tools() == ()
                with pytest.raises(AgentRunError) as untyped:
                    await crm.call_untyped(TOOL, {})
                with pytest.raises(AgentRunError) as typed:
                    await crm.call(TOOL, {}, expect=dict)

        assert untyped.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE
        assert typed.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE

    @pytest.mark.parametrize("reachable", [True, False], ids=["reachable", "unreachable"])
    async def test_a_caller_with_no_subject_learns_nothing_of_the_server(
        self, make_runtime: Any, reachable: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = make_runtime(reachable=reachable)
        crm = McpMarkerResolver(runtime, observability=None).resolve(
            Mcp(SERVER, include=["find_*"]), McpHandle
        )
        asked: list[str] = []
        monkeypatch.setattr(runtime, "use_case_mcp_grant", asked.append)
        refusals: list[AgentRunErrorCode] = []

        async with runtime:
            with ExecutionScope(ANONYMOUS):
                for call in (
                    lambda: crm.call_untyped(TOOL, {}),
                    lambda: crm.call(TOOL, {}, expect=dict),
                ):
                    with pytest.raises(AgentRunError) as refused:
                        await call()
                    refusals.append(refused.value.code)
                with pytest.raises(AgentRunError) as listing:
                    crm.tools()
                refusals.append(listing.value.code)

        assert refusals == [AgentRunErrorCode.UNAUTHORIZED] * 3
        assert asked == []


class TestACallerWithNoSubject:
    async def test_an_anonymous_agent_caller_keeps_unauthorized(
        self, runtime: AgentRuntime, triage: Any, crm: Any, backend_calls: Any
    ) -> None:
        async with runtime:
            with ExecutionScope(ANONYMOUS):
                handle_call = HANDLE_CALLS["agent"](triage, crm)
                with pytest.raises(AgentRunError) as refused:
                    await handle_call()

        assert refused.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(refused.value) == "agent 'acme-triage' requires an authenticated caller"
        assert backend_calls() == 0

    @pytest.mark.parametrize("caller", [ANONYMOUS, NO_SUBJECT], ids=["anonymous", "no-subject"])
    @pytest.mark.parametrize("call", list(HANDLE_CALLS.values()), ids=list(HANDLE_CALLS))
    async def test_is_refused_with_no_backend_call_even_where_a_row_has_no_subject(
        self,
        runtime: AgentRuntime,
        triage: Any,
        crm: Any,
        caller: Identity,
        call: Any,
        sql_service: AcmeSqlService,
        backend_calls: Any,
    ) -> None:
        assert "" in sql_service.rows_by_subject

        async with runtime:
            with ExecutionScope(caller):
                handle_call = call(triage, crm)
                with pytest.raises(AgentRunError) as refused:
                    await handle_call()

        assert refused.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert backend_calls() == 0
