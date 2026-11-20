"""Handles are identity-free proxies memoised per declaration; every call reads the identity."""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.execution import AgentHost
from loom.ai.execution._handle import AgentMarkerResolver, McpMarkerResolver
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.errors import IdentityFixed
from loom.core.execution import ExecutionRequired, ExecutionScope, NoExecution
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.observability import Observability
from loom.core.use_case import Agent, Mcp
from tests.integration.ai.conftest import RecordingMcpSession, RecordingObserver

from .doubles import AGENT, ALICE, BOB, HANDLE_CALLS, SERVER, TOOL, AcmeEngine, AcmeSqlService


class TestResolution:
    def test_a_declaration_resolves_to_one_proxy_however_often_it_is_resolved(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        triage, crm = Agent(AGENT), Mcp(SERVER, include=["find_*"])

        with ExecutionScope(ALICE):
            first = (
                agent_resolver.resolve(triage, AgentHandle[Any]),
                mcp_resolver.resolve(crm, McpHandle),
            )
        with ExecutionScope(BOB):
            second = (
                agent_resolver.resolve(triage, AgentHandle[Any]),
                mcp_resolver.resolve(crm, McpHandle),
            )

        assert first[0] is second[0]
        assert first[1] is second[1]

    def test_equal_declarations_share_one_proxy_and_different_ones_do_not(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        agent = agent_resolver.resolve(Agent(AGENT), AgentHandle[Any])
        mcp = mcp_resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle)

        assert agent_resolver.resolve(Agent(AGENT), AgentHandle[Any]) is agent
        assert agent_resolver.resolve(Agent("acme-other"), AgentHandle[Any]) is not agent
        assert mcp_resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle) is mcp
        assert mcp_resolver.resolve(Mcp(SERVER, include=["list_*"]), McpHandle) is not mcp

    def test_resolving_reads_no_identity_so_a_singleton_can_hold_a_handle(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        with NoExecution():
            triage = agent_resolver.resolve(Agent(AGENT), AgentHandle[Any])
            crm = mcp_resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle)

        assert triage is not None
        assert crm is not None


class TestTheLegacyCall:
    def test_serves_the_declarations_handle_for_the_execution_identity(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        with ExecutionScope(ALICE):
            triage = agent_resolver(AGENT, ALICE)
            crm = mcp_resolver(SERVER, ("find_*",), ALICE)

        assert triage is agent_resolver.resolve(Agent(AGENT), AgentHandle[Any])
        assert crm is mcp_resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle)

    def test_refuses_another_identity_than_the_execution_acts_as(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        with ExecutionScope(ALICE):
            with pytest.raises(IdentityFixed, match="agent 'acme-triage' acts as"):
                agent_resolver(AGENT, BOB)
            with pytest.raises(IdentityFixed, match="mcp server 'acme-crm' acts as"):
                mcp_resolver(SERVER, ("find_*",), BOB)


class TestBindingAResolver:
    def test_refuses_positional_resolvers(self) -> None:
        executor = UseCaseExecutor(UseCaseCompiler())

        with pytest.raises(TypeError, match=r"resolve\(declaration, annotation\)"):
            executor.bind_agent_resolver(lambda name, identity: name)
        with pytest.raises(TypeError, match=r"resolve\(declaration, annotation\)"):
            executor.bind_mcp_resolver(lambda server, include, identity: server)

    def test_binds_the_call_time_resolvers_without_a_warning(
        self, agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
    ) -> None:
        executor = UseCaseExecutor(UseCaseCompiler())

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            executor.bind_agent_resolver(agent_resolver)
            executor.bind_mcp_resolver(mcp_resolver)


class TestEachCallReadsTheIdentity:
    async def test_one_agent_handle_runs_as_whoever_calls_it(
        self, host: AgentHost, triage: Any, crm: Any, engine: AcmeEngine
    ) -> None:
        async with host:
            for caller in (ALICE, BOB, ALICE):
                with ExecutionScope(caller):
                    await HANDLE_CALLS["agent"](triage, crm)()

        assert engine.identities == [ALICE, BOB, ALICE]
        assert engine.acting == [ALICE, BOB, ALICE]

    async def test_one_sql_view_queries_with_the_roles_of_whoever_calls_it(
        self, host: AgentHost, triage: Any, crm: Any, sql_service: AcmeSqlService
    ) -> None:
        async with host:
            view = triage.sql("acme-reporting")
            for caller in (ALICE, BOB):
                with ExecutionScope(caller):
                    await view.query("select id from orders")

        assert sql_service.calls == [("analyst",), ("auditor",)]

    @pytest.mark.parametrize("call", list(HANDLE_CALLS.values()), ids=list(HANDLE_CALLS))
    async def test_a_handle_called_outside_any_execution_fails_closed(
        self, host: AgentHost, triage: Any, crm: Any, call: Any, backend_calls: Any
    ) -> None:
        async with host:
            handle_call = call(triage, crm)
            with pytest.raises(ExecutionRequired):
                await handle_call()

        assert backend_calls() == 0

    async def test_one_mcp_handle_calls_each_tool_as_whoever_calls_it(
        self, host: AgentHost, session: RecordingMcpSession
    ) -> None:
        recorder = RecordingObserver()
        resolver = McpMarkerResolver(host, observability=Observability([recorder]))
        crm = resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle)

        async with host:
            for caller in (ALICE, BOB):
                with ExecutionScope(caller):
                    assert await crm.call_untyped(TOOL, {}) == {"orders": 1}

        subjects = [
            event.meta["subject"]
            for event in recorder.events
            if event.scope is Scope.TOOL and event.kind is EventKind.START
        ]
        assert subjects == [ALICE.subject, BOB.subject]
        assert len(session.calls) == 2
