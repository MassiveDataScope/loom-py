"""A singleton adapter holding handles serves every execution as its own caller."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.execution import AgentHost
from loom.ai.execution._handle import AgentMarkerResolver, McpMarkerResolver
from loom.core.bootstrap import Kernel, create_kernel
from loom.core.di import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.execution import ExecutionRequired
from loom.core.identity import Identity
from loom.core.observability.event import EventKind
from loom.core.observability.event import Scope as SpanScope
from loom.core.observability.observability import Observability
from loom.core.use_case import Agent, Mcp, UseCase
from tests.integration.ai.conftest import RecordingMcpSession, RecordingObserver

from .doubles import AGENT, ALICE, BOB, CONNECTION, SERVER, TOOL, AcmeEngine, AcmeSqlService


class AcmeAdvisor:
    """The adapter, bound as a singleton: built once, at startup, outside any execution."""

    def __init__(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(AGENT),
        crm: McpHandle = Mcp(SERVER, include=["find_*"]),
    ) -> None:
        self.triage, self.crm = triage, crm


class AcmeGates:
    """Holds the first execution until the second has run, to force the interleaving."""

    def __init__(self) -> None:
        self.first_started = asyncio.Event()
        self.second_done = asyncio.Event()


class AcmeAdviseUseCase(UseCase[object, None]):
    def __init__(self, advisor: AcmeAdvisor, gates: AcmeGates) -> None:
        self._advisor = advisor
        self._gates = gates

    async def execute(self, first: bool) -> None:
        if first:
            self._gates.first_started.set()
            await self._gates.second_done.wait()
        await self._advisor.triage.run("advise")
        await self._advisor.crm.call_untyped(TOOL, {})
        if not first:
            self._gates.second_done.set()


class AcmeInterleavedUseCase(UseCase[object, None]):
    """Declares both handles on ``execute()``, so each execution resolves them as it starts."""

    def __init__(self, gates: AcmeGates, *, first: bool) -> None:
        self._gates = gates
        self._first = first

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(AGENT),
        crm: McpHandle = Mcp(SERVER, include=["find_*"]),
    ) -> None:
        if self._first:
            self._gates.first_started.set()
            await self._gates.second_done.wait()
        await triage.run("advise")
        await triage.sql(CONNECTION).query("select id from orders")
        await crm.call_untyped(TOOL, {})
        if not self._first:
            self._gates.second_done.set()


class AcmeConfig:
    pass


@pytest.fixture
def kernel(agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver) -> Kernel:
    gates = AcmeGates()

    def module(container: LoomContainer) -> None:
        container.register(AcmeAdvisor, AcmeAdvisor, scope=Scope.APPLICATION)
        container.register_instance(AcmeGates, gates)

    return create_kernel(
        config=AcmeConfig(),
        use_cases=(AcmeAdviseUseCase,),
        modules=(module,),
        agent_resolver=agent_resolver,
        mcp_resolver=mcp_resolver,
    )


class TestASingletonAdapterHoldingHandles:
    def test_is_built_at_startup_with_the_call_time_resolvers(self, kernel: Kernel) -> None:
        advisor = kernel.container.resolve(AcmeAdvisor)

        assert advisor is kernel.container.resolve(AcmeAdvisor)

    async def test_interleaved_executions_each_act_as_themselves(
        self,
        kernel: Kernel,
        runtime: AgentHost,
        engine: AcmeEngine,
        session: RecordingMcpSession,
    ) -> None:
        async def advise(caller: Identity, first: bool) -> None:
            await kernel.executor.run(AcmeAdviseUseCase, params={"first": first}, identity=caller)

        async with runtime:
            first = asyncio.create_task(advise(ALICE, True))
            await kernel.container.resolve(AcmeGates).first_started.wait()
            await advise(BOB, False)
            await first

        assert engine.identities == [BOB, ALICE]
        assert engine.acting == [BOB, ALICE]
        assert len(session.calls) == 2

    async def test_its_handles_fail_closed_outside_any_execution(
        self, kernel: Kernel, runtime: AgentHost
    ) -> None:
        advisor = kernel.container.resolve(AcmeAdvisor)

        async with runtime:
            with pytest.raises(ExecutionRequired):
                await advisor.triage.run("advise")
            with pytest.raises(ExecutionRequired):
                await advisor.crm.call_untyped(TOOL, {})


class TestHandlesResolvedInEachExecution:
    async def test_a_resolve_by_another_caller_does_not_change_who_an_earlier_one_acts_as(
        self,
        runtime: AgentHost,
        engine: AcmeEngine,
        sql_service: AcmeSqlService,
        session: RecordingMcpSession,
    ) -> None:
        recorder = RecordingObserver()
        executor = UseCaseExecutor(UseCaseCompiler())
        executor.bind_agent_resolver(
            AgentMarkerResolver(runtime, sql_query_service=sql_service, observability=None)
        )
        executor.bind_mcp_resolver(
            McpMarkerResolver(runtime, observability=Observability([recorder]))
        )
        gates = AcmeGates()

        async with runtime:
            first = asyncio.create_task(
                executor.execute(AcmeInterleavedUseCase(gates, first=True), identity=ALICE)
            )
            await gates.first_started.wait()
            await executor.execute(AcmeInterleavedUseCase(gates, first=False), identity=BOB)
            await first

        subjects = [
            event.meta["subject"]
            for event in recorder.events
            if event.scope is SpanScope.TOOL and event.kind is EventKind.START
        ]
        assert engine.identities == [BOB, ALICE]
        assert engine.acting == [BOB, ALICE]
        assert sql_service.calls == [("auditor",), ("analyst",)]
        assert subjects == [BOB.subject, ALICE.subject]
        assert len(session.calls) == 2
