"""Handles called during an agent run act as the run's caller."""

from __future__ import annotations

from typing import Any

from loom.ai.abc import McpHandle
from loom.ai.execution._handle import McpMarkerResolver
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.identity import Identity
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.observability import Observability
from loom.core.use_case import Caller, Mcp, UseCase
from tests.integration.ai.conftest import RecordingMcpSession, RecordingObserver

from .doubles import AGENT, ALICE, SERVER, TOOL, AcmeActingEngine


class AcmeWhoUseCase(UseCase[object, str]):
    async def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class TestDuringAnAgentRun:
    async def test_an_mcp_handle_resolved_outside_any_execution_calls_as_the_run_caller(
        self, make_runtime: Any, session: RecordingMcpSession
    ) -> None:
        recorder = RecordingObserver()
        holder: list[Any] = []

        async def call_tool() -> object:
            return await holder[0].call_untyped(TOOL, {})

        engine = AcmeActingEngine(during=call_tool)
        runtime = make_runtime(engine)
        holder.append(
            McpMarkerResolver(runtime, observability=Observability([recorder])).resolve(
                Mcp(SERVER, include=[TOOL]), McpHandle
            )
        )

        async with runtime:
            await runtime.run(AGENT, "p", identity=ALICE)

        subjects = {
            event.meta["subject"]
            for event in recorder.events
            if event.scope is Scope.TOOL and event.kind is EventKind.START
        }
        assert subjects == {ALICE.subject}
        assert len(session.calls) == 2

    async def test_a_use_case_run_from_the_engine_acts_as_the_run_caller(
        self, make_runtime: Any
    ) -> None:
        executor = UseCaseExecutor(UseCaseCompiler())
        seen: list[str] = []

        async def run_use_case() -> object:
            seen.append(await executor.execute(AcmeWhoUseCase()))
            return None

        engine = AcmeActingEngine(during=run_use_case)

        async with make_runtime(engine) as runtime:
            await runtime.run(AGENT, "p", identity=ALICE)

        assert seen == [ALICE.subject, ALICE.subject]
