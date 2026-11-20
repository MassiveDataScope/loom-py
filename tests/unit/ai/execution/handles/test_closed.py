"""A handle kept by a task that outlived its execution acts as its caller within the delegation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.execution import AgentHost
from loom.ai.execution._handle import AgentMarkerResolver, McpMarkerResolver
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.errors import Unauthenticated
from loom.core.use_case import Agent, Mcp, UseCase

from .doubles import AGENT, ALICE, HANDLE_CALLS, SERVER, AcmeEngine, Call

_Bind = Callable[[Any, Any], Call]


class AcmeOutlivingUseCase(UseCase[object, "asyncio.Task[object]"]):
    """Binds *bind*'s call in the execution and starts a task making it once *release* is set."""

    def __init__(self, release: asyncio.Event, bind: _Bind) -> None:
        self._release = release
        self._bind = bind

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(AGENT),
        crm: McpHandle = Mcp(SERVER, include=["find_*"]),
    ) -> asyncio.Task[object]:
        call = self._bind(triage, crm)

        async def later() -> object:
            await self._release.wait()
            return await call()

        return asyncio.get_running_loop().create_task(later())


@pytest.fixture
def executor(
    agent_resolver: AgentMarkerResolver, mcp_resolver: McpMarkerResolver
) -> UseCaseExecutor:
    executor = UseCaseExecutor(UseCaseCompiler())
    executor.bind_agent_resolver(agent_resolver)
    executor.bind_mcp_resolver(mcp_resolver)
    return executor


@pytest.fixture
def outliving(executor: UseCaseExecutor) -> Any:
    """Run the use case as Alice and return its task, still waiting, once the execution ended."""

    async def start(call: _Bind) -> tuple[asyncio.Task[object], asyncio.Event]:
        release = asyncio.Event()
        task = await executor.execute(AcmeOutlivingUseCase(release, call), identity=ALICE)
        return task, release

    return start


class TestAHandleInATaskThatOutlivedItsExecution:
    @pytest.mark.parametrize("call", list(HANDLE_CALLS.values()), ids=list(HANDLE_CALLS))
    async def test_is_refused_past_the_delegation_deadline_with_no_backend_call(
        self,
        host: AgentHost,
        outliving: Any,
        call: _Bind,
        past_the_window: Callable[[], None],
        backend_calls: Any,
    ) -> None:
        async with host:
            task, release = await outliving(call)
            past_the_window()
            release.set()

            with pytest.raises(Unauthenticated, match="delegation of this caller has expired"):
                await task

        assert backend_calls() == 0

    @pytest.mark.parametrize("call", list(HANDLE_CALLS.values()), ids=list(HANDLE_CALLS))
    async def test_acts_as_the_caller_within_the_delegation(
        self,
        host: AgentHost,
        outliving: Any,
        call: _Bind,
        window: float,
        backend_calls: Any,
    ) -> None:
        del window
        async with host:
            task, release = await outliving(call)
            release.set()
            await task

        assert backend_calls() == 1

    async def test_the_agent_runs_as_the_caller_of_the_ended_execution(
        self, host: AgentHost, outliving: Any, engine: AcmeEngine, window: float
    ) -> None:
        del window
        async with host:
            task, release = await outliving(HANDLE_CALLS["agent"])
            release.set()
            await task

        assert engine.identities == [ALICE]
        assert engine.acting == [ALICE]
