"""End-to-end coverage of the ``Agent()`` marker (spec 014, PR2).

Drives the whole path a consumer would: a real ``UseCaseCompiler`` compiles a
use case declaring ``Agent()``, a real ``RuntimeExecutor`` runs it, and a real
``AgentRuntime`` serves the named agent — only the model (a scripted engine)
and the network are faked, at the edge. Nothing here stubs the seam the tests
are meant to prove: the marker resolution, the identity binding, the cycle
and depth bounds, and the concurrency admission all run through the genuine
production collaborators.

Scope: PR2 only. The three run modes, the granted tool and query views and
the test-runner double are PR3/PR4 work and are not exercised here — see the
PR2 report for exactly what remains.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from loom.ai.abc import AgentEvent
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import agent_marker_resolver
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.sql.service import NullSqlQueryService
from tests.integration.ai.conftest import (
    INNER_MARKER_AGENT_NAME,
    MARKER_AGENT_NAME,
    OUTER_MARKER_AGENT_NAME,
    CountingEngineProvider,
    InnerMarkerUseCase,
    MarkerAgentUseCase,
    OuterMarkerUseCase,
    RecordingScriptedEngine,
    ScriptedEngine,
    ShadowedIdentityParamUseCase,
    StubDepsFactory,
    make_ai_config,
    make_plan,
    marker_use_case_executor,
)

_ALICE = Identity(subject="alice", roles=("analyst",), mechanism="test")


def _wired(
    *,
    plans: dict[str, object],
    compiler_and_executor: tuple[UseCaseCompiler, RuntimeExecutor],
    deps: StubDepsFactory,
    container: LoomContainer,
    max_concurrent_runs: int = 8,
    max_agent_depth: int = 1,
) -> tuple[RuntimeExecutor, AgentRuntime]:
    """Wire a real executor to a real ``AgentRuntime`` through the marker resolver.

    Mirrors the composition a deployment's ``create_app`` performs between
    ``resolve_ai`` and ``result.factory.verify()``: the executor is built
    first, and the agent resolver is bound to it once the AI runtime exists.
    """
    _, executor = compiler_and_executor
    provider = CountingEngineProvider(engines=dict(plans))  # type: ignore[arg-type]
    runtime = AgentRuntime(
        plans=[make_plan(name) for name in plans],
        config=make_ai_config(
            max_concurrent_runs=max_concurrent_runs, max_agent_depth=max_agent_depth
        ),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )
    executor.bind_agent_resolver(
        agent_marker_resolver(runtime, sql_query_service=NullSqlQueryService(), observability=None)
    )
    return executor, runtime


class TestTheHandleRunsAsTheVerifiedCaller:
    """Property 1: the capability runs as the verified caller, not as the worker."""

    async def test_the_identity_reaching_the_engine_is_the_executors(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = RecordingScriptedEngine()
        executor, runtime = _wired(
            plans={MARKER_AGENT_NAME: engine},
            compiler_and_executor=marker_use_case_executor(MarkerAgentUseCase),
            deps=deps,
            container=container,
        )

        async with runtime:
            result = await executor.execute(MarkerAgentUseCase(), identity=_ALICE)

        assert result["caller_subject"] == "alice"
        assert engine.identities == [_ALICE]

    async def test_two_different_callers_never_cross(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Concurrent runs each bind their own caller."""
        bob = Identity(subject="bob", mechanism="test")
        engine = RecordingScriptedEngine()
        executor, runtime = _wired(
            plans={MARKER_AGENT_NAME: engine},
            compiler_and_executor=marker_use_case_executor(MarkerAgentUseCase),
            deps=deps,
            container=container,
            max_concurrent_runs=2,
        )

        async with runtime:
            results = await asyncio.gather(
                executor.execute(MarkerAgentUseCase(), identity=_ALICE),
                executor.execute(MarkerAgentUseCase(), identity=bob),
            )

        assert {r["caller_subject"] for r in results} == {"alice", "bob"}
        assert sorted(i.subject for i in engine.identities) == ["alice", "bob"]


class TestAParameterDoesNotOverrideTheVerifiedIdentity:
    """Property 2: the SQL PR's defect, pinned here from the start."""

    async def test_a_primitive_parameter_named_identity_never_reaches_the_model(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = RecordingScriptedEngine()
        executor, runtime = _wired(
            plans={MARKER_AGENT_NAME: engine},
            compiler_and_executor=marker_use_case_executor(ShadowedIdentityParamUseCase),
            deps=deps,
            container=container,
        )

        async with runtime:
            result = await executor.execute(
                ShadowedIdentityParamUseCase(),
                params={"identity": "attacker"},
                identity=_ALICE,
            )

        # The primitive parameter is bound exactly as any other primitive —
        # nothing hides it from the use case body.
        assert result["param_identity"] == "attacker"
        # But the caller marker, and the agent handle behind it, only ever
        # see the identity the executor itself was given.
        assert result["caller_subject"] == "alice"
        assert engine.identities == [_ALICE]
        assert all(i.subject != "attacker" for i in engine.identities)


class _CallbackScriptedEngine(RecordingScriptedEngine):
    """A one-turn engine that awaits a callback before it starts replaying.

    ``RecordingScriptedEngine`` already records every identity it was given;
    overriding only ``_iterate`` — the coroutine both ``run`` and
    ``run_stream`` already delegate to — is the smallest change that lets a
    test drive a nested marker call from inside an outer one, the exact
    position an output hook would run from.
    """

    def __init__(self, callback: Callable[[], Awaitable[None]]) -> None:
        super().__init__()
        self._callback = callback

    async def _iterate(self) -> AsyncGenerator[AgentEvent, None]:
        await self._callback()
        async for event in super()._iterate():
            yield event


class TestCycleAndDepthThroughTheMarker:
    """Property 6 (first half): cycle and depth refusals."""

    async def test_a_nested_run_of_another_agent_exceeds_the_default_depth(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The inner use case declares its own Agent(); it does not fit with max_agent_depth=1."""
        inner_engine = RecordingScriptedEngine()
        compiler_and_executor = marker_use_case_executor(OuterMarkerUseCase, InnerMarkerUseCase)
        _, executor = compiler_and_executor

        async def _run_inner() -> None:
            inner_use_case = InnerMarkerUseCase()
            with pytest.raises(AgentRunError) as excinfo:
                await executor.execute(inner_use_case, identity=_ALICE)
            assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_TOO_DEEP

        outer_engine = _CallbackScriptedEngine(_run_inner)
        _, runtime = _wired(
            plans={OUTER_MARKER_AGENT_NAME: outer_engine, INNER_MARKER_AGENT_NAME: inner_engine},
            compiler_and_executor=compiler_and_executor,
            deps=deps,
            container=container,
            max_agent_depth=1,
        )

        async with runtime:
            await executor.execute(OuterMarkerUseCase(), identity=_ALICE)

        assert inner_engine.identities == []

    async def test_a_raised_depth_lets_the_nested_run_through(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        inner_engine = RecordingScriptedEngine()
        compiler_and_executor = marker_use_case_executor(OuterMarkerUseCase, InnerMarkerUseCase)
        _, executor = compiler_and_executor

        async def _run_inner() -> None:
            result = await executor.execute(InnerMarkerUseCase(), identity=_ALICE)
            assert result["output"] == {"answer": "42"}  # ScriptedEngine's DEFAULT_OUTPUT

        outer_engine = _CallbackScriptedEngine(_run_inner)
        _, runtime = _wired(
            plans={OUTER_MARKER_AGENT_NAME: outer_engine, INNER_MARKER_AGENT_NAME: inner_engine},
            compiler_and_executor=compiler_and_executor,
            deps=deps,
            container=container,
            max_agent_depth=2,
        )

        async with runtime:
            await executor.execute(OuterMarkerUseCase(), identity=_ALICE)

        assert inner_engine.identities == [_ALICE]


class TestTheConcurrencyPermitIsRefusedInsteadOfQueued:
    """Property 6 (second half): with no queue, an exhausted quota is refused instantly."""

    async def test_a_second_run_is_refused_while_the_first_is_still_open(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        # First event fires immediately (started), second after 40ms, holding
        # the concurrency permit open long enough for a rival call to arrive.
        held_engine = ScriptedEngine(delays_ms=(0, 40))
        executor, runtime = _wired(
            plans={MARKER_AGENT_NAME: held_engine},
            compiler_and_executor=marker_use_case_executor(MarkerAgentUseCase),
            deps=deps,
            container=container,
            max_concurrent_runs=1,
        )

        async with runtime:
            first = asyncio.ensure_future(executor.execute(MarkerAgentUseCase(), identity=_ALICE))
            await held_engine.started.wait()

            second_use_case = MarkerAgentUseCase()
            with pytest.raises(AgentRunError) as excinfo:
                await executor.execute(second_use_case, identity=_ALICE)
            assert excinfo.value.code is AgentRunErrorCode.TOO_MANY_RUNS

            await first
