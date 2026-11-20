"""``ai.max_agent_depth``: cycle and depth bounds over the in-flight run chain (T203/T204).

Every run started through ``AgentHost.run`` pushes its own name onto a
task-local chain and pops it in a ``finally`` — this is what a marker-driven
nested run reuses unmodified, so the coverage lives here, one level below the
``AgentHandle`` adapter, where the mechanism this test protects actually
lives.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest

from loom.ai.abc import AgentEvent, AgentResult, Conversation, FinalEvent, HealthStatus
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentHost
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)

_OUTER = "outer"
_INNER = "inner"


class _CallbackEngine:
    """Runs one scripted turn, invoking *callback* (if any) before finishing.

    The callback is where a test drives a nested ``host.run`` call from
    inside an in-flight one — the same position a marker-resolved
    ``AgentHandle.run`` call would run from inside a use case a hook or a
    tool invokes.
    """

    def __init__(self, callback: Callable[[], Awaitable[None]] | None = None) -> None:
        self._callback = callback

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        del prompt, identity, conversation, state

        async def _events() -> AsyncIterator[AgentEvent]:
            if self._callback is not None:
                await self._callback()
            yield FinalEvent(output={}, usage=DEFAULT_USAGE)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            yield _events()

        return _stream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        last: AgentEvent | None = None
        async with self.run_stream(
            prompt, identity=identity, conversation=conversation, state=state
        ) as events:
            async for event in events:
                last = event
        assert isinstance(last, FinalEvent)
        return AgentResult(output=last.output, usage=last.usage)

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


def _host(
    *,
    max_agent_depth: int,
    outer_callback: Callable[[], Awaitable[None]] | None,
    deps: StubDepsFactory,
    container: LoomContainer,
) -> AgentHost:
    outer_engine = _CallbackEngine(outer_callback)
    inner_engine = _CallbackEngine()
    engines = {_OUTER: outer_engine, _INNER: inner_engine}
    provider = CountingEngineProvider(engines=engines)  # type: ignore[arg-type]
    return AgentHost(
        plans=[make_plan(_OUTER), make_plan(_INNER)],
        config=make_ai_config(max_agent_depth=max_agent_depth),
        engine_plugin=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestDefaultDepth:
    """With ``max_agent_depth`` at its default value of one."""

    async def test_a_nested_run_of_another_agent_exceeds_the_depth(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The root run already exhausts the default budget of one."""

        async def _nest() -> None:
            with pytest.raises(AgentRunError) as excinfo:
                await host.run(_INNER, "hola", identity=identity)
            assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_TOO_DEEP
            # Pins the exact message docs/rest/use-case-dsl.md quotes for
            # AGENT_CALL_TOO_DEEP; edit one without the other and this is the
            # gap the next review catches.
            assert str(excinfo.value) == (
                "agent call chain outer -> inner exceeds ai.max_agent_depth=1"
            )

        host = _host(max_agent_depth=1, outer_callback=_nest, deps=deps, container=container)
        async with host:
            await host.run(_OUTER, "hola", identity=identity)

    async def test_a_nested_run_of_the_same_agent_is_a_cycle(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The same name in the chain is reported as a cycle, not as depth."""

        async def _nest() -> None:
            with pytest.raises(AgentRunError) as excinfo:
                await host.run(_OUTER, "hola", identity=identity)
            assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_CYCLE
            assert _OUTER in str(excinfo.value)
            # Pins the exact message docs/rest/use-case-dsl.md quotes for
            # AGENT_CALL_CYCLE.
            assert str(excinfo.value) == "agent call cycle detected: outer -> outer"

        host = _host(max_agent_depth=5, outer_callback=_nest, deps=deps, container=container)
        async with host:
            await host.run(_OUTER, "hola", identity=identity)

    async def test_two_consecutive_runs_of_the_same_agent_do_not_contaminate_each_other(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The chain is restored in the 'finally': two non-nested runs do not collide.

        If the restoration were lost, the second run would still see the
        first run's name in the chain and fail as a spurious cycle.
        """
        host = _host(max_agent_depth=1, outer_callback=None, deps=deps, container=container)
        async with host:
            await host.run(_OUTER, "hola", identity=identity)
            await host.run(_OUTER, "hola de nuevo", identity=identity)

    async def test_a_failed_run_also_restores_the_chain(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A failure inside the run does not leave the name stuck in the chain."""

        calls = 0

        async def _fail_once() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")

        host = _host(max_agent_depth=1, outer_callback=_fail_once, deps=deps, container=container)
        async with host:
            with pytest.raises(RuntimeError, match="boom"):
                await host.run(_OUTER, "hola", identity=identity)
            # Same agent, same task: succeeds only if the failed run's name
            # was popped from the chain despite the exception.
            await host.run(_OUTER, "otra vez", identity=identity)


class TestRaisedDepth:
    """With ``max_agent_depth`` explicitly raised by the operator."""

    async def test_a_nested_run_of_another_agent_fits_the_budget(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        seen: list[str] = []

        async def _nest() -> None:
            result = await host.run(_INNER, "hola", identity=identity)
            seen.append("nested-ok")
            assert result.output == {}

        host = _host(max_agent_depth=2, outer_callback=_nest, deps=deps, container=container)
        async with host:
            await host.run(_OUTER, "hola", identity=identity)

        assert seen == ["nested-ok"]
