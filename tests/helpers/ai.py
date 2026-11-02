"""Shared offline AI runtime doubles and configuration builders for tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    Conversation,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
)
from loom.ai.config import A2AAgentConfig, A2AConfig, AgentEndpointConfig, AiConfig, McpServerConfig
from loom.ai.engines.pydantic_ai import create_a2a_client, create_mcp_client
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity

DEFAULT_USAGE = AgentUsage(
    input_tokens=11,
    output_tokens=7,
    requests=1,
    duration_ms=3,
    cache_read_tokens=4,
    cache_write_tokens=2,
    tool_calls=1,
    cost=Decimal("0.0021"),
    details={"reasoning_tokens": 5},
)
"""Fixed usage every scripted terminal event carries.

Every counter is distinct and non-zero, including one that only exists in
``details``, so a surface that drops or confuses one of them fails a test
rather than passing on a shared default of ``0``.
"""

DEFAULT_OUTPUT: Mapping[str, Any] = {"answer": "42"}
"""Fixed decoded output the default script returns."""


def default_script(output: object = DEFAULT_OUTPUT) -> tuple[AgentEvent, ...]:
    """Return the canonical success script: one delta plus one ``final``."""
    return (TextDeltaEvent(text="ok"), FinalEvent(output=output, usage=DEFAULT_USAGE))


class ScriptedEngine:
    """Offline :class:`~loom.ai.abc.AgentEngine` double with explicit delays.

    ``FakeAgentEngine`` replays a script instantly, which cannot express the
    timing the limit and cancellation tests need.  This double adds a declared
    per-event delay and records whether the stream observed cancellation.

    Args:
        script: Events to replay, terminal event last.
        delays_ms: Delay before each event, positionally aligned with
            ``script``; missing positions default to ``0``.
        health_status: Status reported by :meth:`health`.
        health_gate: When set, :meth:`health` waits on it before answering, so
            a test can hold the background probe open indefinitely.
    """

    def __init__(
        self,
        *,
        script: Sequence[AgentEvent] | None = None,
        delays_ms: Sequence[int] = (),
        health_status: str = "ok",
        health_gate: asyncio.Event | None = None,
    ) -> None:
        self.script: tuple[AgentEvent, ...] = tuple(
            script if script is not None else default_script()
        )
        self.delays_ms = tuple(delays_ms)
        self.health_status = health_status
        self.health_gate = health_gate
        self.emitted: list[AgentEvent] = []
        self.cancelled = False
        self.started = asyncio.Event()
        self.stream_count = 0
        self.conversations: list[Conversation | None] = []

    def _delay_for(self, position: int) -> float:
        if position < len(self.delays_ms):
            return self.delays_ms[position] / 1000
        return 0.0

    async def _iterate(self) -> AsyncGenerator[AgentEvent, None]:
        try:
            for position, event in enumerate(self.script):
                delay = self._delay_for(position)
                if delay:
                    await asyncio.sleep(delay)
                self.started.set()
                self.emitted.append(event)
                yield event
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> Any:  # AbstractAsyncContextManager[AsyncIterator[AgentEvent]]
        """Replay the script as an event stream, recording the conversation."""
        del prompt, identity, state
        self.stream_count += 1
        self.conversations.append(conversation)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            iterator = self._iterate()
            try:
                yield iterator
            finally:
                await iterator.aclose()

        return _stream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Replay the script to completion and return its terminal outcome."""
        async with self.run_stream(
            prompt, identity=identity, conversation=conversation, state=state
        ) as stream:
            last: AgentEvent | None = None
            async for event in stream:
                last = event
        if isinstance(last, FinalEvent):
            return AgentResult(output=last.output, usage=last.usage)
        raise AssertionError("scripted run did not end in a FinalEvent")

    async def health(self) -> HealthStatus:
        """Report the scripted health, optionally blocking on the gate."""
        if self.health_gate is not None:
            await self.health_gate.wait()
        return HealthStatus(status=self.health_status)  # type: ignore[arg-type]


class CountingEngineProvider:
    """Engine provider counting how often the runtime builds an engine.

    Args:
        engines: Engine per plan name.  A plan absent from the mapping gets a
            fresh default-script :class:`ScriptedEngine`.
    """

    LOOM_AI_ENGINE_API = 2

    def __init__(self, engines: Mapping[str, ScriptedEngine] | None = None) -> None:
        self.engines: dict[str, ScriptedEngine] = dict(engines or {})
        self.calls: list[str] = []

    def create_engine(self, plan: object, *, deps: object, container: object) -> ScriptedEngine:
        """Build (or return the pre-scripted) engine for one plan."""
        del deps, container
        name = str(getattr(plan, "name", plan))
        self.calls.append(name)
        return self.engines.setdefault(name, ScriptedEngine())

    def supported_capability_kinds(self) -> frozenset[str]:
        """Accept every compiled capability kind."""
        return frozenset({"usecase", "sql", "mcp", "skills", "python", "a2a"})

    # An engine that announces 'mcp' and 'a2a' must supply what serving them
    # needs: the composition root reads these off the provider rather than
    # importing an engine package, so announcing a kind without its factory is
    # now an inconsistency the runtime reports instead of one it hides.
    #
    # Kept as a staticmethod on purpose: 'engine_client_factories' reads the
    # attribute with getattr, so a third-party engine may still declare one,
    # and this double is where that shape is recorded. It is never invoked --
    # this provider builds ScriptedEngines, which serve no real MCP grant, and
    # every runtime here is handed an explicit factory. A pydantic-ai
    # deployment must instead read 'mcp_client_factory' off the very provider
    # instance that builds its engines, which 'SharedMcpToolsets.for_build'
    # now refuses to let a deployment get wrong.
    mcp_client_factory = staticmethod(create_mcp_client)
    a2a_client_factory = staticmethod(create_a2a_client)


class StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the dependency bundle for one invocation."""
        del container
        return {"identity": identity}


def make_ai_config(
    *,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    a2a: A2AConfig | None = None,
    mcp_servers: Mapping[str, McpServerConfig] | None = None,
    a2a_agents: Mapping[str, A2AAgentConfig] | None = None,
    startup_timeout_ms: int = 500,
    max_concurrent_runs: int = 8,
    max_prompt_bytes: int = 65536,
    max_state_bytes: int = 65536,
    health_cache_ttl_ms: int = 20,
    remote_clients: str = "required",
    max_agent_depth: int = 1,
) -> AiConfig:
    """Build an ``AiConfig`` with test-sized budgets and no model secrets."""
    return AiConfig(
        engine="fake",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        endpoints=dict(endpoints or {}),
        a2a=a2a,
        mcp_servers=dict(mcp_servers or {}),
        a2a_agents=dict(a2a_agents or {}),
        startup_timeout_ms=startup_timeout_ms,
        max_concurrent_runs=max_concurrent_runs,
        max_prompt_bytes=max_prompt_bytes,
        max_state_bytes=max_state_bytes,
        health_cache_ttl_ms=health_cache_ttl_ms,
        remote_clients=remote_clients,
        max_agent_depth=max_agent_depth,
    )
