"""Build-time refusal when a capability keeps streaming despite ``streaming: false``.

``AgentRun.next`` streams a node whenever the run's root capability overrides
``wrap_run_event_stream``, whatever the binding asked for -- so a binding
declaring ``streaming: false`` alongside such a capability would silently keep
streaming. The engine refuses to build that combination instead.

The capability that trips the guard below is built by this test: neither
``pydantic_ai_harness.Skills`` nor ``pydantic_ai.capabilities.NativeTool`` --
the only capabilities loom builds today -- overrides ``wrap_run_event_stream``,
so with loom's current grants the guard cannot fire. It stays anyway, because
pydantic-ai has a second, unrelated way to force streaming -- registering
``Hooks.on.event``/``Hooks.on.run_event_stream`` -- that ``has_wrap_run_event_stream``
does not see, and loom builds no ``Hooks`` capability today either.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any

import pytest
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.tools import RunContext

from loom.ai.engines.pydantic_ai import provider as _provider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from tests.helpers.pydantic_ai_engine import build_engine, make_plan, prose_model


class _AlwaysStreams(AbstractCapability[Any]):
    """A capability overriding ``wrap_run_event_stream``, the way a real hook would."""

    async def wrap_run_event_stream(
        self, ctx: RunContext[Any], *, stream: AsyncIterable[Any]
    ) -> AsyncIterable[Any]:
        async for event in stream:
            yield event


class TestStreamingCapabilityConflict:
    @pytest.fixture
    def streaming_capability(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_provider, "build_capabilities", lambda plan: (_AlwaysStreams(),))

    def test_a_whole_binding_with_a_streaming_capability_fails_at_build(
        self, streaming_capability: None
    ) -> None:
        plan = make_plan(
            inference=InferenceTarget(provider="openai", model="a-model", streaming=False)
        )

        with pytest.raises(AgentCompilationError) as failure:
            build_engine(plan, prose_model())

        assert failure.value.issues[0].code is AgentErrorCode.STREAMING_REQUIRED_BY_CAPABILITY
        assert plan.name in failure.value.issues[0].message

    def test_the_same_capability_builds_fine_when_the_binding_still_streams(
        self, streaming_capability: None
    ) -> None:
        plan = make_plan()

        build_engine(plan, prose_model())
