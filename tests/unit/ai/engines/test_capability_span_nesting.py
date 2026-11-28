"""The tool span of a capability call nests under the run's agent span.

The two spans are opened by different layers — the agent span by the HTTP or
A2A surface, the tool span by the engine's capability wrapper — and they only
meet through the OTEL context. This drives the engine's real
``capability_call`` inside a real agent span and asserts the exported tree.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from loom.ai.engines.pydantic_ai._guards import BuildContext, capability_call
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.observability.event import Scope
from loom.core.observability.span import LoomSpan
from tests.unit.ai.doubles import tracing_runtime


def _by_name(spans: tuple[ReadableSpan, ...], name: str) -> ReadableSpan:
    matches = [span for span in spans if span.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r} span, got {len(matches)}"
    return matches[0]


def _span_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.span_id


def _parent_span_id(span: ReadableSpan) -> int | None:
    return None if span.parent is None else span.parent.span_id


class TestToolSpanNesting:
    @pytest.fixture
    def traced_run(self) -> Callable[..., tuple[InMemorySpanExporter, BuildContext, LoomSpan]]:
        def open_run(*, timeout_s: float) -> tuple[InMemorySpanExporter, BuildContext, LoomSpan]:
            host, exporter = tracing_runtime()
            context = BuildContext(
                agent="analyst",
                container=LoomContainer(),
                observability=host,
                timeout_s=timeout_s,
                mcp=SharedMcpToolsets(),
                mcp_grants=(),
            )
            agent = host.open_span(Scope.AGENT, "agent_run", agent="analyst")
            return exporter, context, agent

        return open_run

    @pytest.mark.asyncio
    async def test_tool_span_is_a_child_of_the_agent_span(
        self, traced_run: Callable[..., tuple[InMemorySpanExporter, BuildContext, LoomSpan]]
    ) -> None:
        exporter, context, agent = traced_run(timeout_s=5.0)
        with agent.as_current():
            async with capability_call(context, "usecase", "create_order"):
                await asyncio.sleep(0)
        agent.end()

        spans = exporter.get_finished_spans()
        agent_span = _by_name(spans, "agent:agent_run")
        tool_span = _by_name(spans, "tool:create_order")
        assert _parent_span_id(tool_span) == _span_id(agent_span)
        assert _parent_span_id(agent_span) is None
        assert tool_span.status.status_code is StatusCode.OK
        assert tool_span.attributes is not None
        assert tool_span.attributes["capability"] == "usecase"
        assert "subject" not in tool_span.attributes

    @pytest.mark.asyncio
    async def test_a_timed_out_tool_closes_its_span_as_an_error_under_the_agent_span(
        self, traced_run: Callable[..., tuple[InMemorySpanExporter, BuildContext, LoomSpan]]
    ) -> None:
        exporter, context, agent = traced_run(timeout_s=0.001)

        async def overrun() -> None:
            """Wait until the plan timeout cancels the tool body."""
            async with capability_call(context, "usecase", "create_order"):
                await asyncio.Event().wait()

        with agent.as_current():  # noqa: SIM117 - separate blocks so only the call under test can raise
            with pytest.raises(AgentRunError) as raised:
                await asyncio.wait_for(overrun(), timeout=5)
        agent.end()

        assert raised.value.code is AgentRunErrorCode.TOOL_TIMEOUT
        spans = exporter.get_finished_spans()
        tool_span = _by_name(spans, "tool:create_order")
        agent_span = _by_name(spans, "agent:agent_run")
        assert tool_span.status.status_code is StatusCode.ERROR
        assert _parent_span_id(tool_span) == _span_id(agent_span)
        # The agent span outlives the tool failure: the run reports it, the
        # transport decides the outcome.
        assert agent_span.status.status_code is StatusCode.OK
