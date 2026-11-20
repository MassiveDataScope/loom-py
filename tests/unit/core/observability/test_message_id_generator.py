"""The mechanism that makes a Loom trace id the OTEL trace id.

Every assertion here reads exported span structure — trace ids, parents, span
ids — from a real SDK provider. Asserting that a call did not raise, or that an
object has a type, is what let four tracing defects ship behind a green suite.
"""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.otel_ids import (
    LoomMessageIdGenerator,
    build_sampler,
    parse_otel_trace_id,
)
from loom.core.tracing.context import active_trace_id

_TRACE_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
_TRACE_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2"
_TRACE_C = "ccccccccccccccccccccccccccccccc3"


class _CountingDelegate(IdGenerator):
    """Delegate that reports how often it was asked for an id."""

    def __init__(self) -> None:
        self.trace_calls = 0
        self.span_calls = 0

    def generate_span_id(self) -> int:
        self.span_calls += 1
        return 0x1000 + self.span_calls

    def generate_trace_id(self) -> int:
        self.trace_calls += 1
        return 0xF000 + self.trace_calls


def _runtime() -> tuple[Observability, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(id_generator=LoomMessageIdGenerator())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return Observability([], tracer=provider.get_tracer("loom.test")), exporter


def _by_name(exporter: InMemorySpanExporter) -> dict[str, ReadableSpan]:
    return {span.name: span for span in exporter.get_finished_spans()}


def _hex_trace(span: ReadableSpan) -> str:
    context = span.get_span_context()
    assert context is not None
    return format(context.trace_id, "032x")


class TestParseOtelTraceId:
    @pytest.mark.parametrize(
        "value",
        [None, "", "job-42", "not-hex-not-hex-not-hex-not-hexx", "abc", "0" * 32],
    )
    def test_anything_not_a_valid_otel_trace_id_is_rejected(self, value: str | None) -> None:
        assert parse_otel_trace_id(value) is None

    def test_a_loom_trace_id_converts_exactly(self) -> None:
        assert parse_otel_trace_id(_TRACE_A) == int(_TRACE_A, 16)


class TestGeneratorDelegation:
    def test_span_ids_are_always_delegated_even_with_a_trace_active(self) -> None:
        delegate = _CountingDelegate()
        generator = LoomMessageIdGenerator(delegate)

        with active_trace_id(_TRACE_A):
            first = generator.generate_span_id()
            second = generator.generate_span_id()
            assert generator.generate_trace_id() == int(_TRACE_A, 16)

        assert first != second, "two spans of one message must stay distinguishable"
        assert delegate.span_calls == 2
        assert delegate.trace_calls == 0

    def test_no_active_trace_falls_back_to_the_delegate(self) -> None:
        delegate = _CountingDelegate()

        assert LoomMessageIdGenerator(delegate).generate_trace_id() == 0xF001
        assert delegate.trace_calls == 1

    def test_an_unusable_trace_id_falls_back_rather_than_emitting_a_broken_trace(self) -> None:
        delegate = _CountingDelegate()

        with active_trace_id("job-42"):
            assert LoomMessageIdGenerator(delegate).generate_trace_id() == 0xF001


class TestExportedSpanStructure:
    def test_independent_roots_carry_their_own_message_trace_and_have_no_parent(self) -> None:
        runtime, exporter = _runtime()

        for trace_id, name in ((_TRACE_A, "a"), (_TRACE_B, "b"), (_TRACE_C, "c")):
            with runtime.span(Scope.NODE, name, trace_id=trace_id):
                pass

        spans = _by_name(exporter)
        assert {name: _hex_trace(span) for name, span in spans.items()} == {
            "node:a": _TRACE_A,
            "node:b": _TRACE_B,
            "node:c": _TRACE_C,
        }
        assert [span.parent for span in spans.values()] == [None, None, None], (
            "a fabricated parent points at a span that was never exported"
        )

    def test_a_nested_span_inherits_its_parents_trace_not_its_own_argument(self) -> None:
        runtime, exporter = _runtime()

        with (
            runtime.span(Scope.NODE, "outer", trace_id=_TRACE_A),
            runtime.span(Scope.NODE, "inner", trace_id=_TRACE_B),
        ):
            pass

        spans = _by_name(exporter)
        outer, inner = spans["node:outer"], spans["node:inner"]
        assert _hex_trace(inner) == _TRACE_A, (
            "the generator is consulted only for roots; a child must follow its parent"
        )
        assert inner.parent is not None
        outer_context = outer.get_span_context()
        assert outer_context is not None
        assert inner.parent.span_id == outer_context.span_id

    def test_root_true_detaches_a_span_from_the_enclosing_trace(self) -> None:
        runtime, exporter = _runtime()

        with runtime.span(Scope.NODE, "outer", trace_id=_TRACE_A):
            runtime.open_span(Scope.TERMINAL, "sink_write", trace_id=_TRACE_B, root=True).end()

        spans = _by_name(exporter)
        terminal = spans["terminal:sink_write"]
        assert terminal.parent is None
        assert _hex_trace(terminal) == _TRACE_B
        assert _hex_trace(spans["node:outer"]) == _TRACE_A

    def test_the_active_trace_does_not_leak_past_the_span_that_set_it(self) -> None:
        runtime, exporter = _runtime()

        with runtime.span(Scope.NODE, "traced", trace_id=_TRACE_A):
            pass
        with runtime.span(Scope.NODE, "untraced"):
            pass

        spans = _by_name(exporter)
        assert _hex_trace(spans["node:untraced"]) != _TRACE_A


class TestSamplerBuilder:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("always_on", "AlwaysOnSampler"),
            ("always_off", "AlwaysOffSampler"),
            ("traceidratio", "TraceIdRatioBased{0.25}"),
            ("parentbased_always_on", "ParentBased{root:AlwaysOnSampler"),
            ("parentbased_traceidratio", "ParentBased{root:TraceIdRatioBased{0.25}"),
        ],
    )
    def test_each_configured_name_builds_the_sampler_it_promises(
        self, name: str, expected: str
    ) -> None:
        sampler = build_sampler(OtelConfig(sampler=name, sampler_ratio=0.25))

        assert sampler.get_description().startswith(expected)
