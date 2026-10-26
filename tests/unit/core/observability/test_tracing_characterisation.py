"""Tracing corpus — the inversion of the pinned OTEL defects.

Every assertion here was written the other way round one PR ago, when tracing
was reconstructed after the fact from two independent ``on_event`` calls: a
child never found its parent (E2), every root span pointed at a fabricated
parent that was never exported (E5), and two concurrent runs of the same
``(scope, name)`` collided on one process-wide registry key (E7). Spans are now
opened by ``ObservabilityRuntime`` itself, in the one lexical scope that covers
both ends of a unit of work, so each of those assertions is inverted here.

Spans are exported through a real ``TracerProvider`` with a
``SimpleSpanProcessor`` and an ``InMemorySpanExporter``, and every assertion is
about exported span structure — trace ids, parent span ids, span counts. A
``ProxyTracerProvider`` (what ``endpoint=""`` yields with no host SDK) produces
non-recording spans that can never fail such assertions, which is exactly how
these defects shipped: never assert on types, truthiness, or the absence of an
exception here.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import AsyncGenerator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from loom.ai._transport import always_closed
from loom.core.config.observability import OtelConfig
from loom.core.model.redaction import Dsn
from loom.core.observability.config import ObservabilityConfig, OtelObservabilityConfig
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observer import otel as _otel_module
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan

_TRACE_ID_HEX = "4b3f9a1c2d8e0f7b6a5c3e1d9f2b4a0c"
_OTHER_TRACE_ID_HEX = "0f1e2d3c4b5a69788796a5b4c3d2e1f0"

_OTEL_CONTEXT_LOGGER = "opentelemetry.context"
_DETACH_FAILURE = "Failed to detach context"


class RecordingObserver:
    """Observer that keeps every event, with the span current when it arrived."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []
        self.span_ids: list[int] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)
        self.span_ids.append(trace.get_current_span().get_span_context().span_id)


def _exporting_provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _exporting_runtime(
    observer: RecordingObserver | None = None,
) -> tuple[ObservabilityRuntime, InMemorySpanExporter]:
    """Build a runtime whose spans land in an in-memory exporter."""
    provider, exporter = _exporting_provider()
    observers = [observer] if observer is not None else []
    runtime = ObservabilityRuntime(observers, tracer=provider.get_tracer("loom.tracing-corpus"))
    return runtime, exporter


@pytest.fixture
def exporting_runtime() -> tuple[ObservabilityRuntime, InMemorySpanExporter]:
    return _exporting_runtime()


def _span_by_name(spans: tuple[ReadableSpan, ...], name: str) -> ReadableSpan:
    matches = [span for span in spans if span.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r} span, got {len(matches)}"
    return matches[0]


def _span_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.span_id


def _trace_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.trace_id


def _parent_span_id(span: ReadableSpan) -> int | None:
    return None if span.parent is None else span.parent.span_id


def _attribute(span: ReadableSpan, key: str) -> object:
    assert span.attributes is not None
    return span.attributes.get(key)


class TestSpanTreeIsExported:
    """E2 inverted — a child scope is parented to the scope that opened it.

    The parent is no longer looked up in a registry under a key nothing ever
    wrote: it is the span that is current when the child opens, which is the
    only definition that cannot drift.
    """

    def test_child_scope_is_parented_to_its_open_parent_scope(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        with (
            runtime.span(Scope.POLL_CYCLE, "orders", trace_id=_TRACE_ID_HEX),
            runtime.span(Scope.NODE, "transform", trace_id=_TRACE_ID_HEX),
        ):
            pass

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        poll_cycle = _span_by_name(spans, "poll_cycle:orders")
        node = _span_by_name(spans, "node:transform")

        assert _parent_span_id(node) == _span_id(poll_cycle)
        assert _parent_span_id(poll_cycle) is None
        assert _trace_id(node) == _trace_id(poll_cycle)

    def test_without_a_trace_id_parent_and_child_are_still_one_trace(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        """This is what the Bytewax runner emits: a POLL_CYCLE with no trace id."""
        runtime, exporter = exporting_runtime

        with runtime.span(Scope.POLL_CYCLE, "orders"), runtime.span(Scope.NODE, "transform"):
            pass

        spans = exporter.get_finished_spans()
        poll_cycle = _span_by_name(spans, "poll_cycle:orders")
        node = _span_by_name(spans, "node:transform")

        assert _parent_span_id(node) == _span_id(poll_cycle)
        assert _trace_id(node) == _trace_id(poll_cycle)

    def test_three_levels_export_one_tree(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        with (
            runtime.span(Scope.PIPELINE, "DailySales"),
            runtime.span(Scope.PROCESS, "Staging"),
            runtime.span(Scope.STEP, "LoadOrders"),
        ):
            pass

        spans = exporter.get_finished_spans()
        pipeline = _span_by_name(spans, "pipeline:DailySales")
        process = _span_by_name(spans, "process:Staging")
        step = _span_by_name(spans, "step:LoadOrders")

        assert _parent_span_id(step) == _span_id(process)
        assert _parent_span_id(process) == _span_id(pipeline)
        assert _parent_span_id(pipeline) is None

    def test_a_failing_body_closes_the_span_as_an_error_without_double_recording(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        with pytest.raises(ValueError), runtime.span(Scope.STEP, "LoadOrders"):
            raise ValueError("boom")

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        step = spans[0]
        assert step.status.status_code is StatusCode.ERROR
        assert step.status.description == "boom"
        # ``record_exception=False`` on the span: exactly one exception event,
        # recorded by the runtime, not a second one added by the SDK's own
        # exit handler.
        assert [event.name for event in step.events] == ["exception"]

    def test_start_is_emitted_with_its_own_span_current(self) -> None:
        """Log correlation depends on this: the span opens before START is emitted.

        Emitting START before opening the span attaches the *parent* span id to
        the START log line, or none at all for a root.
        """
        observer = RecordingObserver()
        runtime, exporter = _exporting_runtime(observer)

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            pass

        use_case = exporter.get_finished_spans()[0]
        assert [event.kind for event in observer.events] == [EventKind.START, EventKind.END]
        assert observer.span_ids == [_span_id(use_case), _span_id(use_case)]


class TestNoDanglingParent:
    """E5 inverted — no exported span points at a span nobody exported.

    The business trace id travels as a span attribute. It no longer fabricates
    a remote ``SpanContext`` with ``span_id=1`` for every root span to hang off.
    """

    def test_every_exported_parent_is_itself_exported(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        with (
            runtime.span(Scope.PIPELINE, "DailySales", trace_id=_TRACE_ID_HEX),
            runtime.span(Scope.STEP, "LoadOrders", trace_id=_TRACE_ID_HEX),
        ):
            pass
        with runtime.span(Scope.NODE, "enrich", trace_id=_OTHER_TRACE_ID_HEX):
            pass

        spans = exporter.get_finished_spans()
        assert len(spans) == 3
        exported_span_ids = {_span_id(span) for span in spans}

        for span in spans:
            parent = _parent_span_id(span)
            assert parent is None or parent in exported_span_ids

    def test_unrelated_root_spans_do_not_share_a_parent(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        with runtime.span(Scope.NODE, "transform", trace_id=_TRACE_ID_HEX):
            pass
        with runtime.span(Scope.NODE, "enrich", trace_id=_OTHER_TRACE_ID_HEX):
            pass

        spans = exporter.get_finished_spans()
        transform = _span_by_name(spans, "node:transform")
        enrich = _span_by_name(spans, "node:enrich")

        assert _parent_span_id(transform) is None
        assert _parent_span_id(enrich) is None
        assert _trace_id(transform) != _trace_id(enrich)
        assert _attribute(transform, "trace_id") == _TRACE_ID_HEX
        assert _attribute(enrich, "trace_id") == _OTHER_TRACE_ID_HEX


class TestConcurrentRunsAreDisjoint:
    """E7 inverted — two runs of the same scope and name never share a span.

    There is no process-wide registry keyed by ``scope:name:trace_id`` left to
    collide on: each ``span()`` call owns its own span object.
    """

    def test_concurrent_runs_of_the_same_scope_and_name_export_two_trees(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime
        both_open = threading.Barrier(2, timeout=5)

        def _run(run_id: str) -> None:
            with (
                runtime.span(Scope.POLL_CYCLE, "orders", trace_id=_TRACE_ID_HEX, id=run_id),
                runtime.span(Scope.NODE, "transform", trace_id=_TRACE_ID_HEX, id=run_id),
            ):
                both_open.wait()

        threads = [threading.Thread(target=_run, args=(run_id,)) for run_id in ("run-a", "run-b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()

        spans = exporter.get_finished_spans()
        assert len(spans) == 4
        by_run = {
            (str(_attribute(span, "scope")), str(_attribute(span, "id"))): span for span in spans
        }
        assert set(by_run) == {
            ("poll_cycle", "run-a"),
            ("poll_cycle", "run-b"),
            ("node", "run-a"),
            ("node", "run-b"),
        }

        for run_id in ("run-a", "run-b"):
            parent = by_run[("poll_cycle", run_id)]
            child = by_run[("node", run_id)]
            assert _parent_span_id(child) == _span_id(parent)
            assert _parent_span_id(parent) is None

        assert _trace_id(by_run[("poll_cycle", "run-a")]) != _trace_id(
            by_run[("poll_cycle", "run-b")]
        )
        assert _span_id(by_run[("node", "run-a")]) != _span_id(by_run[("node", "run-b")])


class TestSpanAcrossAsendBoundaries:
    """A span opened in one ``asend`` and closed in another.

    ``span()`` cannot serve this shape: its context token would be created in
    one ``asend`` and detached in another, which OTEL swallows into a log line
    and which leaks the span into the consumer between frames. ``open_span``
    never attaches, so neither happens.
    """

    @staticmethod
    def _frames(handle: LoomSpan, *, fail: bool = False) -> AsyncGenerator[str, None]:
        async def _generate() -> AsyncGenerator[str, None]:
            with always_closed(handle):
                yield "first"
                if fail:
                    raise RuntimeError("stream broke")
                yield "second"

        return _generate()

    @staticmethod
    def _detach_failures(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
        return [
            record
            for record in caplog.records
            if record.name == _OTEL_CONTEXT_LOGGER and _DETACH_FAILURE in record.getMessage()
        ]

    @pytest.mark.asyncio
    async def test_frames_pulled_in_one_task_do_not_leak_the_span_between_them(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        """Same-task drive: the failure mode is a leak, not a detach error.

        Both ``asend`` calls share one context, so a token attached in the
        first would detach cleanly in the second — and stay attached in
        between, silently reparenting everything the consumer does.
        """
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        frames = self._frames(handle)

        assert await frames.asend(None) == "first"
        assert trace.get_current_span() is trace.INVALID_SPAN
        assert await frames.asend(None) == "second"
        with pytest.raises(StopAsyncIteration):
            await frames.asend(None)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "agent:agent_run"
        assert spans[0].status.status_code is StatusCode.OK

    @pytest.mark.asyncio
    async def test_frames_pulled_from_different_tasks_log_no_detach_failure(
        self,
        exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Task-per-frame drive: this is the shape that produces the log line.

        Each task copies the context, so a token attached while serving one
        frame does not exist in the context that serves the next. OTEL catches
        the ``ValueError`` and logs it, which is why this is asserted on the
        log and not on a raised exception.
        """
        runtime, exporter = exporting_runtime
        caplog.set_level(logging.DEBUG, logger=_OTEL_CONTEXT_LOGGER)

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        frames = self._frames(handle)

        assert await asyncio.create_task(frames.asend(None)) == "first"
        assert await asyncio.create_task(frames.asend(None)) == "second"
        final_frame = frames.asend(None)

        with pytest.raises(StopAsyncIteration):
            await asyncio.create_task(final_frame)

        assert self._detach_failures(caplog) == []
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code is StatusCode.OK

    @pytest.mark.asyncio
    async def test_a_failure_after_the_first_frame_closes_the_span_as_an_error(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        frames = self._frames(handle, fail=True)

        assert await frames.asend(None) == "first"
        with pytest.raises(RuntimeError):
            await frames.asend(None)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code is StatusCode.ERROR
        assert spans[0].status.description == "stream broke"

    @pytest.mark.asyncio
    async def test_an_abandoned_stream_closes_the_span_exactly_once(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        """A client that walks away ends the generator with ``GeneratorExit``."""
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        frames = self._frames(handle)
        assert await frames.asend(None) == "first"
        await frames.aclose()
        # ``aclose`` already closed the span through ``always_closed``; closing
        # it a second time must not export a second span or emit a second END.
        handle.end()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code is StatusCode.OK

    def test_an_inner_region_can_still_nest_under_an_open_span(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        with handle.as_current(), runtime.span(Scope.TOOL, "create_order"):
            pass
        handle.end()

        spans = exporter.get_finished_spans()
        agent = _span_by_name(spans, "agent:agent_run")
        tool = _span_by_name(spans, "tool:create_order")
        assert _parent_span_id(tool) == _span_id(agent)
        assert _parent_span_id(agent) is None


class TestClosingAnnotations:
    """Values only known when the work ends still reach the span that covers it.

    A run's token usage does not exist when its span opens, so a surface that
    could only supply attributes at open time had to publish none at all.
    """

    def test_an_annotation_reaches_the_exported_span(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        handle.annotate({"gen_ai.usage.input_tokens": 1840})
        handle.end()

        span = _span_by_name(exporter.get_finished_spans(), "agent:agent_run")
        assert _attribute(span, "gen_ai.usage.input_tokens") == 1840

    def test_an_annotation_reaches_the_closing_event_and_not_the_start(self) -> None:
        observer = RecordingObserver()
        runtime, _exporter = _exporting_runtime(observer)

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        handle.annotate({"gen_ai.usage.input_tokens": 1840})
        handle.end()

        start, end = observer.events
        assert "gen_ai.usage.input_tokens" not in start.meta
        assert end.meta["gen_ai.usage.input_tokens"] == 1840
        assert end.meta["agent"] == "analyst"

    def test_a_failing_span_still_carries_what_the_run_spent(self) -> None:
        observer = RecordingObserver()
        runtime, _exporter = _exporting_runtime(observer)

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        handle.annotate({"gen_ai.usage.input_tokens": 1840})
        handle.fail(RuntimeError("provider died"))

        assert observer.events[-1].meta["gen_ai.usage.input_tokens"] == 1840

    def test_annotating_a_closed_span_changes_nothing(
        self, exporting_runtime: tuple[ObservabilityRuntime, InMemorySpanExporter]
    ) -> None:
        runtime, exporter = exporting_runtime

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        handle.end()
        handle.annotate({"gen_ai.usage.input_tokens": 1840})

        span = _span_by_name(exporter.get_finished_spans(), "agent:agent_run")
        assert _attribute(span, "gen_ai.usage.input_tokens") is None


class TestHostProviderCoexistence:
    """Loom's own provider next to a host SDK's, sharing only the context.

    Loom never calls ``set_tracer_provider``. The context is global and the
    provider is not, so nesting works in both directions while each provider
    exports only its own spans — the property that makes running under logfire
    (which does own the global provider) work at all.
    """

    def test_loom_nests_under_a_host_span_and_parents_the_next_host_span(self) -> None:
        host_provider, host_exporter = _exporting_provider()
        loom_provider, loom_exporter = _exporting_provider()
        host_tracer = host_provider.get_tracer("host.sdk")
        runtime = ObservabilityRuntime([], tracer=loom_provider.get_tracer("loom"))

        with (
            host_tracer.start_as_current_span("host:request"),
            runtime.span(Scope.AGENT, "agent_run"),
        ):
            with host_tracer.start_as_current_span("host:chat gpt-4"):
                pass
            with runtime.span(Scope.TOOL, "create_order"):
                pass

        host_spans = host_exporter.get_finished_spans()
        loom_spans = loom_exporter.get_finished_spans()
        assert {span.name for span in host_spans} == {"host:request", "host:chat gpt-4"}
        assert {span.name for span in loom_spans} == {"agent:agent_run", "tool:create_order"}

        request = _span_by_name(host_spans, "host:request")
        llm = _span_by_name(host_spans, "host:chat gpt-4")
        agent = _span_by_name(loom_spans, "agent:agent_run")
        tool = _span_by_name(loom_spans, "tool:create_order")

        assert _parent_span_id(agent) == _span_id(request)
        assert _parent_span_id(llm) == _span_id(agent)
        assert _parent_span_id(tool) == _span_id(agent)
        assert len({_trace_id(span) for span in (request, llm, agent, tool)}) == 1

    def test_an_empty_endpoint_resolves_the_host_provider_installed_later(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``endpoint=""`` yields a proxy tracer, so import order does not matter."""
        monkeypatch.setattr(trace, "_TRACER_PROVIDER", None, raising=False)
        monkeypatch.setattr(trace, "_TRACER_PROVIDER_SET_ONCE", trace.Once(), raising=False)

        runtime = ObservabilityRuntime.from_config(
            ObservabilityConfig(
                otel=OtelObservabilityConfig(enabled=True, config=OtelConfig(endpoint=None))
            )
        )

        host_provider, host_exporter = _exporting_provider()
        monkeypatch.setattr(trace, "_TRACER_PROVIDER", host_provider, raising=False)

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            pass

        spans = host_exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "use_case:CreateOrder"

    def test_a_configured_endpoint_exports_the_tree_with_no_host_provider(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A root scope closing drains Loom's own batch processor.

        Without the flush a ``BatchSpanProcessor`` would still be holding the
        run's spans when a short-lived process exits.
        """
        monkeypatch.setattr(trace, "_TRACER_PROVIDER", None, raising=False)
        monkeypatch.setattr(trace, "_TRACER_PROVIDER_SET_ONCE", trace.Once(), raising=False)
        exporter = InMemorySpanExporter()
        monkeypatch.setattr(_otel_module, "_build_exporter", lambda config: exporter)

        runtime = ObservabilityRuntime.from_config(
            ObservabilityConfig(
                otel=OtelObservabilityConfig(
                    enabled=True,
                    config=OtelConfig(endpoint=Dsn("http://collector.invalid:4318/v1/traces")),
                )
            )
        )

        with (
            runtime.span(Scope.PIPELINE, "DailySales"),
            runtime.span(Scope.STEP, "LoadOrders"),
        ):
            pass

        spans = exporter.get_finished_spans()
        assert {span.name for span in spans} == {"pipeline:DailySales", "step:LoadOrders"}
        pipeline = _span_by_name(spans, "pipeline:DailySales")
        step = _span_by_name(spans, "step:LoadOrders")
        assert _parent_span_id(step) == _span_id(pipeline)
        # Loom built that provider for itself and installed nothing globally:
        # a host SDK is still free to own the process-wide provider.
        assert trace._TRACER_PROVIDER is None


class TestEventStreamIsUnchanged:
    """The structlog and Prometheus observers see exactly what they saw before.

    Tracing moved; the lifecycle event stream did not. These are the sequences
    the fan-out produced before spans were opened by the runtime.
    """

    @staticmethod
    def _projection(event: LifecycleEvent) -> tuple[object, ...]:
        return (
            event.scope,
            event.name,
            event.kind,
            event.trace_id,
            event.correlation_id,
            event.id,
            event.status,
            event.error,
            dict(event.meta),
            event.duration_ms is not None,
        )

    def test_a_successful_nested_run_emits_the_same_four_events(self) -> None:
        observer = RecordingObserver()
        runtime, _ = _exporting_runtime(observer)

        with (
            runtime.span(
                Scope.PIPELINE,
                "DailySales",
                trace_id=_TRACE_ID_HEX,
                correlation_id="corr-1",
                id="run-1",
                attempt=1,
            ),
            runtime.span(Scope.STEP, "LoadOrders", trace_id=_TRACE_ID_HEX),
        ):
            pass

        assert [self._projection(event) for event in observer.events] == [
            (
                Scope.PIPELINE,
                "DailySales",
                EventKind.START,
                _TRACE_ID_HEX,
                "corr-1",
                "run-1",
                None,
                None,
                {"attempt": 1},
                False,
            ),
            (
                Scope.STEP,
                "LoadOrders",
                EventKind.START,
                _TRACE_ID_HEX,
                None,
                None,
                None,
                None,
                {},
                False,
            ),
            (
                Scope.STEP,
                "LoadOrders",
                EventKind.END,
                _TRACE_ID_HEX,
                None,
                None,
                "success",
                None,
                {},
                True,
            ),
            (
                Scope.PIPELINE,
                "DailySales",
                EventKind.END,
                _TRACE_ID_HEX,
                "corr-1",
                "run-1",
                "success",
                None,
                {"attempt": 1},
                True,
            ),
        ]

    def test_a_failing_run_emits_the_same_error_event(self) -> None:
        observer = RecordingObserver()
        runtime, _ = _exporting_runtime(observer)

        with (
            pytest.raises(ValueError),
            runtime.span(Scope.STEP, "LoadOrders", trace_id=_TRACE_ID_HEX, id="step-1"),
        ):
            raise ValueError("boom")

        assert [self._projection(event) for event in observer.events] == [
            (
                Scope.STEP,
                "LoadOrders",
                EventKind.START,
                _TRACE_ID_HEX,
                None,
                "step-1",
                None,
                None,
                {},
                False,
            ),
            (
                Scope.STEP,
                "LoadOrders",
                EventKind.ERROR,
                _TRACE_ID_HEX,
                None,
                "step-1",
                "failure",
                "boom",
                {"error_type": "ValueError"},
                True,
            ),
        ]

    def test_an_observer_failure_still_does_not_interrupt_the_body(self) -> None:
        class _Broken:
            def on_event(self, event: LifecycleEvent) -> None:
                raise RuntimeError("observer is down")

        observer = RecordingObserver()
        provider, exporter = _exporting_provider()
        runtime = ObservabilityRuntime([_Broken(), observer], tracer=provider.get_tracer("loom"))

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            pass

        assert [event.kind for event in observer.events] == [EventKind.START, EventKind.END]
        assert len(exporter.get_finished_spans()) == 1
