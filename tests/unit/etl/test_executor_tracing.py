"""The span tree a real ETL run exports.

``ETLExecutor`` opens three nested scopes — pipeline, process, step. This
asserts the exported ``parent_span_id`` chain rather than the lifecycle
events, which ``test_executor`` already covers.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from loom.core.observability.runtime import ObservabilityRuntime
from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor
from loom.etl.testing import StubSourceReader, StubTargetWriter


class TracingParams(ETLParams):  # type: ignore[misc]
    run_date: date


class LoadOrders(ETLStep[TracingParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: TracingParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class FailingLoad(ETLStep[TracingParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: TracingParams, *, orders: Any) -> Any:  # type: ignore[override]
        raise ValueError("intentional failure")


class LoadReturns(ETLStep[TracingParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.returns").replace()

    def execute(self, params: TracingParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class Staging(ETLProcess[TracingParams]):
    steps = [LoadOrders]


class ParallelStaging(ETLProcess[TracingParams]):
    steps = [[LoadOrders, LoadReturns]]


class FailingStaging(ETLProcess[TracingParams]):
    steps = [FailingLoad]


class DailySales(ETLPipeline[TracingParams]):
    processes = [Staging]


class FailingSales(ETLPipeline[TracingParams]):
    processes = [FailingStaging]


class ParallelSales(ETLPipeline[TracingParams]):
    processes = [ParallelStaging]


def _executor() -> tuple[ETLExecutor, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    executor = ETLExecutor(
        StubSourceReader({"orders": object()}),
        StubTargetWriter(),
        observability=ObservabilityRuntime([], tracer=provider.get_tracer("loom.etl")),
    )
    return executor, exporter


def _by_name(spans: tuple[ReadableSpan, ...], name: str) -> ReadableSpan:
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


def test_a_sequential_pipeline_exports_one_tree() -> None:
    executor, exporter = _executor()
    plan = ETLCompiler().compile(DailySales)

    executor.run_pipeline(plan, TracingParams(run_date=date(2024, 1, 5)))

    spans = exporter.get_finished_spans()
    assert {span.name for span in spans} == {
        "pipeline:DailySales",
        "process:Staging",
        "step:LoadOrders",
    }
    pipeline = _by_name(spans, "pipeline:DailySales")
    process = _by_name(spans, "process:Staging")
    step = _by_name(spans, "step:LoadOrders")

    assert _parent_span_id(step) == _span_id(process)
    assert _parent_span_id(process) == _span_id(pipeline)
    assert _parent_span_id(pipeline) is None
    assert len({_trace_id(span) for span in spans}) == 1


def test_a_parallel_group_nests_its_steps_under_the_process() -> None:
    executor, exporter = _executor()
    plan = ETLCompiler().compile(ParallelSales)

    executor.run_pipeline(plan, TracingParams(run_date=date(2024, 1, 5)))

    spans = exporter.get_finished_spans()
    process = _by_name(spans, "process:ParallelStaging")
    for name in ("step:LoadOrders", "step:LoadReturns"):
        assert _parent_span_id(_by_name(spans, name)) == _span_id(process)
    assert len({_trace_id(span) for span in spans}) == 1


def test_a_failing_step_marks_its_whole_branch_as_an_error() -> None:
    executor, exporter = _executor()
    plan = ETLCompiler().compile(FailingSales)

    params = TracingParams(run_date=date(2024, 1, 5))

    with pytest.raises(ValueError):
        executor.run_pipeline(plan, params)

    spans = exporter.get_finished_spans()
    assert {span.name for span in spans} == {
        "pipeline:FailingSales",
        "process:FailingStaging",
        "step:FailingLoad",
    }
    for span in spans:
        assert span.status.status_code is StatusCode.ERROR
        assert span.status.description == "intentional failure"
    assert _parent_span_id(_by_name(spans, "step:FailingLoad")) == _span_id(
        _by_name(spans, "process:FailingStaging")
    )
