"""Tests for ETLExecutor run_step, run_process and run_pipeline."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

import pytest

from loom.core.observability.observability import Observability
from loom.etl import ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.checkpoint import CheckpointCleanupError, CheckpointStore
from loom.etl.compiler import ETLCompiler
from loom.etl.event import EtlEvent
from loom.etl.executor import ETLExecutor, ThreadDispatcher
from loom.etl.lineage._records import (
    EventName as LineageEventName,
)
from loom.etl.lineage._records import (
    RunContext as LineageRunContext,
)
from loom.etl.lineage._records import (
    RunStatus as LineageRunStatus,
)
from loom.etl.testing import InMemorySourceReader, InMemoryTargetWriter, RecordingRunObserver
from tests.unit.etl.doubles import OrdersStagingAStep, RunDateIgnoredParams

ExecutorFactory = Callable[..., tuple[ETLExecutor, InMemoryTargetWriter, RecordingRunObserver]]


def _table_ref(spec: Any) -> Any:
    table_ref = getattr(spec, "table_ref", None)
    assert table_ref is not None
    return table_ref


SENTINEL_A = object()
SENTINEL_B = object()


class CustomersStagingBStep(ETLStep[RunDateIgnoredParams]):
    customers = FromTable("raw.customers")
    target = IntoTable("staging.b").replace()

    def execute(self, params: RunDateIgnoredParams, *, customers: Any) -> Any:  # type: ignore[override]
        return customers


class GeneratedCalendarStep(ETLStep[RunDateIgnoredParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunDateIgnoredParams) -> Any:  # type: ignore[override]
        return {"generated": True}


class StepStreaming(ETLStep[RunDateIgnoredParams]):
    streaming = True
    orders = FromTable("raw.orders")
    target = IntoTable("staging.stream").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class FailingStep(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.fail").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        raise ValueError("intentional failure")


class OrdersCustomersProcess(ETLProcess[RunDateIgnoredParams]):
    steps = [OrdersStagingAStep, CustomersStagingBStep]


class OrdersCustomersParallelProcess(ETLProcess[RunDateIgnoredParams]):
    steps = [[OrdersStagingAStep, CustomersStagingBStep]]


class FailingCleaner:
    def delete_tree(self, path: str) -> None:
        raise CheckpointCleanupError(path)


class PipelineSeq(ETLPipeline[RunDateIgnoredParams]):
    processes = [OrdersCustomersProcess]


class PipelineParallel(ETLPipeline[RunDateIgnoredParams]):
    processes = [[OrdersCustomersProcess]]


@pytest.fixture
def params() -> RunDateIgnoredParams:
    return RunDateIgnoredParams(run_date=date(2024, 1, 5))


@pytest.fixture
def compiler() -> ETLCompiler:
    return ETLCompiler()


@pytest.fixture
def executor_factory() -> ExecutorFactory:
    def _make(
        frames: dict[str, Any] | None = None,
        observer: RecordingRunObserver | None = None,
        dispatcher: Any = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> tuple[ETLExecutor, InMemoryTargetWriter, RecordingRunObserver]:
        writer = InMemoryTargetWriter()
        run_observer = observer or RecordingRunObserver()
        reader = InMemorySourceReader(frames or {"orders": SENTINEL_A, "customers": SENTINEL_B})
        executor = ETLExecutor(
            reader,
            writer,
            observability=Observability([run_observer]),
            dispatcher=dispatcher,
            checkpoint_store=checkpoint_store,
        )
        return executor, writer, run_observer

    return _make


class TestRunStep:
    @pytest.mark.parametrize(
        "step_type,expected_frame,expected_target",
        [
            (OrdersStagingAStep, SENTINEL_A, "staging.a"),
            (CustomersStagingBStep, SENTINEL_B, "staging.b"),
            (GeneratedCalendarStep, {"generated": True}, "staging.calendar"),
        ],
    )
    def test_run_step_writes_expected_output(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
        step_type: type[ETLStep[RunDateIgnoredParams]],
        expected_frame: object,
        expected_target: str,
    ) -> None:
        plan = compiler.compile_step(step_type)
        executor, writer, _ = executor_factory()
        executor.run_step(plan, params)
        assert len(writer.written) == 1
        frame, spec = writer.written[0]
        assert frame == expected_frame
        table_ref = _table_ref(spec)
        assert table_ref.ref == expected_target

    def test_run_step_observer_success_and_metadata(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_step(OrdersStagingAStep)
        executor, _, observer = executor_factory()
        context = LineageRunContext(run_id="fixed-run-id")
        executor.run_step(plan, params, context)

        assert observer.event_names == [LineageEventName.STEP_START, LineageEventName.STEP_END]
        assert observer.step_statuses == [LineageRunStatus.SUCCESS]
        start_event = next(data for name, data in observer.events if name == "step_start")
        assert start_event["step"] == "OrdersStagingAStep"
        assert start_event["run_id"] == "fixed-run-id"

    def test_run_step_forwards_streaming_flag_to_writer(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        streaming_plan = compiler.compile_step(StepStreaming)
        normal_plan = compiler.compile_step(OrdersStagingAStep)
        executor, writer, _ = executor_factory()

        executor.run_step(streaming_plan, params)
        executor.run_step(normal_plan, params)

        assert writer.streaming_flags == [True, False]

    def test_streaming_step_invokes_read_streaming(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
    ) -> None:
        """A streaming step routes through reader.read_streaming, not read."""

        class _TracingReader:
            def __init__(self) -> None:
                self.read_calls: list[Any] = []
                self.stream_calls: list[Any] = []

            def read(self, spec: Any, _params: Any, /) -> Any:
                self.read_calls.append(spec)
                return SENTINEL_A

            def read_streaming(self, spec: Any, _params: Any, /) -> Any:
                self.stream_calls.append(spec)
                return SENTINEL_A

        reader = _TracingReader()
        writer = InMemoryTargetWriter()
        executor = ETLExecutor(
            reader,
            writer,
            observability=Observability([RecordingRunObserver()]),
        )

        streaming_plan = compiler.compile_step(StepStreaming)
        executor.run_step(streaming_plan, params)

        assert len(reader.stream_calls) == 1
        assert reader.read_calls == []

    def test_streaming_step_without_capability_raises(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
    ) -> None:
        """A streaming step refuses to fall back to non-streaming read."""

        class _NonStreamingReader:
            def read(self, spec: Any, _params: Any, /) -> Any:
                return SENTINEL_A

        executor = ETLExecutor(
            _NonStreamingReader(),
            InMemoryTargetWriter(),
            observability=Observability([RecordingRunObserver()]),
        )
        streaming_plan = compiler.compile_step(StepStreaming)

        with pytest.raises(TypeError, match="StreamingSourceReader"):
            executor.run_step(streaming_plan, params)

    def test_run_step_error_events_and_order(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_step(FailingStep)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError, match="intentional failure"):
            executor.run_step(plan, params)

        assert LineageEventName.STEP_ERROR in observer.event_names
        assert observer.step_statuses == [LineageRunStatus.FAILED]
        names = observer.event_names
        assert names.index(LineageEventName.STEP_ERROR) < names.index(LineageEventName.STEP_END)


class TestRunProcess:
    @pytest.mark.parametrize(
        "process_type,expect_order",
        [
            (OrdersCustomersProcess, True),
            (OrdersCustomersParallelProcess, False),
        ],
    )
    def test_run_process_writes_steps(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
        process_type: type[ETLProcess[RunDateIgnoredParams]],
        expect_order: bool,
    ) -> None:
        plan = compiler.compile_process(process_type)
        executor, writer, _ = executor_factory()
        executor.run_process(plan, params)

        assert len(writer.written) == 2
        refs: list[str] = []
        for _, spec in writer.written:
            table_ref = getattr(spec, "table_ref", None)
            if table_ref is not None:
                refs.append(table_ref.ref)
        assert set(refs) == {"staging.a", "staging.b"}
        if expect_order:
            assert refs == ["staging.a", "staging.b"]

    def test_run_process_observer_success_events(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_process(OrdersCustomersProcess)
        executor, _, observer = executor_factory()
        executor.run_process(plan, params)
        assert LineageEventName.PROCESS_START in observer.event_names
        assert LineageEventName.PROCESS_END in observer.event_names
        assert observer.step_statuses == [LineageRunStatus.SUCCESS, LineageRunStatus.SUCCESS]

    def test_run_process_failed_status_on_step_error(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        class ProcFail(ETLProcess[RunDateIgnoredParams]):
            steps = [FailingStep]

        plan = compiler.compile_process(ProcFail)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError):
            executor.run_process(plan, params)

        process_end = next(data for name, data in observer.events if name == "process_end")
        assert process_end["status"] == "failed"


class TestRunPipeline:
    @pytest.mark.parametrize(
        "pipeline_type",
        [PipelineSeq, PipelineParallel],
    )
    def test_run_pipeline_writes_expected_rows(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
        pipeline_type: type[ETLPipeline[RunDateIgnoredParams]],
    ) -> None:
        plan = compiler.compile(pipeline_type)
        executor, writer, _ = executor_factory()
        executor.run_pipeline(plan, params)
        assert len(writer.written) == 2

    def test_run_pipeline_observer_start_end_and_success(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile(PipelineSeq)
        executor, _, observer = executor_factory()
        executor.run_pipeline(plan, params)
        assert LineageEventName.PIPELINE_START in observer.event_names
        assert LineageEventName.PIPELINE_END in observer.event_names
        assert observer.pipeline_statuses == [LineageRunStatus.SUCCESS]

    def test_run_pipeline_failed_status_on_step_error(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        class ProcFail(ETLProcess[RunDateIgnoredParams]):
            steps = [FailingStep]

        class PipelineFail(ETLPipeline[RunDateIgnoredParams]):
            processes = [ProcFail]

        plan = compiler.compile(PipelineFail)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError):
            executor.run_pipeline(plan, params)
        assert observer.pipeline_statuses == [LineageRunStatus.FAILED]

    def test_run_pipeline_logs_instead_of_raising_when_temp_cleanup_fails(
        self,
        compiler: ETLCompiler,
        params: RunDateIgnoredParams,
        executor_factory: ExecutorFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        plan = compiler.compile(PipelineSeq)
        store = CheckpointStore("memory://temps", cleaner=FailingCleaner())
        executor, _, _ = executor_factory(checkpoint_store=store)

        executor.run_pipeline(plan, params)

        assert EtlEvent.CHECKPOINT_RUN_CLEANUP_FAILED in caplog.messages


class TestFlush:
    def test_flush_calls_observer_capability(self) -> None:
        class _FlushObserver:
            def __init__(self) -> None:
                self.calls = 0

            def on_event(self, event: Any) -> None:
                return None

            def flush(self) -> None:
                self.calls += 1

        observer = _FlushObserver()
        executor = ETLExecutor(
            InMemorySourceReader({"raw.orders": object()}),
            InMemoryTargetWriter(),
            observability=Observability([observer]),
        )

        executor.flush()

        assert observer.calls == 1


class TestThreadDispatcher:
    @pytest.mark.parametrize(
        "max_workers,n",
        [(None, 0), (None, 5), (2, 4)],
    )
    def test_thread_dispatcher_runs_all_tasks(self, max_workers: int | None, n: int) -> None:
        results: list[int] = []
        kwargs = {"max_workers": max_workers} if max_workers is not None else {}
        dispatcher = ThreadDispatcher(**kwargs)

        def make_task(value: int) -> Callable[[], None]:
            def task() -> None:
                results.append(value)

            return task

        tasks = [make_task(i) for i in range(n)]
        dispatcher.run_all(tasks)
        assert sorted(results) == list(range(n))

    def test_thread_dispatcher_reraises_first_exception(self) -> None:
        def fail() -> None:
            raise RuntimeError("thread error")

        dispatcher = ThreadDispatcher()
        with pytest.raises(RuntimeError, match="thread error"):
            dispatcher.run_all([fail, fail])
