"""Tests for the ETL lineage observer."""

from __future__ import annotations

from loom.core.logger import LogField
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.etl.lineage._observer import LineageObserver
from loom.etl.lineage._records import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunStatus,
    StepRunRecord,
)
from loom.etl.log_events import EtlLogField


class _CaptureStore:
    def __init__(self) -> None:
        self.records: list[object] = []

    def write_record(self, record: object) -> None:
        self.records.append(record)


class TestLineageObserver:
    def test_lineage_observer_writes_pipeline_record_on_end(self) -> None:
        store = _CaptureStore()
        observer = LineageObserver(store)

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.PIPELINE,
                name="DailyPipeline",
                id="run-1",
                attributes={
                    EtlLogField.RUN_ID: "run-1",
                    LogField.ATTEMPT: 2,
                    EtlLogField.LAST_ATTEMPT: False,
                },
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.PIPELINE,
                name="DailyPipeline",
                id="run-1",
                duration_ms=12.0,
                status=LifecycleStatus.SUCCESS,
                attributes={EtlLogField.RUN_ID: "run-1", LogField.ATTEMPT: 2},
            )
        )

        assert len(store.records) == 1
        [record] = store.records
        assert isinstance(record, PipelineRunRecord)
        assert record.event is EventName.PIPELINE_END
        assert record.run_id == "run-1"
        assert record.attempt == 2
        assert record.status is RunStatus.SUCCESS
        assert record.duration_ms == 12

    def test_lineage_observer_writes_step_error_details_on_failure(self) -> None:
        store = _CaptureStore()
        observer = LineageObserver(store)

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.STEP,
                name="BuildOrders",
                id="step-1",
                correlation_id="corr-1",
                attributes={
                    EtlLogField.RUN_ID: "run-1",
                    LogField.ATTEMPT: 1,
                    EtlLogField.PROCESS_RUN_ID: "proc-1",
                },
            )
        )
        observer.on_event(
            LifecycleEvent.exception(
                scope=Scope.STEP,
                name="BuildOrders",
                id="step-1",
                correlation_id="corr-1",
                duration_ms=7.5,
                failure=RuntimeError("boom"),
                attributes={
                    EtlLogField.RUN_ID: "run-1",
                    LogField.ATTEMPT: 1,
                    EtlLogField.PROCESS_RUN_ID: "proc-1",
                },
            )
        )

        assert len(store.records) == 1
        [record] = store.records
        assert isinstance(record, StepRunRecord)
        assert record.event is EventName.STEP_END
        assert record.step == "BuildOrders"
        assert record.process_run_id == "proc-1"
        assert record.status is RunStatus.FAILED
        assert record.error == "boom"
        assert record.error_type == "RuntimeError"
        assert record.error_message == "boom"

    def test_lineage_observer_handles_process_record_and_defaults(self) -> None:
        store = _CaptureStore()
        observer = LineageObserver(store)

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.PROCESS,
                name="BuildCatalog",
                id="proc-1",
                correlation_id="corr-1",
                attributes={EtlLogField.RUN_ID: "run-1"},
            )
        )
        observer.on_event(
            LifecycleEvent.exception(
                scope=Scope.PROCESS,
                name="BuildCatalog",
                id="proc-1",
                correlation_id="corr-1",
                failure=ValueError("boom"),
                attributes={EtlLogField.RUN_ID: "run-1"},
            )
        )

        assert len(store.records) == 1
        [record] = store.records
        assert isinstance(record, ProcessRunRecord)
        assert record.event is EventName.PROCESS_END
        assert record.run_id == "run-1"
        assert record.attempt == 1
        assert record.status is RunStatus.FAILED
        assert record.error == "boom"
        assert record.error_type == "ValueError"

    def test_lineage_observer_handles_step_success_and_ignores_unknown_scope(self) -> None:
        store = _CaptureStore()
        observer = LineageObserver(store)

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.STEP,
                name="Transform",
                id="step-1",
                attributes={EtlLogField.RUN_ID: "run-1", EtlLogField.PROCESS_RUN_ID: "proc-1"},
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.STEP,
                name="Transform",
                id="step-1",
                duration_ms=2.0,
                status=LifecycleStatus.SUCCESS,
                attributes={EtlLogField.RUN_ID: "run-1", EtlLogField.PROCESS_RUN_ID: "proc-1"},
            )
        )
        observer.on_event(
            LifecycleEvent(
                scope=Scope.USE_CASE,
                name="ignored",
                kind=EventKind.START,
            )
        )

        assert len(store.records) == 1
        [record] = store.records
        assert isinstance(record, StepRunRecord)
        assert record.event is EventName.STEP_END
        assert record.process_run_id == "proc-1"
        assert record.status is RunStatus.SUCCESS
        assert record.duration_ms == 2

    def test_lineage_observer_ignores_terminal_events_without_start(self) -> None:
        store = _CaptureStore()
        observer = LineageObserver(store)

        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.PIPELINE,
                name="DailyPipeline",
                id="missing",
                duration_ms=1.0,
                status=LifecycleStatus.SUCCESS,
            )
        )

        assert store.records == []
