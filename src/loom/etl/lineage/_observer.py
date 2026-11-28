"""Lineage observer that persists ETL lifecycle events."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.protocol import LifecycleObserver
from loom.etl.lineage._records import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.lineage.sinks._protocol import LineageStore


class LineageObserver(LifecycleObserver):
    """Persist pipeline, process, and step lineage into an execution store."""

    def __init__(self, store: LineageStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._pipeline: dict[str, _EntityContext] = {}
        self._process: dict[str, _EntityContext] = {}
        self._step: dict[str, _EntityContext] = {}

    def on_event(self, event: LifecycleEvent) -> None:
        """Persist ETL lifecycle events as lineage records."""
        match event.scope:
            case Scope.PIPELINE:
                self._handle_pipeline(event)
            case Scope.PROCESS:
                self._handle_process(event)
            case Scope.STEP:
                self._handle_step(event)
            case _:
                return

    def _handle_pipeline(self, event: LifecycleEvent) -> None:
        run_id = _event_id(event)
        if event.kind is EventKind.START:
            with self._lock:
                self._pipeline[run_id] = _EntityContext.from_event(event)
            return
        if event.kind not in (EventKind.END, EventKind.ERROR):
            return
        with self._lock:
            entity_ctx = self._pipeline.pop(run_id, None)
        if entity_ctx is None:
            return
        self._store.write_record(
            _build_pipeline_record(
                entity_ctx,
                event=event,
            )
        )

    def _handle_process(self, event: LifecycleEvent) -> None:
        process_run_id = _event_id(event)
        if event.kind is EventKind.START:
            with self._lock:
                self._process[process_run_id] = _EntityContext.from_event(event)
            return
        if event.kind not in (EventKind.END, EventKind.ERROR):
            return
        with self._lock:
            entity_ctx = self._process.pop(process_run_id, None)
        if entity_ctx is None:
            return
        self._store.write_record(
            _build_process_record(
                entity_ctx,
                process_run_id=process_run_id,
                event=event,
            )
        )

    def _handle_step(self, event: LifecycleEvent) -> None:
        step_run_id = _event_id(event)
        if event.kind is EventKind.START:
            with self._lock:
                self._step[step_run_id] = _EntityContext.from_event(event)
            return
        if event.kind not in (EventKind.END, EventKind.ERROR):
            return
        with self._lock:
            entity_ctx = self._step.pop(step_run_id, None)
        if entity_ctx is None:
            return
        self._store.write_record(
            _build_step_record(
                entity_ctx,
                step_run_id=step_run_id,
                event=event,
            )
        )


@dataclass(frozen=True)
class _EntityContext:
    run_ctx: RunContext
    name: str
    started_at: datetime

    @classmethod
    def from_event(cls, event: LifecycleEvent) -> _EntityContext:
        return cls(
            run_ctx=RunContext(
                run_id=_meta_str(event, "run_id") or _event_id(event),
                correlation_id=event.correlation_id,
                attempt=_meta_int(event, "attempt") or 1,
                last_attempt=_meta_bool(event, "last_attempt", True),
                process_run_id=_meta_str(event, "process_run_id"),
            ),
            name=event.name,
            started_at=_now(),
        )


def _build_pipeline_record(
    entity_ctx: _EntityContext,
    *,
    event: LifecycleEvent,
) -> PipelineRunRecord:
    return PipelineRunRecord(
        event=EventName.PIPELINE_END,
        run_id=entity_ctx.run_ctx.run_id,
        correlation_id=entity_ctx.run_ctx.correlation_id,
        attempt=entity_ctx.run_ctx.attempt,
        pipeline=entity_ctx.name,
        started_at=entity_ctx.started_at,
        status=_event_status(event),
        duration_ms=_duration_ms(event),
        error_type=_error_type(event),
        error_code=_error_code(event),
    )


def _build_process_record(
    entity_ctx: _EntityContext,
    *,
    process_run_id: str,
    event: LifecycleEvent,
) -> ProcessRunRecord:
    return ProcessRunRecord(
        event=EventName.PROCESS_END,
        run_id=entity_ctx.run_ctx.run_id,
        correlation_id=entity_ctx.run_ctx.correlation_id,
        attempt=entity_ctx.run_ctx.attempt,
        process_run_id=process_run_id,
        process=entity_ctx.name,
        started_at=entity_ctx.started_at,
        status=_event_status(event),
        duration_ms=_duration_ms(event),
        error_type=_error_type(event),
        error_code=_error_code(event),
    )


def _build_step_record(
    entity_ctx: _EntityContext,
    *,
    step_run_id: str,
    event: LifecycleEvent,
) -> StepRunRecord:
    return StepRunRecord(
        event=EventName.STEP_END,
        run_id=entity_ctx.run_ctx.run_id,
        correlation_id=entity_ctx.run_ctx.correlation_id,
        attempt=entity_ctx.run_ctx.attempt,
        step_run_id=step_run_id,
        step=entity_ctx.name,
        started_at=entity_ctx.started_at,
        status=_event_status(event),
        duration_ms=_duration_ms(event),
        process_run_id=entity_ctx.run_ctx.process_run_id,
        error_type=_error_type(event),
        error_code=_error_code(event),
    )


def _error_type(event: LifecycleEvent) -> str | None:
    return event.failure.type if event.failure else None


def _error_code(event: LifecycleEvent) -> str | None:
    return event.failure.code if event.failure else None


def _event_id(event: LifecycleEvent) -> str:
    return event.id or _meta_str(event, "run_id") or event.name


def _event_status(event: LifecycleEvent) -> RunStatus:
    if event.kind is EventKind.ERROR:
        return RunStatus.FAILED
    return RunStatus.SUCCESS


def _duration_ms(event: LifecycleEvent) -> int:
    if event.duration_ms is None:
        return 0
    return int(event.duration_ms)


def _meta_str(event: LifecycleEvent, key: str) -> str | None:
    value = event.meta.get(key)
    return value if isinstance(value, str) else None


def _meta_int(event: LifecycleEvent, key: str) -> int | None:
    value = event.meta.get(key)
    return value if isinstance(value, int) else None


def _meta_bool(event: LifecycleEvent, key: str, default: bool) -> bool:
    value = event.meta.get(key)
    return value if isinstance(value, bool) else default


def _now() -> datetime:
    return datetime.now(tz=UTC)


__all__ = ["LineageObserver"]
