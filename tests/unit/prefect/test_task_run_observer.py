"""Tests for ``loom.prefect.observer._task_run.PrefectTaskRunObserver``."""

from __future__ import annotations

import uuid
from unittest.mock import patch

from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.prefect.observer import PrefectTaskRunObserver
from loom.prefect.observer._logging_bridge import current_task_run_id


def _start(name: str, step_run_id: str) -> LifecycleEvent:
    event = LifecycleEvent.start(scope=Scope.STEP, name=name)
    return event.__class__(
        scope=event.scope,
        name=event.name,
        kind=event.kind,
        id=step_run_id,
        trace_id=event.trace_id,
        correlation_id=event.correlation_id,
        meta=event.meta,
    )


def _end(name: str, step_run_id: str) -> LifecycleEvent:
    event = LifecycleEvent.end(
        scope=Scope.STEP,
        name=name,
        duration_ms=1.0,
        status=LifecycleStatus.SUCCESS,
    )
    return event.__class__(
        scope=event.scope,
        name=event.name,
        kind=event.kind,
        id=step_run_id,
        trace_id=event.trace_id,
        correlation_id=event.correlation_id,
        meta=event.meta,
        duration_ms=event.duration_ms,
        status=event.status,
    )


class TestPrefectTaskRunObserver:
    def test_step_start_binds_contextvar_step_end_resets(self) -> None:
        observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())
        fake_task_run = uuid.uuid4()

        with (
            patch.object(observer, "_create_task_run", return_value=fake_task_run),
            patch.object(observer, "_set_state"),
        ):
            assert current_task_run_id() is None
            observer.on_event(_start("MyStep", "step-1"))
            assert current_task_run_id() == fake_task_run
            observer.on_event(_end("MyStep", "step-1"))
            assert current_task_run_id() is None

    def test_step_marker_is_cached_per_name(self) -> None:
        observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())

        first = observer._step_marker("StepA")
        second = observer._step_marker("StepA")
        third = observer._step_marker("StepB")

        assert first is second
        assert first is not third

    def test_process_scope_events_are_ignored(self) -> None:
        observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())
        event = LifecycleEvent.start(scope=Scope.PROCESS, name="MyProcess")
        observer.on_event(event)
        assert not observer._task_runs


class TestActiveFlowRunId:
    def test_returns_runtime_id_when_available(self) -> None:
        import types

        stored = uuid.uuid4()
        runtime_id = uuid.uuid4()
        observer = PrefectTaskRunObserver(flow_run_id=stored)

        fake_module = types.SimpleNamespace(flow_run=types.SimpleNamespace(id=str(runtime_id)))
        with patch.dict("sys.modules", {"prefect.runtime": fake_module}):
            result = observer._active_flow_run_id()

        assert result == runtime_id

    def test_falls_back_to_stored_when_runtime_id_is_none(self) -> None:
        import types

        stored = uuid.uuid4()
        observer = PrefectTaskRunObserver(flow_run_id=stored)

        fake_module = types.SimpleNamespace(flow_run=types.SimpleNamespace(id=None))
        with patch.dict("sys.modules", {"prefect.runtime": fake_module}):
            result = observer._active_flow_run_id()

        assert result == stored

    def test_falls_back_to_stored_on_attribute_error(self) -> None:
        import types

        stored = uuid.uuid4()
        observer = PrefectTaskRunObserver(flow_run_id=stored)

        fake_module = types.SimpleNamespace()  # no flow_run attribute
        with patch.dict("sys.modules", {"prefect.runtime": fake_module}):
            result = observer._active_flow_run_id()

        assert result == stored
