"""Tests for loom.prefect.observer.PrefectObserver.

Verifies:
- PrefectObserver implements the LifecycleObserver protocol (has on_event).
- on_event() logs every event kind on the stdlib logger when no Prefect run is active.
- on_event() logs on the Prefect run logger, at the event's level, inside a flow run.
"""

from __future__ import annotations

import logging

import pytest

from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.prefect.observer import PrefectObserver

_STDLIB_LOGGER = "loom.prefect.observer._flow"
_RUN_LOGGER = "prefect.flow_runs"


def _start_event(name: str = "MyPipeline") -> LifecycleEvent:
    return LifecycleEvent.start(
        scope=Scope.PIPELINE,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
    )


def _end_event(name: str = "MyPipeline") -> LifecycleEvent:
    return LifecycleEvent.end(
        scope=Scope.PIPELINE,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
        duration_ms=123.0,
        status=LifecycleStatus.SUCCESS,
    )


def _error_event(name: str = "MyStep") -> LifecycleEvent:
    return LifecycleEvent.exception(
        scope=Scope.STEP,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
        duration_ms=5.0,
        failure=RuntimeError("something went wrong"),
    )


def _records(caplog: pytest.LogCaptureFixture, logger_name: str) -> list[tuple[int, str]]:
    return [
        (record.levelno, record.message) for record in caplog.records if record.name == logger_name
    ]


class TestPrefectObserver:
    # ---------------------------------------------------------------------------
    # Construction
    # ---------------------------------------------------------------------------

    def test_can_be_instantiated(self) -> None:
        """PrefectObserver can be constructed without arguments."""
        observer = PrefectObserver()
        assert observer is not None

    def test_has_on_event_method(self) -> None:
        """PrefectObserver exposes an on_event method."""
        observer = PrefectObserver()
        assert callable(getattr(observer, "on_event", None))

    # ---------------------------------------------------------------------------
    # No Prefect run context
    # ---------------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("event", "level", "message"),
        [
            (_start_event(), logging.INFO, "[pipeline] START MyPipeline"),
            (_end_event(), logging.INFO, "[pipeline] END MyPipeline 123.0ms"),
            (_error_event(), logging.ERROR, "[step] ERROR MyStep: RuntimeError"),
            (
                LifecycleEvent.start(scope=Scope.STEP, name="TransformStep"),
                logging.INFO,
                "[step] START TransformStep",
            ),
            (
                LifecycleEvent.end(
                    scope=Scope.STEP,
                    name="TransformStep",
                    duration_ms=10.0,
                    status=LifecycleStatus.SUCCESS,
                ),
                logging.INFO,
                "[step] END TransformStep 10.0ms",
            ),
        ],
        ids=["pipeline-start", "pipeline-end", "step-error", "step-start", "step-end"],
    )
    def test_on_event_logs_on_the_stdlib_logger_outside_a_run(
        self,
        event: LifecycleEvent,
        level: int,
        message: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Each lifecycle kind is logged on the stdlib logger when no run is active."""
        caplog.set_level(logging.INFO)
        observer = PrefectObserver()

        observer.on_event(event)

        assert _records(caplog, _STDLIB_LOGGER) == [(level, message)]
        assert _records(caplog, _RUN_LOGGER) == []

    # ---------------------------------------------------------------------------
    # With a Prefect flow-run context
    # ---------------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("event", "level", "message"),
        [
            (_start_event("SomePipeline"), logging.INFO, "[pipeline] START SomePipeline"),
            (
                _error_event("FailedStep"),
                logging.ERROR,
                "[step] ERROR FailedStep: RuntimeError",
            ),
        ],
        ids=["start", "error"],
    )
    def test_on_event_logs_on_the_prefect_run_logger_inside_a_run(
        self,
        event: LifecycleEvent,
        level: int,
        message: str,
        prefect_run_context: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Events go to the Prefect run logger, at their level, when a flow run is active."""
        caplog.set_level(logging.INFO)
        observer = PrefectObserver()

        observer.on_event(event)

        assert _records(caplog, _RUN_LOGGER) == [(level, message)]
        assert _records(caplog, _STDLIB_LOGGER) == []
