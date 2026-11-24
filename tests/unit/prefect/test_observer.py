"""PrefectObserver writes loom's lifecycle records and the pipeline summary artifact."""

from __future__ import annotations

import logging

import prefect.artifacts
import pytest

from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.prefect.observer import PrefectObserver

_STDLIB_LOGGER = "loom.core.observability.observer.log"
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
        error=ValueError(),
    )


def _records(caplog: pytest.LogCaptureFixture, logger_name: str) -> list[tuple[int, str]]:
    return [
        (record.levelno, record.message) for record in caplog.records if record.name == logger_name
    ]


class TestPrefectObserver:
    @pytest.mark.parametrize(
        ("event", "level", "message"),
        [
            (_start_event(), logging.INFO, ObservabilityEvent.LIFECYCLE_STARTED),
            (_end_event(), logging.INFO, ObservabilityEvent.LIFECYCLE_ENDED),
            (_error_event(), logging.ERROR, ObservabilityEvent.LIFECYCLE_FAILED),
            (
                LifecycleEvent.start(scope=Scope.STEP, name="TransformStep"),
                logging.DEBUG,
                ObservabilityEvent.LIFECYCLE_STARTED,
            ),
            (
                LifecycleEvent.end(
                    scope=Scope.STEP,
                    name="TransformStep",
                    duration_ms=10.0,
                    status=LifecycleStatus.SUCCESS,
                ),
                logging.INFO,
                ObservabilityEvent.LIFECYCLE_ENDED,
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
        caplog.set_level(logging.DEBUG)
        observer = PrefectObserver()

        observer.on_event(event)

        assert _records(caplog, _STDLIB_LOGGER) == [(level, message)]
        assert _records(caplog, _RUN_LOGGER) == []

    def test_inside_a_run_the_event_is_the_loom_record_not_a_run_logger_line(
        self, prefect_run_context: None, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.INFO)

        PrefectObserver().on_event(_error_event("FailedStep"))

        assert _records(caplog, _STDLIB_LOGGER) == [
            (logging.ERROR, ObservabilityEvent.LIFECYCLE_FAILED)
        ]
        assert _records(caplog, _RUN_LOGGER) == []

    def test_a_pipeline_end_inside_a_flow_run_publishes_its_summary_artifact(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        published: list[dict[str, str]] = []
        monkeypatch.setenv("PREFECT__FLOW_RUN_ID", "0b6c2f0e-6f43-4a39-9d4c-1d1e2f3a4b5c")
        monkeypatch.setattr(
            prefect.artifacts, "create_markdown_artifact", lambda **kw: published.append(kw)
        )

        PrefectObserver().on_event(_end_event("Nightly"))

        summary = "## Nightly\n\n- Status: success\n- Duration: 123.0ms\n"
        assert published == [{"key": "pipeline-summary", "markdown": summary}]
