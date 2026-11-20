"""Unit tests for PrometheusMetricsRecorder."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from loom.core.engine.events import AbandonReason, EventKind, RuntimeEvent
from loom.prometheus import PrometheusMetricsRecorder


def _adapter() -> tuple[PrometheusMetricsRecorder, CollectorRegistry]:
    registry = CollectorRegistry()
    return PrometheusMetricsRecorder(registry=registry), registry


def _metric_value(registry: CollectorRegistry, sample_name: str, labels: dict[str, str]) -> float:
    """Look up a sample value by its full sample name (e.g. 'loom_usecase_requests_total')."""
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name == sample_name and all(
                sample.labels.get(k) == v for k, v in labels.items()
            ):
                return sample.value
    return 0.0


class TestExecDone:
    def test_increments_requests_total_success(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name="CreateOrder",
                status="success",
                duration_ms=10.0,
            )
        )
        # Counter named "loom_usecase_requests" → sample name "loom_usecase_requests_total"
        val = _metric_value(
            registry,
            "loom_usecase_requests_total",
            {"usecase": "CreateOrder", "status": "success"},
        )
        assert val == pytest.approx(1.0)

    def test_observes_duration_histogram(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name="GetOrder",
                duration_ms=500.0,
            )
        )
        count = _metric_value(
            registry,
            "loom_usecase_duration_seconds_count",
            {"usecase": "GetOrder"},
        )
        assert count == pytest.approx(1.0)

    def test_noop_when_duration_none(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name="GetOrder",
                duration_ms=None,
            )
        )
        count = _metric_value(
            registry,
            "loom_usecase_duration_seconds_count",
            {"usecase": "GetOrder"},
        )
        assert count == pytest.approx(0.0)


class TestExecError:
    def test_increments_requests_total_with_status(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name="CreateOrder",
                status="rule_failure",
                error=ValueError("bad"),
            )
        )
        val = _metric_value(
            registry,
            "loom_usecase_requests_total",
            {"usecase": "CreateOrder", "status": "rule_failure"},
        )
        assert val == pytest.approx(1.0)

    def test_increments_errors_total_by_error_kind(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name="CreateOrder",
                error=ValueError("oops"),
            )
        )
        # Counter named "loom_usecase_errors" → sample name "loom_usecase_errors_total"
        val = _metric_value(
            registry,
            "loom_usecase_errors_total",
            {"usecase": "CreateOrder", "error_kind": "ValueError"},
        )
        assert val == pytest.approx(1.0)

    def test_error_kind_unknown_when_error_none(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name="X",
                error=None,
            )
        )
        val = _metric_value(
            registry,
            "loom_usecase_errors_total",
            {"usecase": "X", "error_kind": "unknown"},
        )
        assert val == pytest.approx(1.0)


class TestExecStart:
    def test_noop_on_exec_start(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_START,
                use_case_name="CreateOrder",
            )
        )
        # No requests should be recorded for EXEC_START
        val = _metric_value(
            registry,
            "loom_usecase_requests_total",
            {"usecase": "CreateOrder", "status": "success"},
        )
        assert val == pytest.approx(0.0)


class TestJobEvents:
    def test_counts_a_job_event_by_kind_job_and_reason(self) -> None:
        adapter, registry = _adapter()
        for _ in range(2):
            adapter.on_event(
                RuntimeEvent(
                    kind=EventKind.JOB_ABANDONED,
                    use_case_name="SendEmail",
                    reason=AbandonReason.ATTEMPTS_EXHAUSTED,
                )
            )

        count = _metric_value(
            registry,
            "loom_job_events_total",
            {"kind": "job_abandoned", "job": "SendEmail", "reason": "attempts_exhausted"},
        )
        assert count == pytest.approx(2.0)

    def test_counts_an_event_that_has_no_reason_under_an_empty_one(self) -> None:
        adapter, registry = _adapter()
        adapter.on_event(RuntimeEvent(kind=EventKind.JOB_SUCCEEDED, use_case_name="SendEmail"))

        count = _metric_value(
            registry,
            "loom_job_events_total",
            {"kind": "job_succeeded", "job": "SendEmail", "reason": ""},
        )
        assert count == pytest.approx(1.0)
