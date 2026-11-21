"""Prometheus metrics recorder for Loom use-case events.

Implements the :class:`~loom.core.engine.execution_observer.ExecutionObserver` protocol
using ``prometheus_client`` instruments.  Install the optional dependency
with::

    pip install "loom-py[prometheus]"

Usage::

    from loom.prometheus import PrometheusExecutionObserver

    recorder = PrometheusExecutionObserver()

    kernel = create_kernel(
        config=cfg,
        use_cases=[CreateOrderUseCase],
        modules=[register_repositories],
        execution_observer=recorder,
    )
    app = create_fastapi_app(kernel, RouteSources(python=[OrderInterface]))

:func:`~loom.composition.create_app` builds and passes one itself when
``observability.prometheus.enabled`` is set.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.engine.events import EventKind, ExecutionEvent
from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

_REQUESTS_TOTAL = counter_spec(
    "loom_usecase_requests",
    "Total number of use-case executions by outcome.",
    "usecase",
    "status",
)
_DURATION_SECONDS = histogram_spec(
    "loom_usecase_duration_seconds",
    "Use-case execution wall-clock time in seconds.",
    "usecase",
)
_ERRORS_TOTAL = counter_spec(
    "loom_usecase_errors",
    "Total number of use-case execution errors by error kind.",
    "usecase",
    "error_kind",
)
_JOB_EVENTS_TOTAL = counter_spec(
    "loom_job_events",
    "Total number of job events by kind, job and abandonment reason.",
    "kind",
    "job",
    "reason",
)
_JOB_EVENT_KINDS = frozenset(kind for kind in EventKind if kind.startswith("job_"))
_INSTRUMENTS = (_REQUESTS_TOTAL, _DURATION_SECONDS, _ERRORS_TOTAL, _JOB_EVENTS_TOTAL)


class PrometheusExecutionObserver:
    """Prometheus implementation of the ``ExecutionObserver`` protocol.

    Records the use-case instruments and one for jobs:

    - ``loom_usecase_requests_total`` — Counter, labels: ``usecase``, ``status``.
    - ``loom_usecase_duration_seconds`` — Histogram, labels: ``usecase``.
    - ``loom_usecase_errors_total`` — Counter, labels: ``usecase``, ``error_kind``.
    - ``loom_job_events_total`` — Counter, labels: ``kind``, ``job``, ``reason``.

    Event mapping:

    ============  ============================================================
    EventKind     Action
    ============  ============================================================
    EXEC_START    noop — duration is measured end-to-end and arrives in EXEC_DONE
    EXEC_DONE     increment ``requests_total{status="success"}``; observe histogram
    EXEC_ERROR    increment ``requests_total{status=<event.status>}``; increment
                  ``errors_total{error_kind=<type(error).__name__>}``
    JOB_*         increment ``job_events_total{kind, job, reason}``; the reason
                  is the abandonment reason on ``JOB_ABANDONED``, else empty
    others        noop
    ============  ============================================================

    Args:
        registry: Optional ``CollectorRegistry``.  Defaults to
            ``prometheus_client.REGISTRY`` (the global default registry).
            Pass a custom registry in tests to avoid cross-test pollution.
            Multiple recorder instances sharing the default registry reuse
            the same instruments — no duplicate-collector errors.

    Example::

        from loom.prometheus import PrometheusExecutionObserver

        recorder = PrometheusExecutionObserver()
        kernel = create_kernel(config=cfg, use_cases=[...], execution_observer=recorder)
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        instruments = cached_instruments(registry, _INSTRUMENTS)
        self._requests_total = instruments[_REQUESTS_TOTAL]
        self._duration_seconds = instruments[_DURATION_SECONDS]
        self._errors_total = instruments[_ERRORS_TOTAL]
        self._job_events_total = instruments[_JOB_EVENTS_TOTAL]

    def on_event(self, event: ExecutionEvent) -> None:
        """Process a runtime event and update Prometheus instruments.

        Args:
            event: Immutable event from the executor or compiler.
        """
        if event.kind in _JOB_EVENT_KINDS:
            self._job_events_total.labels(
                kind=event.kind.value,
                job=event.use_case_name,
                reason=event.reason or "",
            ).inc()

        elif event.kind == EventKind.EXEC_DONE:
            self._requests_total.labels(
                usecase=event.use_case_name,
                status=event.status or "success",
            ).inc()
            if event.duration_ms is not None:
                self._duration_seconds.labels(
                    usecase=event.use_case_name,
                ).observe(event.duration_ms / 1000)

        elif event.kind == EventKind.EXEC_ERROR:
            self._requests_total.labels(
                usecase=event.use_case_name,
                status=event.status or "failure",
            ).inc()
            error_kind = type(event.error).__name__ if event.error is not None else "unknown"
            self._errors_total.labels(
                usecase=event.use_case_name,
                error_kind=error_kind,
            ).inc()
