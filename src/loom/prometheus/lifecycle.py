"""Prometheus lifecycle adapter — records LifecycleEvent as Prometheus instruments."""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.model.redaction import Dsn
from loom.core.observability.event import EventKind, LifecycleEvent
from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

_DURATION_SECONDS = histogram_spec(
    "lifecycle_duration_seconds",
    "Lifecycle span wall-clock duration in seconds.",
    "scope",
    "name",
)
_ERRORS_TOTAL = counter_spec(
    "lifecycle_errors_total",
    "Total lifecycle span errors by scope and name.",
    "scope",
    "name",
)
_INSTRUMENTS = (_DURATION_SECONDS, _ERRORS_TOTAL)


class PrometheusLifecycleAdapter:
    """Lifecycle observer that records span durations and errors as Prometheus metrics.

    Instruments:

    - ``lifecycle_duration_seconds`` — Histogram, labels: ``scope``, ``name``.
      Observed on ``END`` events when ``duration_ms`` is present.
    - ``lifecycle_errors_total`` — Counter, labels: ``scope``, ``name``.
      Incremented on ``ERROR`` events.

    For long-running services (streaming, REST), metrics are served at ``/metrics``
    via the standard ``prometheus_client`` default registry. For batch jobs (ETL),
    pass ``pushgateway_url`` to push metrics to a Pushgateway at runner shutdown.

    Args:
        registry: Optional custom ``CollectorRegistry``. Pass an isolated registry
            in tests to prevent cross-test pollution.
        pushgateway_url: Pushgateway URL for batch-job metric delivery.
            Call :meth:`flush` explicitly at runner shutdown when set.

    Example::

        adapter = PrometheusLifecycleAdapter()
        runtime = ObservabilityRuntime([adapter])

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            ...

        # For ETL batch jobs:
        adapter.flush(job_name="ingest_orders")
    """

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        *,
        pushgateway_url: Dsn | str | None = None,
    ) -> None:
        instruments = cached_instruments(registry, _INSTRUMENTS)
        self._duration = instruments[_DURATION_SECONDS]
        self._errors = instruments[_ERRORS_TOTAL]
        self._pushgateway_url = pushgateway_url
        self._registry = registry

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one lifecycle event on Prometheus instruments.

        Args:
            event: Lifecycle event from the runtime.
        """
        if event.kind is EventKind.END and event.duration_ms is not None:
            self._duration.labels(scope=event.scope, name=event.name).observe(
                event.duration_ms / 1000
            )
        elif event.kind is EventKind.ERROR:
            self._errors.labels(scope=event.scope, name=event.name).inc()

    def flush(self, *, job_name: str = "loom_etl") -> None:
        """Push accumulated metrics to the configured Pushgateway.

        A no-op when no ``pushgateway_url`` was provided at construction.

        Args:
            job_name: Pushgateway job label. Defaults to ``"loom_etl"``.

        Raises:
            RuntimeError: If ``prometheus_client.push_to_gateway`` fails.
        """
        url = self._pushgateway_url
        if url is None:
            return
        from prometheus_client import push_to_gateway

        registry = self._registry
        if registry is None:
            from prometheus_client import REGISTRY

            registry = REGISTRY
        gateway = url.reveal() if isinstance(url, Dsn) else url
        push_to_gateway(gateway, job=job_name, registry=registry)


__all__ = ["PrometheusLifecycleAdapter"]
