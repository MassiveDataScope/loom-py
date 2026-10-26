"""Prometheus metrics of the relational connection pool.

Gauges are read from the pool when the registry is scraped, never on the
request path. Checkout duration and checkout timeouts are recorded by the
instrumented pool class, which exists only when this sink is configured.

Metrics, labelled ``application``, ``entrypoint`` and ``database``:

- ``loom_db_pool_size`` — connections the pool keeps open.
- ``loom_db_pool_checked_out`` — connections in use.
- ``loom_db_pool_overflow`` — overflow connections in use beyond the size.
- ``loom_db_pool_checkout_seconds`` — histogram of checkout duration.
- ``loom_db_pool_checkout_timeouts_total`` — checkouts that ran out of time.
"""

from __future__ import annotations

import weakref
from collections.abc import Iterator
from threading import Lock
from typing import Any
from weakref import WeakKeyDictionary

from prometheus_client import REGISTRY, CollectorRegistry
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec

_LABELS = ("application", "entrypoint", "database")

_CHECKOUT_SECONDS = histogram_spec(
    "loom_db_pool_checkout_seconds", "Connection checkout duration in seconds.", *_LABELS
)
_CHECKOUT_TIMEOUTS = counter_spec(
    "loom_db_pool_checkout_timeouts", "Connection checkouts that ran out of time.", *_LABELS
)
_INSTRUMENTS = (_CHECKOUT_SECONDS, _CHECKOUT_TIMEOUTS)


class _PoolGauges(Collector):
    """One collector per registry, reading every registered engine's pool on scrape."""

    def __init__(self) -> None:
        # Keyed by labels: a later engine with the same labels (a rebuilt app)
        # replaces the earlier one instead of exporting a duplicate series.
        self._engines: dict[tuple[str, str, str], weakref.ref[Any]] = {}
        self._lock = Lock()

    def add(self, engine: Any, labels: tuple[str, str, str]) -> None:
        with self._lock:
            self._engines[labels] = weakref.ref(engine)

    def describe(self) -> Iterator[GaugeMetricFamily]:
        yield from self._families()

    def collect(self) -> Iterator[GaugeMetricFamily]:
        size, checked_out, overflow = self._families()
        with self._lock:
            self._engines = {
                labels: ref for labels, ref in self._engines.items() if ref() is not None
            }
            alive = [(ref(), labels) for labels, ref in self._engines.items()]
        for engine, labels in alive:
            if engine is None:
                continue
            pool = engine.pool
            size.add_metric(labels, pool.size())
            checked_out.add_metric(labels, pool.checkedout())
            overflow.add_metric(labels, max(0, pool.overflow()))
        yield from (size, checked_out, overflow)

    @staticmethod
    def _families() -> tuple[GaugeMetricFamily, GaugeMetricFamily, GaugeMetricFamily]:
        return (
            GaugeMetricFamily(
                "loom_db_pool_size", "Connections the pool keeps open.", labels=_LABELS
            ),
            GaugeMetricFamily("loom_db_pool_checked_out", "Connections in use.", labels=_LABELS),
            GaugeMetricFamily(
                "loom_db_pool_overflow", "Overflow connections in use.", labels=_LABELS
            ),
        )


_GAUGES_BY_REGISTRY: WeakKeyDictionary[CollectorRegistry, _PoolGauges] = WeakKeyDictionary()
_GAUGES_LOCK = Lock()


def _gauges_for(registry: CollectorRegistry) -> _PoolGauges:
    with _GAUGES_LOCK:
        gauges = _GAUGES_BY_REGISTRY.get(registry)
        if gauges is None:
            gauges = _PoolGauges()
            registry.register(gauges)
            _GAUGES_BY_REGISTRY[registry] = gauges
        return gauges


class PrometheusPoolMetrics:
    """:class:`~loom.core.persistence.profile.PoolMetricsSink` publishing to Prometheus.

    Args:
        registry: Target registry; ``None`` is the global default registry.
        application: ``app.name`` of the process.
        entrypoint: Runtime profile name (``server``, ``worker``...).
    """

    def __init__(
        self, registry: CollectorRegistry | None, *, application: str, entrypoint: str
    ) -> None:
        self._registry = registry if registry is not None else REGISTRY
        self._application = application
        self._entrypoint = entrypoint
        instruments = cached_instruments(self._registry, _INSTRUMENTS)
        self._checkout_seconds = instruments[_CHECKOUT_SECONDS]
        self._checkout_timeouts = instruments[_CHECKOUT_TIMEOUTS]

    def register_engine(self, engine: Any, database: str) -> None:
        """Report the gauges of *engine*'s pool until the engine is collected."""
        _gauges_for(self._registry).add(engine, self._labels(database))

    def observe_checkout(self, database: str, seconds: float) -> None:
        """Record one checkout duration."""
        self._checkout_seconds.labels(*self._labels(database)).observe(seconds)

    def count_checkout_timeout(self, database: str) -> None:
        """Record one checkout that ran out of time."""
        self._checkout_timeouts.labels(*self._labels(database)).inc()

    def _labels(self, database: str) -> tuple[str, str, str]:
        return (self._application, self._entrypoint, database)


__all__ = ["PrometheusPoolMetrics"]
