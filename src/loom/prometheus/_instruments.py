"""Declarative construction and process-wide caching of Prometheus instruments.

Internal module.  Each adapter in :mod:`loom.prometheus` declares a table of
:class:`InstrumentSpec` values and asks :func:`cached_instruments` for the
matching instruments::

    _REQUESTS = counter_spec("loom_usecase_requests", "Executions by outcome.", "usecase")
    _SPECS = (_REQUESTS,)

    instruments = cached_instruments(registry, _SPECS)
    requests_total = instruments[_REQUESTS]  # typed as Counter
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast
from weakref import WeakKeyDictionary

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram
from prometheus_client.metrics import MetricWrapperBase

from loom.core.errors import InvalidValue

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_InstrumentT = TypeVar("_InstrumentT", bound="MetricWrapperBase")


class InstrumentKind(Enum):
    """Prometheus instrument type a spec is built as."""

    COUNTER = "counter"
    HISTOGRAM = "histogram"


@dataclass(frozen=True)
class InstrumentSpec(Generic[_InstrumentT]):
    """Immutable description of a single Prometheus instrument.

    Doubles as the lookup key of the instrument built from it in
    :class:`InstrumentSet`.

    Args:
        kind: Instrument type to build.
        name: Metric name as exported to Prometheus.
        documentation: Metric help string.
        labelnames: Label names, in declaration order.
    """

    kind: InstrumentKind
    name: str
    documentation: str
    labelnames: tuple[str, ...]


def counter_spec(name: str, documentation: str, *labelnames: str) -> InstrumentSpec[Counter]:
    """Declare a Counter instrument.

    Args:
        name: Metric name as exported to Prometheus.
        documentation: Metric help string.
        *labelnames: Label names, in declaration order.

    Returns:
        Spec that yields a ``Counter``.
    """
    return InstrumentSpec(InstrumentKind.COUNTER, name, documentation, labelnames)


def histogram_spec(name: str, documentation: str, *labelnames: str) -> InstrumentSpec[Histogram]:
    """Declare a Histogram instrument.

    Args:
        name: Metric name as exported to Prometheus.
        documentation: Metric help string.
        *labelnames: Label names, in declaration order.

    Returns:
        Spec that yields a ``Histogram``.
    """
    return InstrumentSpec(InstrumentKind.HISTOGRAM, name, documentation, labelnames)


class InstrumentSet:
    """Instruments built from a spec table, addressable by their spec.

    Args:
        instruments: Instrument per spec, as built for one registry.
    """

    def __init__(self, instruments: Mapping[InstrumentSpec[Any], MetricWrapperBase]) -> None:
        self._instruments = instruments

    def __getitem__(self, spec: InstrumentSpec[_InstrumentT]) -> _InstrumentT:
        """Return the instrument built from *spec*.

        Args:
            spec: Spec that is part of the table this set was built from.

        Returns:
            The instrument, typed as the spec declares it.

        Raises:
            KeyError: If *spec* is not part of the table.
        """
        return cast("_InstrumentT", self._instruments[spec])


_CACHE_BY_REGISTRY: WeakKeyDictionary[CollectorRegistry, dict[str, MetricWrapperBase]] = (
    WeakKeyDictionary()
)
_CACHE_LOCK = Lock()


def cached_instruments(
    registry: CollectorRegistry | None,
    specs: Sequence[InstrumentSpec[Any]],
) -> InstrumentSet:
    """Return the instruments for *specs* bound to *registry*.

    Every registry holds at most one instrument per metric name, built on first
    request and reused afterwards, so several adapters sharing a registry share
    its instruments instead of colliding on duplicate collectors.  Passing the
    global default registry explicitly is the same registry as passing ``None``.
    Registries keep separate instruments from each other, and a
    garbage-collected registry releases its own.  The first spec to claim a name
    fixes its documentation.

    Args:
        registry: Target ``CollectorRegistry``, or ``None`` for the global
            default registry.
        specs: Instrument table to build.

    Returns:
        Set of instruments, addressable by the specs in *specs*.

    Raises:
        ValueError: If a spec asks for a metric name the registry already holds
            as a different instrument kind or with different label names.
    """
    target = registry if registry is not None else REGISTRY
    with _CACHE_LOCK:
        cache = _CACHE_BY_REGISTRY.setdefault(target, {})
        return InstrumentSet({spec: _resolve(cache, target, spec) for spec in specs})


def _resolve(
    cache: dict[str, MetricWrapperBase],
    registry: CollectorRegistry,
    spec: InstrumentSpec[Any],
) -> MetricWrapperBase:
    """Return the cached instrument for *spec*, building it on first request.

    Args:
        cache: Instrument-by-name cache owned by *registry*.
        registry: Target ``CollectorRegistry``.
        spec: Instrument to resolve.

    Returns:
        The instrument registered under ``spec.name``.

    Raises:
        ValueError: If the cached instrument under ``spec.name`` was built for a
            different instrument kind or with different label names.
    """
    cached = cache.get(spec.name)
    if cached is None:
        cached = _build_instrument(registry, spec)
        cache[spec.name] = cached
        return cached
    _reject_mismatch(cached, spec)
    return cached


def _reject_mismatch(cached: MetricWrapperBase, spec: InstrumentSpec[Any]) -> None:
    """Check that *cached* is the instrument *spec* asks for.

    Args:
        cached: Instrument already registered under ``spec.name``.
        spec: Instrument being resolved.

    Raises:
        ValueError: If kind or label names differ from the cached instrument.
    """
    labelnames = tuple(cached._labelnames)  # noqa: SLF001
    if isinstance(cached, _factory(spec.kind)) and labelnames == spec.labelnames:
        return
    message = (
        f"Metric {spec.name!r} is already registered as {type(cached).__name__} "
        f"with labels {labelnames}, cannot build it as {spec.kind.value} "
        f"with labels {spec.labelnames}."
    )
    raise InvalidValue(message)


def _build_instrument(
    registry: CollectorRegistry,
    spec: InstrumentSpec[Any],
) -> MetricWrapperBase:
    """Build the instrument described by *spec* on *registry*.

    Args:
        registry: Target ``CollectorRegistry``.
        spec: Instrument to build.

    Returns:
        The freshly registered instrument.
    """
    return _factory(spec.kind)(
        spec.name,
        spec.documentation,
        spec.labelnames,
        registry=registry,
    )


def _factory(kind: InstrumentKind) -> type[MetricWrapperBase]:
    """Return the prometheus_client class that builds instruments of *kind*.

    Args:
        kind: Instrument type to build.

    Returns:
        The instrument class.
    """
    factories: dict[InstrumentKind, type[MetricWrapperBase]] = {
        InstrumentKind.COUNTER: Counter,
        InstrumentKind.HISTOGRAM: Histogram,
    }
    return factories[kind]
