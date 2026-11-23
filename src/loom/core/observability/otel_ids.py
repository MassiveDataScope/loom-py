"""Make the Loom message trace id the OTEL trace id, and build the sampler.

A streaming message must be traceable from ingestion, through every node, to
its death, under one trace id, continuous across services. The trace id that
satisfies that already exists: it arrives in a Kafka header, rides
``MessageMeta.trace_id`` through every node, survives error snapshots, and
crosses the Celery broker as a task kwarg.

There is no Tracer API for *"start a span in trace X"*, and fabricating a
parent ``SpanContext`` where no upstream span exists invents a parent edge to a
span that was never exported. The correct hook is the SDK's ``IdGenerator``:
``generate_trace_id`` is consulted **only for root spans**, so a span with a
real parent still inherits its parent's trace and only genuine roots adopt the
message id.

The whole module imports the OpenTelemetry **SDK**, which is extras-only, so it
must only ever be imported lazily from inside a function — see
:mod:`loom.core.observability.observer.otel`.

Ordering trap: ``TracerProvider.get_tracer`` snapshots ``id_generator`` into
the ``Tracer``. Assigning it afterwards is silently ignored. Install the
generator **before** acquiring any tracer. This is pinned by
``tests/contract/observability/test_otel_sdk_contract.py``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.id_generator import IdGenerator, RandomIdGenerator
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)
from opentelemetry.trace import INVALID_TRACE_ID

from loom.core.config.observability import OtelConfig
from loom.core.observability.diagnostic import ObservabilityEvent, ObservabilityKey
from loom.core.tracing.context import get_trace_id

_TRACE_ID_HEX_LEN = 32
_log = logging.getLogger(__name__)


def parse_otel_trace_id(value: str | None) -> int | None:
    """Convert a Loom trace id to an OTEL trace id, or ``None`` when unusable.

    A Loom trace id is a 32-character lowercase hex string — the same shape and
    the same uniform randomness OTEL requires — so the conversion is exact.
    Anything else (a shorter opaque id set by a foreign producer, a
    non-hexadecimal value, the all-zero id OTEL reserves as invalid) is
    rejected so the caller falls back to a random id rather than emitting an
    unusable trace.

    Args:
        value: Candidate Loom trace id.

    Returns:
        The integer OTEL trace id, or ``None`` when *value* cannot be one.

    Example::

        parse_otel_trace_id("4b3f9a1c2d8e0f7b6a5c3e1d9f2b4a0c")  # -> int
        parse_otel_trace_id("job-42")                            # -> None
    """
    if value is None or len(value) != _TRACE_ID_HEX_LEN:
        return None
    try:
        parsed = int(value, 16)
    except ValueError:
        return None
    if parsed == INVALID_TRACE_ID:
        return None
    return parsed


class LoomMessageIdGenerator(IdGenerator):
    """Id generator that gives root spans the active Loom trace id.

    ``generate_trace_id`` returns the trace id set in
    :mod:`loom.core.tracing.context` whenever one is active, and delegates
    otherwise. Span ids are always delegated: only the trace id is shared.

    Because the SDK consults this only for root spans, a span opened inside
    another span still inherits its parent's trace. And because it delegates
    whenever no Loom trace id is active, installing it on a host-owned provider
    leaves host spans on independent random trace ids.

    Args:
        delegate: Generator used when no Loom trace id is active, and for every
            span id. Defaults to the SDK's random generator.

    Example::

        provider = TracerProvider(id_generator=LoomMessageIdGenerator())
        tracer = provider.get_tracer("loom")  # after installing it, never before
    """

    def __init__(self, delegate: IdGenerator | None = None) -> None:
        self._delegate = delegate if delegate is not None else RandomIdGenerator()

    @property
    def delegate(self) -> IdGenerator:
        """Return the generator used when no Loom trace id is active."""
        return self._delegate

    def generate_span_id(self) -> int:
        """Return a fresh random span id.

        Span ids are never derived from the message: two spans of one message
        must stay distinguishable.
        """
        return self._delegate.generate_span_id()

    def generate_trace_id(self) -> int:
        """Return the active Loom trace id, or a random one when none is set."""
        parsed = parse_otel_trace_id(get_trace_id())
        if parsed is None:
            return self._delegate.generate_trace_id()
        return parsed


def adopt_host_id_generator(provider: object) -> bool:
    """Install :class:`LoomMessageIdGenerator` on a host-owned tracer provider.

    Opt-in and explicit: silently mutating another library's provider would be
    a hidden side effect, even though it is behaviour-preserving — host spans
    keep independent random trace ids because the generator delegates whenever
    no Loom trace id is active.

    When the installed provider is a ``ProxyTracerProvider`` — or anything else
    without an ``id_generator`` — there is nothing to install onto: assigning
    the attribute would create a field nobody reads. That is reported as a
    warning and ignored. Telemetry never interrupts execution.

    Args:
        provider: Provider currently installed in the process.

    Returns:
        ``True`` when the generator was installed, ``False`` when it could not
        be honoured.
    """
    if not isinstance(provider, TracerProvider):
        _log.warning(
            ObservabilityEvent.HOST_ID_GENERATOR_UNAVAILABLE,
            extra={ObservabilityKey.PROVIDER: type(provider).__name__},
        )
        return False
    provider.id_generator = LoomMessageIdGenerator(provider.id_generator)  # type: ignore[assignment]
    return True


def build_sampler(config: OtelConfig) -> Sampler:
    """Build the sampler for Loom's own tracer provider.

    The ratio samplers decide on the low bits of the trace id, and the trace id
    is the message's — stable from the Kafka header through every node to the
    terminal span and across services. The decision is therefore identical at
    every hop: complete traces for a sampled subset, never partial traces for
    all.

    Args:
        config: Validated OTEL configuration.

    Returns:
        The sampler named by ``config.sampler``.
    """
    builders: dict[str, Callable[[float], Sampler]] = {
        "always_on": lambda _: ALWAYS_ON,
        "always_off": lambda _: ALWAYS_OFF,
        "traceidratio": TraceIdRatioBased,
        "parentbased_always_on": lambda _: ParentBased(ALWAYS_ON),
        "parentbased_always_off": lambda _: ParentBased(ALWAYS_OFF),
        "parentbased_traceidratio": lambda ratio: ParentBased(TraceIdRatioBased(ratio)),
    }
    return builders[config.sampler](config.sampler_ratio)


__all__ = [
    "LoomMessageIdGenerator",
    "adopt_host_id_generator",
    "build_sampler",
    "parse_otel_trace_id",
]
