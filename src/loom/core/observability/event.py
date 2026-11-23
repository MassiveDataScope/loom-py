"""Lifecycle event model for unified observability across Loom modules."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Self

import msgspec
from opentelemetry.util.types import AttributeValue

from loom.core.errors.base import ErrorSummary
from loom.core.model import LoomFrozenStruct


def _as_attribute(value: object) -> AttributeValue:
    """Coerce one meta value to something OTEL can store."""
    if isinstance(value, bool | int | float | str):
        return value
    return str(value)


class EventKind(StrEnum):
    """Lifecycle phase emitted by an ``Observability`` span."""

    START = "start"
    END = "end"
    ERROR = "error"


class LifecycleStatus(StrEnum):
    """Outcome label attached to closing lifecycle events."""

    SUCCESS = "success"
    FAILURE = "failure"


class Scope(StrEnum):
    """Recognised unit-of-work scopes for all Loom observers.

    Using a closed enum keeps scope names consistent across ETL, streaming,
    REST, and Kafka transports, and makes parent-span relationships in OTEL
    unambiguous.
    """

    USE_CASE = "use_case"
    JOB = "job"

    PIPELINE = "pipeline"
    PROCESS = "process"
    STEP = "step"

    FLOW = "flow"
    NODE = "node"
    POLL_CYCLE = "poll_cycle"
    BATCH_COLLECT = "batch_collect"

    AGENT = "agent"
    TOOL = "tool"

    TRANSPORT = "transport"
    READ = "read"
    WRITE = "write"
    DELETE = "delete"

    MAINTENANCE = "maintenance"

    TERMINAL = "terminal"


class TerminalReason(StrEnum):
    """Why a streaming message stopped existing.

    A message is traceable from ingestion, through every node, to its death.
    A message written to a sink, turned into an error envelope, or expanded
    to zero rows ends its life; each value names one of those endings and
    becomes the ``name`` of a :attr:`Scope.TERMINAL` span, so the last span of
    a trace says how the message ended.

    The set is closed on purpose: a union that governs behaviour has to be
    closed and tagged, and "some other ending" is not an ending anyone can act
    on.
    """

    SINK_WRITE = "sink_write"
    """Written to a storage sink or an outbound topic."""

    ERROR_ENVELOPE = "error_envelope"
    """Converted to an error envelope and routed to an error sink or DLQ."""

    DROPPED_NO_ROUTE = "dropped_no_route"
    """Expanded or routed to zero rows, so nothing downstream ever sees it."""


class LifecycleEvent(LoomFrozenStruct, frozen=True, kw_only=True):
    """Immutable lifecycle event emitted to all registered observers.

    All fields except ``scope``, ``name``, and ``kind`` are optional — observers
    must tolerate ``None`` for any contextual field.

    Args:
        scope: Logical unit of work — one of the values in :class:`Scope`.
        name: Human-readable label for the operation within the scope.
        kind: Lifecycle phase — ``START``, ``END``, or ``ERROR``.
        trace_id: Trace identifier for the current unit of work. Generated
            once per root span and propagated to all child spans.
        correlation_id: Business lineage identifier (saga, request, batch
            group). Always propagated from the upstream context.
        id: Optional stable identifier for the specific operation instance.
        duration_ms: Wall-clock duration in milliseconds. Set on ``END``
            and ``ERROR`` events only.
        status: Short outcome label. Set on ``END`` and exposed as a stable enum.
        failure: The failure's type and code, never its message. Set on ``ERROR`` only.
        meta: Domain-specific fields forwarded as top-level keys by all
            observer backends. Adapters use this to carry rich context
            (``flow``, ``node_idx``, ``run_id``, …) without coupling
            ``LifecycleEvent`` to domain types.
    """

    scope: Scope
    name: str
    kind: EventKind
    trace_id: str | None = None
    correlation_id: str | None = None
    id: str | None = None
    duration_ms: float | None = None
    status: LifecycleStatus | None = None
    failure: ErrorSummary | None = None
    meta: dict[str, object] = msgspec.field(default_factory=dict)

    @classmethod
    def start(
        cls,
        *,
        scope: Scope,
        name: str,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        meta: Mapping[str, object] | None = None,
    ) -> Self:
        """Build a ``START`` lifecycle event."""
        return cls(
            scope=scope,
            name=name,
            kind=EventKind.START,
            trace_id=trace_id,
            correlation_id=correlation_id,
            id=id,
            meta=dict(meta or {}),
        )

    @classmethod
    def end(
        cls,
        *,
        scope: Scope,
        name: str,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        duration_ms: float | None = None,
        status: LifecycleStatus = LifecycleStatus.SUCCESS,
        meta: Mapping[str, object] | None = None,
    ) -> Self:
        """Build an ``END`` lifecycle event."""
        return cls(
            scope=scope,
            name=name,
            kind=EventKind.END,
            trace_id=trace_id,
            correlation_id=correlation_id,
            id=id,
            duration_ms=duration_ms,
            status=status,
            meta=dict(meta or {}),
        )

    @classmethod
    def exception(
        cls,
        *,
        scope: Scope,
        name: str,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        duration_ms: float | None = None,
        failure: ErrorSummary | None = None,
        status: LifecycleStatus = LifecycleStatus.FAILURE,
        meta: Mapping[str, object] | None = None,
    ) -> Self:
        """Build an ``ERROR`` lifecycle event."""
        return cls(
            scope=scope,
            name=name,
            kind=EventKind.ERROR,
            trace_id=trace_id,
            correlation_id=correlation_id,
            id=id,
            duration_ms=duration_ms,
            status=status,
            failure=failure,
            meta=dict(meta or {}),
        )

    def otel_span_name(self) -> str:
        """Return the canonical OTEL span name for this event."""
        return f"{self.scope.value}:{self.name}"

    def otel_attributes(self) -> dict[str, AttributeValue]:
        """Return OTEL span attributes derived from this event.

        Meta values that OTEL already accepts — ``bool``, ``int``, ``float``,
        ``str`` — are passed through unchanged so a backend can filter on them
        (``loom.batch_size > 100``, ``loom.links_truncated = true``). Anything
        else is stringified, because an attribute of an unsupported type is
        dropped by the SDK with a warning rather than exported.
        """
        attrs: dict[str, AttributeValue] = {"scope": self.scope.value, "name": self.name}
        if self.trace_id:
            attrs["trace_id"] = self.trace_id
        if self.correlation_id:
            attrs["correlation_id"] = self.correlation_id
        if self.id:
            attrs["id"] = self.id
        attrs.update({key: _as_attribute(value) for key, value in self.meta.items()})
        return attrs


__all__ = [
    "EventKind",
    "LifecycleEvent",
    "LifecycleStatus",
    "Scope",
    "TerminalReason",
]
