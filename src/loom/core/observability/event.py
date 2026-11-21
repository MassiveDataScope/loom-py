"""Lifecycle event model for unified observability across Loom modules."""

from __future__ import annotations

from enum import StrEnum
from typing import Self, TypeVar

import msgspec
from msgspec.structs import force_setattr
from opentelemetry.util.types import AttributeValue

from loom.core.logger import NO_ATTRIBUTES, LogField, SpanAttributes
from loom.core.model import LoomFrozenStruct
from loom.core.vocabulary import LogKey, Scalar

S = TypeVar("S", bound=Scalar)


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
        failure: The exception that ended the work. Set on ``ERROR`` only,
            and only while the observers receive the event: the runtime drops
            it once dispatched, so an observer that keeps the event does not
            keep its traceback alive.
        attributes: Domain-specific fields under vocabulary keys, forwarded
            as they are by every observer backend.
    """

    scope: Scope
    name: str
    kind: EventKind
    trace_id: str | None = None
    correlation_id: str | None = None
    id: str | None = None
    duration_ms: float | None = None
    status: LifecycleStatus | None = None
    failure: BaseException | None = None
    attributes: SpanAttributes = msgspec.field(default_factory=lambda: NO_ATTRIBUTES)

    @classmethod
    def start(
        cls,
        *,
        scope: Scope,
        name: str,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        id: str | None = None,
        attributes: SpanAttributes = NO_ATTRIBUTES,
    ) -> Self:
        """Build a ``START`` lifecycle event."""
        return cls(
            scope=scope,
            name=name,
            kind=EventKind.START,
            trace_id=trace_id,
            correlation_id=correlation_id,
            id=id,
            attributes=attributes,
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
        attributes: SpanAttributes = NO_ATTRIBUTES,
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
            attributes=attributes,
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
        failure: BaseException | None = None,
        status: LifecycleStatus = LifecycleStatus.FAILURE,
        attributes: SpanAttributes = NO_ATTRIBUTES,
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
            attributes=attributes,
        )

    def attribute(self, key: LogKey, kind: type[S]) -> S | None:
        """The attribute under *key* when it holds a *kind*, else ``None``."""
        value = self.attributes.get(key)
        return value if isinstance(value, kind) else None

    def drop_failure(self) -> None:
        """Forget the exception once every observer has seen it, so its traceback is freed."""
        if self.failure is not None:
            force_setattr(self, "failure", None)

    def otel_span_name(self) -> str:
        """Return the canonical OTEL span name for this event."""
        return f"{self.scope.value}:{self.name}"

    def otel_attributes(self) -> dict[str, AttributeValue]:
        """Return OTEL span attributes derived from this event, under vocabulary keys.

        The attributes are scalars, which OTEL stores as they are, so a backend
        can filter on them (``loom.batch_size > 100``).
        """
        attrs: dict[str, AttributeValue] = {
            LogField.SCOPE: self.scope.value,
            LogField.NAME: self.name,
        }
        if self.trace_id:
            attrs[LogField.TRACE_ID] = self.trace_id
        if self.correlation_id:
            attrs[LogField.CORRELATION_ID] = self.correlation_id
        if self.id:
            attrs[LogField.EVENT_ID] = self.id
        attrs.update(self.attributes.items())
        return attrs


__all__ = [
    "EventKind",
    "LifecycleEvent",
    "LifecycleStatus",
    "Scope",
    "TerminalReason",
]
