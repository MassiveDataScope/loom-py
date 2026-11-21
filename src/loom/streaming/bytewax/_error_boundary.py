"""Error boundary helpers for Bytewax node execution."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol, TypeAlias, TypeVar

from bytewax.operators import branch, flat_map

from loom.core.errors import DomainError
from loom.core.errors.boundary import BoundaryClass, boundary
from loom.core.errors.failure import Failure
from loom.core.observability.event import TerminalReason
from loom.core.observability.observability import Observability
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, snapshot_message
from loom.streaming.core._message import Message
from loom.streaming.core._tracing import open_terminal_span
from loom.streaming.core._typing import StreamPayload
from loom.streaming.log_events import StreamingLogField

Stream: TypeAlias = Any
NodeResult: TypeAlias = Message[StreamPayload] | ErrorEnvelope[StreamPayload]
ResultT = TypeVar("ResultT")
logger = logging.getLogger(__name__)


class BytewaxBoundary(BoundaryClass):
    """Base of every bytewax source, sink and partition loom writes.

    Bytewax calls their methods from its own worker loop and prints what they
    raise, so each method it calls goes through the loom boundary.
    """

    BOUNDARY_METHODS: ClassVar[frozenset[str]] = frozenset(
        {
            "build",
            "build_part",
            "list_parts",
            "next_batch",
            "next_awake",
            "snapshot",
            "write_batch",
            "close",
        }
    )


class _ErrorSplitContext(Protocol):
    """Context carrying error-output wiring for node boundaries."""

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one error branch to an output sink."""


def _classify_task(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a task error."""
    if isinstance(exc, DomainError):
        return ErrorKind.BUSINESS
    return ErrorKind.TASK


def _classify_routing(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a routing error."""
    del exc
    return ErrorKind.ROUTING


@dataclass(frozen=True, slots=True)
class ErrorBoundary:
    """Where an error envelope is built, and who is told about it.

    Args:
        observer: Runtime the terminal span of the failing message is opened on.
        flow: Name of the enclosing flow, stamped on that span.
    """

    observer: Observability
    flow: str


def _build_error_envelope(
    kind: ErrorKind,
    failure: BaseException,
    original: Message[StreamPayload],
    error_boundary: ErrorBoundary,
) -> ErrorEnvelope[StreamPayload]:
    """Wrap a failed message as an error envelope and close its trace.

    The envelope leaves the process, to an error topic or a dead-letter
    queue: its reason is the text ``Failure`` allows: a ``LoomError``'s message, any other
    exception's type.

    This is where the *original* message dies: what continues downstream is the
    envelope, whose onward journey is the envelope's own life, correlated by
    the trace id it inherited. Modelling it as the original continuing would
    claim a message survived its own failure.
    """
    open_terminal_span(
        error_boundary.observer,
        original.meta,
        TerminalReason.ERROR_ENVELOPE,
        attributes={
            StreamingLogField.FLOW: error_boundary.flow,
            StreamingLogField.ERROR_KIND: kind.value,
        },
    ).end()
    snapshot = snapshot_message(original)
    payload_type = original.payload.__class__.loom_message_type()
    return ErrorEnvelope(
        kind=kind,
        reason=Failure(failure).text,
        payload_type=payload_type,
        original_message=snapshot,
    )


def _execute_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    original: Message[StreamPayload],
    fn: Callable[[], ResultT],
    error_boundary: ErrorBoundary,
) -> ResultT | ErrorEnvelope[StreamPayload]:
    """Execute one message node and capture failures as error envelopes."""
    try:
        return fn()
    except Exception as exc:
        kind = classify(exc)
        _log_boundary_error(kind, exc, original)
        return _build_error_envelope(kind, exc, original, error_boundary)


def _execute_batch_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    originals: Sequence[Message[StreamPayload]],
    fn: Callable[[], Sequence[NodeResult]],
    error_boundary: ErrorBoundary,
) -> list[NodeResult]:
    """Execute one batch node and capture failures as per-message envelopes."""
    try:
        return list(fn())
    except Exception as exc:
        kind = classify(exc)
        if originals:
            _log_boundary_error(kind, exc, originals[0], batch_size=len(originals))
        return [_build_error_envelope(kind, exc, message, error_boundary) for message in originals]


def _split_node_result(
    stream: Stream,
    step_id: str,
    ctx: _ErrorSplitContext,
    kind: ErrorKind,
) -> Stream:
    """Split node results into success and error branches and wire errors."""
    split = branch(f"{step_id}_split", stream, boundary(_is_message))
    ctx.wire_node_error(kind, step_id, split.falses)
    return split.trues


def _split_batch_node_result(
    stream: Stream,
    step_id: str,
    ctx: _ErrorSplitContext,
    kind: ErrorKind,
) -> Stream:
    """Split batch-shaped node results into success and error branches."""
    split = branch(f"{step_id}_split", stream, boundary(_is_message_batch))
    errors = flat_map(f"{step_id}_errors", split.falses, boundary(_identity))
    ctx.wire_node_error(kind, step_id, errors)
    return split.trues


def _is_message(item: Any) -> bool:
    """Return whether one item is a normal message result."""
    return isinstance(item, Message)


def _is_message_batch(item: Any) -> bool:
    """Return whether one batch-shaped result contains normal messages."""
    if not isinstance(item, list):
        return False
    if not item:
        return True
    return isinstance(item[0], Message)


def _identity(items: Any) -> Any:
    """Pass through an item unchanged for flat_map."""
    return items


def _log_boundary_error(
    kind: ErrorKind,
    exc: Exception,
    original: Message[StreamPayload],
    batch_size: int = 1,
) -> None:
    payload_type = original.payload.__class__.loom_message_type()
    context = {
        "kind": kind.value,
        "payload_type": payload_type,
        "message_id": original.meta.message_id,
        "topic": original.meta.topic,
        "partition": original.meta.partition,
        "offset": original.meta.offset,
        "batch_size": batch_size,
    }
    if isinstance(exc, DomainError):
        logger.warning("managed_boundary_error", exc_info=exc, extra=context)
        return
    logger.error("unhandled_boundary_error", exc_info=exc, extra=context)
