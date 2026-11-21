"""Lifecycle observer that writes every event as a log record."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from loom.core.logger import LogField
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.log_events import ObservabilityLogEvent

_log = logging.getLogger(__name__)


_EVENTS = {
    EventKind.START: ObservabilityLogEvent.STARTED,
    EventKind.END: ObservabilityLogEvent.ENDED,
    EventKind.ERROR: ObservabilityLogEvent.FAILED,
}
_META_PREFIX = "loom.meta."

_MESSAGE_LINEAGE: Final = (
    "loom.message_id",
    "loom.parent_trace_id",
    "loom.causation_id",
    "loom.message_type",
    "messaging.source.name",
    "messaging.kafka.partition",
    "messaging.kafka.offset",
)
_BATCH_LINEAGE: Final = ("loom.batch_id", "loom.batch_size", "loom.links_truncated")
_TERMINAL_FACTS: Final = ("terminal.reason", "terminal.failure_scope", "terminal.dlq_topic")
_AGENT_USAGE: Final = tuple(
    f"gen_ai.usage.{name}"
    for name in (
        "input_tokens",
        "output_tokens",
        "cache_read.input_tokens",
        "cache_creation.input_tokens",
        "requests",
        "tool_calls",
        "cost_known",
        "cost",
    )
)


LIFECYCLE_META: Final[Mapping[Scope, frozenset[str]]] = MappingProxyType(
    {
        Scope.USE_CASE: frozenset({"route", "method", "status_code", "read_only", "mechanism"}),
        Scope.JOB: frozenset({"payload_size", "callback"}),
        Scope.PIPELINE: frozenset({"run_id", "attempt", "last_attempt", "nodes"}),
        Scope.PROCESS: frozenset({"run_id", "process_run_id", "steps"}),
        Scope.STEP: frozenset(
            {"run_id", "process_run_id", "step_run_id", "sources", "target", "streaming"}
        ),
        Scope.FLOW: frozenset({"node_count"}),
        Scope.NODE: frozenset(
            {"flow", "node_idx", "node_type", *_MESSAGE_LINEAGE, *_BATCH_LINEAGE}
        ),
        Scope.POLL_CYCLE: frozenset({"node_count"}),
        Scope.BATCH_COLLECT: frozenset(),
        Scope.AGENT: frozenset(
            {
                "agent",
                "route",
                "method",
                "status_code",
                "mechanism",
                "interaction_id",
                *_AGENT_USAGE,
            }
        ),
        Scope.TOOL: frozenset({"agent", "capability", "mcp_server"}),
        Scope.TRANSPORT: frozenset({"content_type", "topic", "queue", "countdown", "priority"}),
        Scope.READ: frozenset({"agent", "connection", "mechanism"}),
        Scope.WRITE: frozenset({"flow", "sink", "batch_size", "loom.flow_run_id", *_BATCH_LINEAGE}),
        Scope.DELETE: frozenset(),
        Scope.MAINTENANCE: frozenset(),
        Scope.TERMINAL: frozenset(
            {
                "flow",
                "sink",
                "loom.flow_run_id",
                *_TERMINAL_FACTS,
                *_MESSAGE_LINEAGE,
                *_BATCH_LINEAGE,
            }
        ),
    }
)
"""The ``meta`` keys a lifecycle event of each scope may write; any other key is dropped.

A failed event also carries ``error_type``. Neither the subject nor the roles of the
caller are allowed: the user is written by security events, under ``user_identity``.
"""

_ON_FAILURE: Final = frozenset({"error_type"})


class LogLifecycleObserver:
    """Lifecycle observer that emits every event as a log record.

    Levels by scope and kind:
    - ``TRANSPORT`` (produce/consume/encode/decode): ``debug`` for END, ``warning`` for ERROR.
      Transport events are high-frequency and must not pollute INFO logs.
    - ``START``: ``debug``, except a ``PIPELINE`` start, which is ``info``.
    - ``END``: ``info`` — standard operational visibility.
    - ``ERROR``: ``error`` — always visible.

    It does not configure logging; the process does, through
    ``observability.log``.
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Write one lifecycle event as a log record.

        Args:
            event: Lifecycle event to log.
        """
        level = _level_of(event)
        if not _log.isEnabledFor(level):
            return
        fields: dict[str, object] = {
            LogField.SCOPE: event.scope.value,
            LogField.NAME: event.name,
            LogField.CORRELATION_ID: event.correlation_id,
            LogField.EVENT_ID: event.id,
        }
        if event.trace_id is not None:
            fields[LogField.TRACE_ID] = event.trace_id
        if event.kind is not EventKind.START:
            fields[LogField.DURATION_MS] = event.duration_ms
        if event.kind is EventKind.END and event.status is not None:
            fields[LogField.STATUS] = event.status.value
        if event.kind is EventKind.ERROR:
            fields[LogField.ERROR_MESSAGE] = event.error
        allowed = _allowed_meta(event)
        fields |= {
            f"{_META_PREFIX}{key}": value for key, value in event.meta.items() if key in allowed
        }
        _log.log(level, _EVENTS[event.kind], extra=fields)


def _allowed_meta(event: LifecycleEvent) -> frozenset[str]:
    keys = LIFECYCLE_META[event.scope]
    return keys | _ON_FAILURE if event.kind is EventKind.ERROR else keys


def _level_of(event: LifecycleEvent) -> int:
    if event.kind is EventKind.ERROR:
        return logging.WARNING if event.scope is Scope.TRANSPORT else logging.ERROR
    if event.scope is Scope.TRANSPORT:
        return logging.DEBUG
    if event.kind is EventKind.END or event.scope is Scope.PIPELINE:
        return logging.INFO
    return logging.DEBUG


__all__ = ["LogLifecycleObserver"]
