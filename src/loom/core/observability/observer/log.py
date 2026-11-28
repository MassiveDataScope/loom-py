"""Lifecycle observer that writes every event as a log record."""

from __future__ import annotations

import logging
from typing import Final

from loom.core.log_names import LogField, LogKey, ValueKind
from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.observability.event import EventKind, LifecycleEvent, Scope

_log = logging.getLogger(__name__)


class LifecycleKey(LogKey):
    """The fields a lifecycle record writes beside the shared ones."""

    EVENT_ID = "loom.lifecycle.event_id"
    META = "loom.lifecycle.meta", ValueKind.OBJECT


_EVENTS: Final = {
    EventKind.START: ObservabilityEvent.LIFECYCLE_STARTED,
    EventKind.END: ObservabilityEvent.LIFECYCLE_ENDED,
    EventKind.ERROR: ObservabilityEvent.LIFECYCLE_FAILED,
}


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
        }
        if event.id is not None:
            fields[LifecycleKey.EVENT_ID] = event.id
        if event.kind is not EventKind.START:
            fields[LogField.DURATION_MS] = event.duration_ms
        if event.kind is EventKind.END and event.status is not None:
            fields[LogField.STATUS] = event.status.value
        if event.failure is not None:
            fields |= event.failure.fields()
        if event.trace_id is not None:
            fields[LogField.TRACE_ID] = event.trace_id
        fields |= {LifecycleKey.META.of(key): value for key, value in event.meta.items()}
        _log.log(level, _EVENTS[event.kind], extra=fields)


def _level_of(event: LifecycleEvent) -> int:
    if event.kind is EventKind.ERROR:
        return logging.WARNING if event.scope is Scope.TRANSPORT else logging.ERROR
    if event.scope is Scope.TRANSPORT:
        return logging.DEBUG
    if event.kind is EventKind.END or event.scope is Scope.PIPELINE:
        return logging.INFO
    return logging.DEBUG


__all__ = ["LogLifecycleObserver"]
