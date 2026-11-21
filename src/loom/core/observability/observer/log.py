"""Lifecycle observer that writes every scope's start and end as a log record."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Final, cast

from loom.core.logger import LogField, Scalar
from loom.core.logger.record import failure_level
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.log_events import ObservabilityLogEvent

_log = logging.getLogger(__name__)

_STATUS: Final = {
    EventKind.END: LifecycleStatus.SUCCESS.value,
    EventKind.ERROR: LifecycleStatus.FAILURE.value,
}


class LogLifecycleObserver:
    """The owner of the scope facts: one record when a scope starts and one when it ends.

    ``observability.lifecycle_started`` and ``observability.lifecycle_ended``
    carry ``loom.scope``, ``loom.name`` and the span's attributes; an end also
    ``loom.status`` and ``loom.duration_ms``. A failed end carries its exception,
    which the record factory writes as ``exception.type`` and ``loom.error.code``,
    and as ``exception.stacktrace`` the first time it is reported at ERROR.

    Levels:

    - a start: ``DEBUG``, except a ``PIPELINE`` start, which is ``INFO``;
    - a successful end: ``INFO``;
    - a failed end: ``INFO`` when the exception is expected, ``ERROR`` the first
      time an unexpected one is reported (the innermost scope, which writes the
      stack), ``WARNING`` in the outer scopes it crosses after;
    - a ``TRANSPORT`` scope, which is high-frequency: ``DEBUG``, and at most
      ``WARNING`` for a failure.

    It does not configure logging; the process does, through ``observability.log``.
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Write one lifecycle event as a log record.

        Args:
            event: Lifecycle event to log.
        """
        level = self._level_of(event)
        if not _log.isEnabledFor(level):
            return
        attributes = cast(Mapping[str, Scalar], event.attributes)
        if event.kind is EventKind.START:
            _log.log(
                level,
                ObservabilityLogEvent.LIFECYCLE_STARTED,
                extra={
                    LogField.SCOPE: event.scope.value,
                    LogField.NAME: event.name,
                    LogField.TRACE_ID: event.trace_id,
                    LogField.CORRELATION_ID: event.correlation_id,
                    LogField.EVENT_ID: event.id,
                    **attributes,
                },
            )
            return
        _log.log(
            level,
            ObservabilityLogEvent.LIFECYCLE_ENDED,
            exc_info=event.failure,
            extra={
                LogField.SCOPE: event.scope.value,
                LogField.NAME: event.name,
                LogField.TRACE_ID: event.trace_id,
                LogField.CORRELATION_ID: event.correlation_id,
                LogField.EVENT_ID: event.id,
                LogField.STATUS: _STATUS[event.kind],
                LogField.DURATION_MS: event.duration_ms,
                **attributes,
            },
        )

    @staticmethod
    def _level_of(event: LifecycleEvent) -> int:
        """The level of the record that writes *event*."""
        if event.kind is EventKind.ERROR:
            level = logging.ERROR if event.failure is None else failure_level(event.failure)
            return min(level, logging.WARNING) if event.scope is Scope.TRANSPORT else level
        if event.scope is Scope.TRANSPORT:
            return logging.DEBUG
        if event.kind is EventKind.END or event.scope is Scope.PIPELINE:
            return logging.INFO
        return logging.DEBUG


__all__ = ["LogLifecycleObserver"]
