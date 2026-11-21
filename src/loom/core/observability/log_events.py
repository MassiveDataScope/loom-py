"""The log events of observability."""

from __future__ import annotations

from loom.core.logger import LogEvent


class ObservabilityLogEvent(LogEvent, prefix="observability"):
    """The records :class:`~loom.core.observability.observer.log.LogLifecycleObserver` writes."""

    STARTED = "observability.lifecycle_started"
    ENDED = "observability.lifecycle_ended"
    FAILED = "observability.lifecycle_failed"
