"""The records observability logs: the lifecycle it writes and what it logs about itself."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class ObservabilityEvent(LogEvent):
    """What the observability runtime logs: each lifecycle event, its observers and providers."""

    LIFECYCLE_STARTED = "loom.observability.lifecycle_started"
    LIFECYCLE_ENDED = "loom.observability.lifecycle_ended"
    LIFECYCLE_FAILED = "loom.observability.lifecycle_failed"
    OBSERVER_FAILED = "loom.observability.observer_failed"
    HOST_ID_GENERATOR_KEPT = "loom.observability.host_id_generator_kept"
    HOST_ID_GENERATOR_UNAVAILABLE = "loom.observability.host_id_generator_unavailable"


class ObservabilityKey(LogKey):
    """The fields of an observability record."""

    PROVIDER = "loom.observability.provider"
