"""The log events and field keys of observability."""

from __future__ import annotations

from loom.core.logger import LogEvent, LogKey


class ObservabilityLogEvent(LogEvent):
    """What the observability runtime and its observers log."""

    LIFECYCLE_STARTED = "observability.lifecycle_started"
    LIFECYCLE_ENDED = "observability.lifecycle_ended"
    OBSERVER_FAILED = "observability.observer_failed"
    HOST_TRACE_IDS_RANDOM = "observability.host_trace_ids_random"
    HOST_ID_GENERATOR_UNAVAILABLE = "observability.host_id_generator_unavailable"


class ObservabilityLogField(LogKey):
    """The field keys only observability writes."""

    OTEL_PROVIDER = "loom.otel.provider"
