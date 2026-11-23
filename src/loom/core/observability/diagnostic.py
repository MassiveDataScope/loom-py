"""The records observability logs about itself, apart from the lifecycle it writes."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class ObservabilityEvent(LogEvent):
    """What the observability runtime logs about its own observers and providers."""

    OBSERVER_FAILED = "observability.observer_failed"
    HOST_PROVIDER_RANDOM_TRACE_IDS = "observability.host_provider_random_trace_ids"
    HOST_ID_GENERATOR_UNAVAILABLE = "observability.host_id_generator_unavailable"


class ObservabilityKey(LogKey):
    """The fields of an observability record."""

    PROVIDER = "loom.observability.provider"
