"""What composition logs: its events and the keys of their fields."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class CompositionEvent(LogEvent):
    """The events composing and hosting an application log."""

    APPLICATION_COMPOSED = "loom.composition.application_composed"
    APPLICATION_CLOSE_FAILED = "loom.composition.application_close_failed"
    INLINE_JOBS_HELD_IN_MEMORY = "loom.composition.inline_jobs_held_in_memory"


class CompositionKey(LogKey):
    """The fields of a composition record."""

    PHASE_MS = "loom.composition.phase_ms", ValueKind.OBJECT
