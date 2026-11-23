"""What composition logs: its events and the keys of their fields."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class CompositionEvent(LogEvent):
    """The events composing and hosting an application log."""

    APPLICATION_COMPOSED = "composition.application_composed"
    APPLICATION_CLOSE_FAILED = "composition.application_close_failed"
    INLINE_JOBS_WAIT_IN_MEMORY = "composition.inline_jobs_wait_in_memory"


class CompositionKey(LogKey):
    """The fields of a composition record."""

    PHASE_MS = "loom.composition.phase_ms"
