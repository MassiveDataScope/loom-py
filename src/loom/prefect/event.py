"""What the Prefect integration logs: its events and the keys of their fields."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class PrefectEvent(LogEvent):
    """The events the Prefect flows, observers, notifiers and manifest store log."""

    NOTIFICATION_FAILED = "loom.prefect.notification_failed"
    MANIFEST_LOAD_FAILED = "loom.prefect.manifest_load_failed"
    MANIFEST_SAVE_FAILED = "loom.prefect.manifest_save_failed"
    MANIFEST_DELETE_FAILED = "loom.prefect.manifest_delete_failed"
    STEP_EVENT_RECEIVED = "loom.prefect.step_event_received"
    STEP_EVENT_FAILED = "loom.prefect.step_event_failed"
    SCHEDULES_PAUSED = "loom.prefect.schedules_paused"
    SCHEDULES_PAUSE_FAILED = "loom.prefect.schedules_pause_failed"
    BACKFILL_CHUNK_STARTED = "loom.prefect.backfill_chunk_started"
    BACKFILL_FINALIZE_STARTED = "loom.prefect.backfill_finalize_started"
    PARAMETERS_RECORD_FAILED = "loom.prefect.parameters_record_failed"


class PrefectKey(LogKey):
    """The fields of a Prefect record."""

    FLOW = "loom.prefect.flow"
    RUN = "loom.prefect.run"
    DEPLOYMENT = "loom.prefect.deployment"
    WINDOW = "loom.prefect.window"
