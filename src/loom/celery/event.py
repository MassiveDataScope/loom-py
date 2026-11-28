"""What the Celery integration logs: its events."""

from __future__ import annotations

from loom.core.log_names import LogEvent


class CeleryEvent(LogEvent):
    """The events the Celery transport and worker log."""

    FAILURE_CALLBACK_NOT_SENT = "loom.celery.failure_callback_not_sent"
    WORKER_START_FAILED = "loom.celery.worker_start_failed"
    WORKER_PROCESS_OPEN_FAILED = "loom.celery.worker_process_open_failed"
    WORKER_CLOSE_FAILED = "loom.celery.worker_close_failed"
    WORKER_GRAPH_INITIALIZED = "loom.celery.worker_graph_initialized"
