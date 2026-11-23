"""What the Celery integration logs: its events."""

from __future__ import annotations

from loom.core.log_names import LogEvent


class CeleryEvent(LogEvent):
    """The events the Celery transport and worker log."""

    FAILURE_CALLBACK_NOT_SENT = "celery.failure_callback_not_sent"
    WORKER_START_FAILED = "celery.worker_start_failed"
    WORKER_PROCESS_OPEN_FAILED = "celery.worker_process_open_failed"
    WORKER_CLOSE_FAILED = "celery.worker_close_failed"
    WORKER_GRAPH_INITIALIZED = "celery.worker_graph_initialized"
