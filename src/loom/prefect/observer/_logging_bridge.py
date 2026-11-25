"""Ship loom's log records to the Prefect logs API through Prefect's own handler."""

from __future__ import annotations

import copy
import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from prefect.logging.handlers import APILogHandler

from loom.core.log_names import LogField
from loom.core.logger.formats import RecordFields

active_task_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_task_run_id",
    default=None,
)
"""The TaskRun of the step running in this context, bound by the task run observer."""

_NOT_SHOWN = frozenset(
    {
        "flow_run_id",
        "task_run_id",
        LogField.CODE_FUNCTION,
        LogField.CODE_LINE,
        LogField.CODE_FILE,
    }
)


class PrefectLogBridgeHandler(APILogHandler):
    """Prefect's API log handler for the records of one flow run.

    Prefect batches, sizes and sends each record as it does its own. Each is
    sent as its event followed by its fields in logfmt, masked as loom's
    formats mask them, then its stack. Where the call was made (``code.*``) is left
    out. A record
    carries the flow run it was built for and, inside a step, the active task run.
    A record that already names its flow run is one of Prefect's own run loggers,
    which ships it, so it is skipped. The shared record is never modified: Prefect
    reads the ids from a copy.

    Args:
        flow_run_id: The flow run every record belongs to.
    """

    def __init__(self, flow_run_id: uuid.UUID) -> None:
        super().__init__(level=logging.NOTSET)
        self._flow_run_id = flow_run_id

    def emit(self, record: logging.LogRecord) -> None:
        if hasattr(record, "flow_run_id"):
            return
        super().emit(record)

    def prepare(self, record: logging.LogRecord) -> dict[str, Any]:
        stamped = copy.copy(record)
        stamped.flow_run_id = self._flow_run_id
        stamped.task_run_id = active_task_run_id.get()
        return super().prepare(stamped)

    def format(self, record: logging.LogRecord) -> str:
        fields = RecordFields(record)
        line = " ".join(filter(None, (fields.message, fields.logfmt(without=_NOT_SHOWN))))
        stack = fields.stack
        return line if stack is None else f"{line}\n{stack}"


@contextmanager
def log_bridge(flow_run_id: uuid.UUID | None) -> Iterator[None]:
    """Send the root logger's records to the Prefect logs of *flow_run_id* while it is open.

    On exit the pending records are sent and the handler is detached. With no
    flow run nothing is attached.
    """
    if flow_run_id is None:
        yield
        return
    root = logging.getLogger()
    handler = PrefectLogBridgeHandler(flow_run_id)
    root.addHandler(handler)
    try:
        yield
    finally:
        handler.flush()
        root.removeHandler(handler)
        handler.close()


__all__ = ["PrefectLogBridgeHandler", "active_task_run_id", "log_bridge"]
