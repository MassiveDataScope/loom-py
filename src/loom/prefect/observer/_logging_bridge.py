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

    Prefect batches, sizes and sends each record as it does its own. A record is
    sent as loom's text body: its event, its fields in logfmt with secrets masked
    and its stack, without where the call was made (``code.*``). It carries the
    handler's flow run and, inside a step, the active task run, which Prefect reads
    from a copy of the record so the shared record is never modified.

    Args:
        flow_run_id: The flow run every record belongs to.
    """

    def __init__(self, flow_run_id: uuid.UUID) -> None:
        super().__init__()
        self._flow_run_id = flow_run_id

    def prepare(self, record: logging.LogRecord) -> dict[str, Any]:
        stamped = copy.copy(record)
        stamped.flow_run_id = self._flow_run_id
        stamped.task_run_id = active_task_run_id.get()
        return super().prepare(stamped)

    def format(self, record: logging.LogRecord) -> str:
        return RecordFields(record).body(without=_NOT_SHOWN)


@contextmanager
def log_bridge(flow_run_id: uuid.UUID | None) -> Iterator[None]:
    """Send loom's records to the Prefect logs of *flow_run_id* while the block runs.

    The handler is on the ``loom`` logger, so no other library's record is sent.
    On exit the pending records are sent and the handler is detached. With no flow
    run nothing is attached.
    """
    if flow_run_id is None:
        yield
        return
    loom_logger = logging.getLogger("loom")
    handler = PrefectLogBridgeHandler(flow_run_id)
    loom_logger.addHandler(handler)
    try:
        yield
    finally:
        handler.flush()
        loom_logger.removeHandler(handler)
        handler.close()


__all__ = ["PrefectLogBridgeHandler", "active_task_run_id", "log_bridge"]
