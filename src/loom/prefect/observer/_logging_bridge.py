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
from loom.core.logger.identity import LogIdentity

_current_task_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_task_run_id",
    default=None,
)

_NOT_SHOWN = frozenset(
    {
        "flow_run_id",
        "task_run_id",
        LogField.CODE_FUNCTION,
        LogField.CODE_LINE,
        LogField.CODE_FILE,
    }
)


@contextmanager
def bind_task_run(task_run_id: uuid.UUID) -> Iterator[None]:
    """Bind *task_run_id* as the active step's TaskRun for the current context.

    Records emitted while this context manager is active carry *task_run_id*
    and the flow run the handler was installed for. Outside it (between
    steps, or in observer code that runs after STEP_END) they carry the flow
    run only.

    Args:
        task_run_id: The Prefect TaskRun UUID returned by
            ``client.create_task_run(...)``.
    """
    token = _current_task_run_id.set(task_run_id)
    try:
        yield
    finally:
        _current_task_run_id.reset(token)


def current_task_run_id() -> uuid.UUID | None:
    """The active TaskRun id, or ``None`` when no step is running."""
    return _current_task_run_id.get()


class PrefectLogBridgeHandler(APILogHandler):
    """Prefect's API log handler for the records of one flow run.

    Prefect batches, sizes and sends each record as it does its own. Each is
    sent as its event followed by its fields in logfmt, masked as loom's
    formats mask them, then its stack. Where the call was made (``code.*``) is left
    out, and the service identity is written only when one is given. A record
    carries the flow run it was built for and, inside a step, the active task run.
    The shared record is never modified: Prefect reads the ids from a copy.

    Args:
        flow_run_id: The flow run every record belongs to.
        identity: The service and process to write on every record; none writes no identity.
    """

    def __init__(self, flow_run_id: uuid.UUID, identity: LogIdentity | None = None) -> None:
        super().__init__(level=logging.NOTSET)
        self._flow_run_id = flow_run_id
        self._identity = identity

    def prepare(self, record: logging.LogRecord) -> dict[str, Any]:
        stamped = copy.copy(record)
        stamped.flow_run_id = self._flow_run_id
        stamped.task_run_id = _current_task_run_id.get()
        return super().prepare(stamped)

    def format(self, record: logging.LogRecord) -> str:
        fields = RecordFields(record, self._identity)
        line = " ".join(filter(None, (fields.message, fields.logfmt(without=_NOT_SHOWN))))
        stack = fields.stack
        return line if stack is None else f"{line}\n{stack}"


def install_log_bridge(flow_run_id: uuid.UUID | None, identity: LogIdentity | None = None) -> None:
    """Send the root logger's records to the Prefect logs of *flow_run_id*.

    A handler installed before is replaced; ``None`` installs nothing.
    """
    uninstall_log_bridge()
    if flow_run_id is not None:
        logging.getLogger().addHandler(PrefectLogBridgeHandler(flow_run_id, identity))


def uninstall_log_bridge() -> None:
    """Send what is pending and detach the bridge handler from the root logger."""
    root = logging.getLogger()
    for handler in [h for h in root.handlers if isinstance(h, PrefectLogBridgeHandler)]:
        handler.flush()
        root.removeHandler(handler)
        handler.close()


__all__ = [
    "PrefectLogBridgeHandler",
    "bind_task_run",
    "current_task_run_id",
    "install_log_bridge",
    "uninstall_log_bridge",
]
