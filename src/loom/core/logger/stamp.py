"""What every record says about who emitted it and what was running."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from enum import StrEnum

from loom.core.logger.context import current_log_fields
from loom.core.logger.field import LogField
from loom.core.tracing.context import get_trace_id


class LogRuntime(StrEnum):
    """The kind of process that emits the records."""

    FASTAPI = "fastapi"
    CELERY = "celery"
    LAMBDA = "lambda"
    CLI = "cli"


@dataclass(frozen=True, slots=True)
class LogIdentity:
    """Which service, and which process of it, emits the records.

    Attributes:
        service_name: ``app.name``; absent for a process with no application.
        service_version: ``app.version`` when declared.
        environment: The environment the process runs in.
        runtime: The kind of process.
    """

    environment: str
    runtime: LogRuntime = LogRuntime.CLI
    service_name: str | None = None
    service_version: str | None = None

    def with_runtime(self, runtime: LogRuntime) -> LogIdentity:
        """This identity, running as *runtime*."""
        return replace(self, runtime=runtime)

    def fields(self) -> dict[str, str]:
        """The fields it stamps, without those it does not know."""
        known = {
            LogField.SERVICE_NAME: self.service_name,
            LogField.SERVICE_VERSION: self.service_version,
            LogField.ENVIRONMENT: self.environment,
            LogField.RUNTIME: self.runtime.value,
        }
        return {key: value for key, value in known.items() if value is not None}


class ContextFilter(logging.Filter):
    """Stamp each record with the emitter's identity, its trace and the running work.

    Attach it to a handler, not to a logger, so the records of the
    application and of its libraries get the same fields as loom's. A field
    already on the record, passed as ``extra``, is kept. It never drops a record.

    Args:
        identity: The service and process the records come from.
    """

    def __init__(self, identity: LogIdentity) -> None:
        super().__init__()
        self._identity = identity
        self._identity_fields = identity.fields()

    def use_runtime(self, runtime: LogRuntime) -> None:
        """Stamp the following records as emitted by *runtime*."""
        self._identity = self._identity.with_runtime(runtime)
        self._identity_fields = self._identity.fields()

    def filter(self, record: logging.LogRecord) -> bool:
        """Add the fields to *record* and let it through."""
        stamped = {
            LogField.CODE_FUNCTION: record.funcName,
            LogField.CODE_LINE: record.lineno,
            LogField.CODE_FILE: record.pathname,
            **self._identity_fields,
            **current_log_fields(),
        }
        trace_id = get_trace_id()
        if trace_id is not None:
            stamped[LogField.TRACE_ID] = trace_id
        for key, value in stamped.items():
            if key not in record.__dict__:
                setattr(record, key, value)
        return True
