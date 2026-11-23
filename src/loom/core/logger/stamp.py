"""What every record says about who emitted it and what was running."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final

from loom.core.errors.summary import ErrorSummary
from loom.core.log_names import LogField
from loom.core.logger.context import current_log_fields
from loom.core.tracing.context import get_trace_id

MAX_STACK_LENGTH: Final = 16_384
"""Characters of a written stack; a longer one keeps its end, where the failure is."""

STACK_CUT_MARKER: Final = "[truncated]..."

_STACK_FORMATTER: Final = logging.Formatter()


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
    already on the record, passed as ``extra``, is kept. It is the one owner of what
    a record says about its exception: ``exception.type``, ``loom.error.code`` for a
    ``CodedError``, and from ERROR up ``exception.stacktrace``, never the message. It
    never drops a record.

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
        """Add the fields to *record* and let it through.

        A field already on the record wins; among the rest the trace id wins over
        the context, the context over the identity, and the identity over the code origin.
        """
        stamped: dict[str, object] = {
            LogField.CODE_FUNCTION: record.funcName,
            LogField.CODE_LINE: record.lineno,
            LogField.CODE_FILE: record.pathname,
            **self._identity_fields,
            **current_log_fields(),
        }
        trace_id = get_trace_id()
        if trace_id is not None:
            stamped[LogField.TRACE_ID] = trace_id
        fields = record.__dict__
        fields.update({key: value for key, value in stamped.items() if key not in fields})
        self._stamp_failure(record)
        return True

    @staticmethod
    def _stamp_failure(record: logging.LogRecord) -> None:
        """Write the type, code and stack of *record*'s exception; nothing else writes them.

        The stack is the filter's alone: an ``extra`` of its name is always dropped,
        since the formats write it unrendered. With an exception, its type and code
        replace any ``extra`` of their names. The stack is formatted once per record
        into the standard ``exc_text`` cache, which every handler then reuses.
        """
        fields = record.__dict__
        fields.pop(LogField.EXCEPTION_STACKTRACE, None)
        if record.exc_info is None or record.exc_info[1] is None:
            return
        fields.pop(LogField.ERROR_CODE, None)
        fields.update(ErrorSummary.of(record.exc_info[1]).fields())
        if record.levelno >= logging.ERROR:
            record.exc_text = record.exc_text or _STACK_FORMATTER.formatException(record.exc_info)
            fields[LogField.EXCEPTION_STACKTRACE] = ContextFilter._kept_end(record.exc_text)

    @staticmethod
    def _kept_end(stack: str) -> str:
        """*stack*, or its last :data:`MAX_STACK_LENGTH` characters after a marker."""
        if len(stack) <= MAX_STACK_LENGTH:
            return stack
        return STACK_CUT_MARKER + stack[len(STACK_CUT_MARKER) - MAX_STACK_LENGTH :]
