"""The names of what loom and an application log: events and the keys of their fields.

A stdlib-only leaf: any package, however small its dependencies, can name what it
logs by importing this module alone.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final

EVENT_FORM: Final = re.compile(
    r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*\.[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
)
"""``<area>.<subject>_<fact>``, lowercase: ``job.delivery_abandoned``."""


class LogEvent(StrEnum):
    """Base of an area's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields (a :class:`LogKey` each), so a collector
    can filter on both. Every member of a subclass is named
    ``<area>.<subject>_<fact>`` in lowercase; an architecture test enforces it.

    Example::

        class InvoiceEvent(LogEvent):
            ISSUED = "billing.invoice_issued"

        log.info(InvoiceEvent.ISSUED, extra={"invoice.id": invoice_id})
    """

    @property
    def is_well_formed(self) -> bool:
        """Whether the value follows ``<area>.<subject>_<fact>``."""
        return EVENT_FORM.fullmatch(self.value) is not None


class SubKey(str):
    """A field key made under a :class:`LogKey` for an open bag: ``loom.meta.<name>``."""

    __slots__ = ()


class LogKey(StrEnum):
    """Base of an area's field keys, declared next to its :class:`LogEvent`.

    An area key is ``loom.<area>.<name>``; the OpenTelemetry semantic-convention
    names belong in :class:`LogField` only. A bag whose names are the developer's
    takes its keys from a member through :meth:`of`.
    """

    def of(self, name: str) -> SubKey:
        """The key for *name* inside the bag this key names."""
        return SubKey(f"{self.value}.{name}")


class LogField(LogKey):
    """A key written by the context filter, ``log_context`` and the error summary."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    ENVIRONMENT = "deployment.environment.name"
    RUNTIME = "loom.runtime"
    TRACE_ID = "trace_id"
    USE_CASE = "loom.use_case"
    JOB = "loom.job"
    TASK_ID = "loom.task_id"
    ATTEMPT = "loom.attempt"
    REASON = "loom.reason"
    CODE_FUNCTION = "code.function.name"
    CODE_LINE = "code.line.number"
    CODE_FILE = "code.file.path"
    SCOPE = "loom.scope"
    NAME = "loom.name"
    EVENT_ID = "loom.event_id"
    CORRELATION_ID = "loom.correlation_id"
    DURATION_MS = "loom.duration_ms"
    STATUS = "loom.status"
    BOOTSTRAP_MODULE = "loom.bootstrap.module"
    OBSERVER = "loom.observer"
    EVENT_KIND = "loom.event_kind"
    USE_CASE_COUNT = "loom.use_case_count"
    EXCEPTION_TYPE = "exception.type"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
    ERROR_CODE = "loom.error.code"
    HTTP_REQUEST_METHOD = "http.request.method"
    HTTP_RESPONSE_STATUS_CODE = "http.response.status_code"
