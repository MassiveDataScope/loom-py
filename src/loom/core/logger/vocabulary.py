"""The names a loom record uses: its events and its field keys, each declared once.

A record is *(event, level, fields)*. The event is a member of a
:class:`LogEvent` subclass, declared by the owner of the fact; a field key is a
member of a :class:`LogKey` subclass: :class:`LogField` for the keys every
package shares, ``<Area>LogField`` in a package's ``log_events.py`` for the keys
only it writes. A key is an OpenTelemetry semantic convention (specification
1.45.0) when one exists and ``loom.*`` otherwise. Names are a public contract,
snapshotted in ``docs/reference/log-contract.txt``.

A key whose value is a value of the business or of a person (a URL with its
query, an SQL text, a client address, a user agent, a message body, model
content) is never declared.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

EVENT_FORM: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+")
"""Dotted lowercase words, at least two: ``job.delivery_abandoned``."""

Scalar = str | int | float | bool
"""A field value that needs no rendering: what a span attribute may hold."""


class LogEvent(StrEnum):
    """Base of an owner's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields, under :class:`LogKey` members, so a
    collector can filter on both. A member's value is its full name, dotted
    lowercase words (:data:`EVENT_FORM`); an architecture test enforces it.

    Example::

        class BillingLogEvent(LogEvent):
            INVOICE_ISSUED = "billing.invoice_issued"

        log.info(BillingLogEvent.INVOICE_ISSUED, extra={LogField.COUNT: 3})
    """

    @property
    def is_well_formed(self) -> bool:
        """Whether the value is dotted lowercase words."""
        return EVENT_FORM.fullmatch(self.value) is not None


class LogKey(StrEnum):
    """Base of every vocabulary of field keys."""


class LogField(LogKey):
    """The field keys every package shares."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    ENVIRONMENT = "deployment.environment.name"
    RUNTIME = "loom.runtime"
    TRACE_ID = "trace_id"
    SPAN_ID = "span_id"
    USE_CASE = "loom.use_case"
    JOB = "loom.job"
    TASK_ID = "loom.task_id"
    ATTEMPT = "loom.attempt"
    CODE_FUNCTION = "code.function.name"
    CODE_LINE = "code.line.number"

    COUNT = "loom.count"
    FAILED_COUNT = "loom.failed_count"
    DURATION_MS = "loom.duration_ms"
    STATUS = "loom.status"
    REASON = "loom.reason"
    SCOPE = "loom.scope"
    NAME = "loom.name"
    EVENT_ID = "loom.event_id"
    CORRELATION_ID = "loom.correlation_id"
    OBSERVER = "loom.observer"
    EVENT_KIND = "loom.event_kind"
    READ_ONLY = "loom.read_only"
    SETTING = "loom.setting"
    SQL_CONNECTION = "loom.sql.connection"
    AUTH_MECHANISM = "loom.auth.mechanism"
    CONTENT_TYPE = "loom.content_type"

    EXCEPTION_TYPE = "exception.type"
    ERROR_CODE = "loom.error.code"
    EXCEPTION_STACKTRACE = "exception.stacktrace"

    HTTP_ROUTE = "http.route"
    HTTP_REQUEST_METHOD = "http.request.method"
    HTTP_RESPONSE_STATUS_CODE = "http.response.status_code"
    MESSAGING_DESTINATION_NAME = "messaging.destination.name"


SpanAttributes = Mapping[LogKey, Scalar]
"""The attributes of a span and of its lifecycle events: vocabulary keys, scalar values."""

NO_ATTRIBUTES: SpanAttributes = MappingProxyType({})
"""No attributes: the default of a span or an event that carries none."""
