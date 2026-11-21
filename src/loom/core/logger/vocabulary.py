"""The names a loom record uses: its events and its field keys, each declared once.

A record is *(event, level, fields)*. The event is a member of a
:class:`~loom.core.fields.LogEvent` subclass, declared by the owner of the fact; a field key is a
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

from collections.abc import Mapping
from types import MappingProxyType

from loom.core.fields import EVENT_FORM, LogEvent, LogKey, Scalar


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

    HTTP_ROUTE = "http.route"
    HTTP_REQUEST_METHOD = "http.request.method"
    HTTP_RESPONSE_STATUS_CODE = "http.response.status_code"
    MESSAGING_DESTINATION_NAME = "messaging.destination.name"


SpanAttributes = Mapping[LogKey, Scalar]
"""The attributes of a span and of its lifecycle events: vocabulary keys, scalar values."""

NO_ATTRIBUTES: SpanAttributes = MappingProxyType({})
"""No attributes: the default of a span or an event that carries none."""


__all__ = [
    "EVENT_FORM",
    "NO_ATTRIBUTES",
    "LogEvent",
    "LogField",
    "LogKey",
    "Scalar",
    "SpanAttributes",
]
