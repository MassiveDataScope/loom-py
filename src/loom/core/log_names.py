"""The names of what loom and an application log: events and the keys of their fields.

A stdlib-only leaf: any package, however small its dependencies, can name what it
logs by importing this module alone.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final, Self

EVENT_FORM: Final = re.compile(
    r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*\.[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
)
"""``<area>.<subject>_<fact>``, lowercase: ``job.delivery_abandoned``."""


class FailureForm(StrEnum):
    """How the records of a :class:`LogEvent` carry the failure they name."""

    ATTACHED = "attached"
    """The record passes the exception as ``exc_info``; the context filter writes its fields."""
    SUMMARY_ONLY = "summary_only"
    """The record writes the exception's summary fields without ``exc_info``: its emitter
    holds only the summary, or must not keep the frames."""


class LogEvent(StrEnum):
    """Base of an area's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields (a :class:`LogKey` each), so a collector
    can filter on both. Every member of a subclass is named
    ``<area>.<subject>_<fact>`` in lowercase; an architecture test enforces it.
    A member names a failure :attr:`FailureForm.ATTACHED` unless it declares its
    :class:`FailureForm` beside its name.

    Example::

        class InvoiceEvent(LogEvent):
            ISSUED = "billing.invoice_issued"
            REFUSED = "billing.invoice_refused", FailureForm.SUMMARY_ONLY

        log.info(InvoiceEvent.ISSUED, extra={"invoice.id": invoice_id})
    """

    _value_: str
    failure_form: FailureForm

    def __new__(cls, value: str, failure_form: FailureForm = FailureForm.ATTACHED) -> Self:
        """Declare the event *value*, whose records carry a failure as *failure_form* says."""
        member = str.__new__(cls, value)
        member._value_ = value
        member.failure_form = failure_form
        return member

    @property
    def is_well_formed(self) -> bool:
        """Whether the value follows ``<area>.<subject>_<fact>``."""
        return EVENT_FORM.fullmatch(self.value) is not None


class ValueKind(StrEnum):
    """The JSON kind of value a key carries; ``OBJECT`` names a bag of :class:`SubKey` fields."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"

    @classmethod
    def of(cls, rendered: object) -> ValueKind | None:
        """The kind of a value :func:`~loom.core.logger.formats.render_value` wrote.

        The inverse of ``render_value``: ``None`` for a missing value.
        """
        match rendered:
            case None:
                return None
            case bool():
                return cls.BOOLEAN
            case int() | float():
                return cls.NUMBER
            case list():
                return cls.ARRAY
            case _:
                return cls.STRING


class SubKey(str):
    """A key made under an ``OBJECT`` :class:`LogKey` for an open bag: ``loom.meta.<name>``."""

    __slots__ = ()


class _KeyTree:
    """The keys declared so far, so that no key is declared with two meanings.

    A key keeps one kind wherever it is declared, and never names both a value and
    the prefix of other keys: ``loom.job`` and ``loom.job.queue`` could not both nest
    in a JSON or OpenTelemetry collector.
    """

    __slots__ = ("_kinds", "_prefixes")

    def __init__(self) -> None:
        self._kinds: dict[str, ValueKind] = {}
        self._prefixes: set[str] = set()

    def declare(self, key: str, kind: ValueKind) -> None:
        """Record *key* carrying *kind*.

        Raises:
            ValueError: *key* was declared with another kind, prefixes a declared key,
                or lies under one.
        """
        prefixes = {key[:at] for at, char in enumerate(key) if char == "."}
        if (
            self._kinds.get(key, kind) != kind
            or key in self._prefixes
            or not prefixes.isdisjoint(self._kinds)
        ):
            raise ValueError(f"log key {key!r} would carry a second meaning")
        self._kinds[key] = kind
        self._prefixes |= prefixes


_DECLARED_KEYS: Final = _KeyTree()


class LogKey(StrEnum):
    """Base of an area's field keys, declared next to its :class:`LogEvent`.

    An area key is ``loom.<area>.<name>``; the OpenTelemetry semantic-convention
    names belong in :class:`LogField` only. A member carries text unless it declares
    its :class:`ValueKind` beside its name, ``ROWS = "loom.etl.rows", ValueKind.NUMBER``;
    declaring a key that clashes with another raises ``ValueError``. A bag whose
    names are the developer's is an ``OBJECT`` member, and takes its keys through :meth:`of`.
    """

    _value_: str
    kind: ValueKind

    def __new__(cls, value: str, kind: ValueKind = ValueKind.STRING) -> Self:
        """Declare the key *value*, carrying *kind*."""
        _DECLARED_KEYS.declare(value, kind)
        member = str.__new__(cls, value)
        member._value_ = value
        member.kind = kind
        return member

    def of(self, name: str) -> SubKey:
        """The key for *name* inside the bag this key names.

        Raises:
            ValueError: This key carries a value, not a bag.
        """
        if self.kind is not ValueKind.OBJECT:
            raise ValueError(f"log key {self.value!r} is not a bag")
        return SubKey(f"{self.value}.{name}")


class LogField(LogKey):
    """A key that several areas write, or an OpenTelemetry semantic-convention name."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    ENVIRONMENT = "deployment.environment.name"
    RUNTIME = "loom.runtime"
    TRACE_ID = "trace_id"
    USE_CASE = "loom.use_case.name"
    JOB = "loom.job.name"
    TASK_ID = "loom.task_id"
    ATTEMPT = "loom.attempt", ValueKind.NUMBER
    REASON = "loom.reason"
    CODE_FUNCTION = "code.function.name"
    CODE_LINE = "code.line.number", ValueKind.NUMBER
    CODE_FILE = "code.file.path"
    SCOPE = "loom.scope"
    NAME = "loom.name"
    CORRELATION_ID = "loom.correlation_id"
    DURATION_MS = "loom.duration_ms", ValueKind.NUMBER
    STATUS = "loom.status"
    OBSERVER = "loom.observer"
    EVENT_KIND = "loom.event_kind"
    USE_CASE_COUNT = "loom.use_case_count", ValueKind.NUMBER
    EXCEPTION_TYPE = "exception.type"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
    ERROR_CODE = "loom.error.code"
    HTTP_REQUEST_METHOD = "http.request.method"
    HTTP_RESPONSE_STATUS_CODE = "http.response.status_code", ValueKind.NUMBER
    HTTP_ROUTE = "http.route"
