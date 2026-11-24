"""The names of what loom and an application log: events and the keys of their fields.

A stdlib-only leaf: any package, however small its dependencies, can name what it
logs by importing this module alone.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final, Self

_EVENT_FORM: Final = re.compile(
    r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*\.[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
)
_KEY_FORM: Final = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*(?:\.[a-z][a-z0-9]*(?:_[a-z0-9]+)*)*")


class _EventTable:
    """The events declared so far, each with the class that declared it.

    One class alone declares a value. A class declared again by ``importlib.reload``
    is the same declaration, and keeps its values.
    """

    __slots__ = ("_declarers",)

    def __init__(self) -> None:
        self._declarers: dict[str, type] = {}

    def declare(self, value: str, cls: type) -> None:
        """Record that *cls* declares the event *value*.

        Raises:
            ValueError: Another class, or another member of *cls*, declared *value*.
        """
        declarer = self._declarers.get(value)
        if declarer is not None and not self._is_reload_of(declarer, cls):
            raise ValueError(f"log event {value!r} is already declared by {declarer.__qualname__}")
        self._declarers[value] = cls

    @staticmethod
    def _is_reload_of(declarer: type, cls: type) -> bool:
        """Whether *cls* is the class *declarer* was, declared again."""
        return declarer is not cls and (declarer.__module__, declarer.__qualname__) == (
            cls.__module__,
            cls.__qualname__,
        )


_DECLARED_EVENTS: Final = _EventTable()


class LogEvent(StrEnum):
    """Base of an area's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields (a :class:`LogKey` each), so a collector
    can filter on both. Every member is named ``<area>.<subject>_<fact>`` in
    lowercase, and one class alone declares each value: declaring a member out of
    form, or a value another class or member already declared, raises ``ValueError``.
    The same class declared again, as ``importlib.reload`` does, keeps its values.

    Example::

        class InvoiceEvent(LogEvent):
            ISSUED = "billing.invoice_issued"

        log.info(InvoiceEvent.ISSUED, extra={InvoiceKey.ID: invoice_id})
    """

    _value_: str

    def __new__(cls, value: str) -> Self:
        """Declare the event *value*.

        Raises:
            ValueError: *value* is out of form, or another class or member declared it.
        """
        if _EVENT_FORM.fullmatch(value) is None:
            raise ValueError(f"log event {value!r} is not named <area>.<subject>_<fact>")
        _DECLARED_EVENTS.declare(value, cls)
        member = str.__new__(cls, value)
        member._value_ = value
        return member


class ValueKind(StrEnum):
    """The JSON kind of value a key carries; ``OBJECT`` names a bag of :class:`SubKey` fields."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class SubKey(str):
    """A key under an ``OBJECT`` :class:`LogKey` for an open bag, ``loom.lifecycle.meta.<name>``."""

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
    its :class:`ValueKind` beside its name, ``ROWS = "loom.etl.rows", ValueKind.NUMBER``.
    A key is dotted lowercase ``snake_case`` and only loom declares a ``loom.`` key:
    declaring one out of form, a ``loom.`` key elsewhere, or a key that clashes with
    another raises ``ValueError``. A bag whose names are the developer's is an
    ``OBJECT`` member, and takes its keys through :meth:`of`.
    """

    _value_: str
    kind: ValueKind

    def __new__(cls, value: str, kind: ValueKind = ValueKind.STRING) -> Self:
        """Declare the key *value*, carrying *kind*.

        Raises:
            ValueError: *value* is not dotted lowercase ``snake_case``, is a ``loom.`` key
                declared outside the ``loom`` package, is an area key of loom outside
                ``loom.``, or clashes with a declared key.
        """
        if _KEY_FORM.fullmatch(value) is None:
            raise ValueError(f"log key {value!r} is not dotted lowercase snake_case")
        declared_by_loom = cls.__module__.partition(".")[0] == "loom"
        if value.startswith("loom.") != declared_by_loom and cls.__module__ != __name__:
            raise ValueError(
                f"log key {value!r} is reserved to loom: only loom, and always loom.<area>.<name>"
            )
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
    ERROR_TYPE = "error.type"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
    ERROR_CODE = "loom.error.code"
    HTTP_REQUEST_METHOD = "http.request.method"
    HTTP_RESPONSE_STATUS_CODE = "http.response.status_code", ValueKind.NUMBER
    HTTP_ROUTE = "http.route"
    USER_HASH = "user.hash"
