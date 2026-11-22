"""The formats loom ships: where the records go and what they look like.

A format is a :class:`LogFormat` that installs itself on the root logger. An extra
or an application adds its own under the ``loom.logging.formats`` entry-point group,
as a callable with no arguments that returns it, such as the class.
"""

from __future__ import annotations

import json
import logging
import math
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Protocol

from loom.core.environment import LambdaHost
from loom.core.logger.field import LogField
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

_STANDARD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered message or text value; longer ones end in the marker."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

_STACKTRACE_LEVEL: Final = logging.ERROR

_STACK_WRITTEN: Final = "__loom_stack_written__"

HANDLER_NAME: Final = "loom"
"""Name of the handler loom installs, so a second configuration replaces it only."""

LAMBDA_RUNTIME_HANDLERS: Final = frozenset(
    {"LambdaLoggerHandler", "LambdaLoggerHandlerWithFrameType"}
)
"""Class names of the handler the Lambda Python runtime puts on the root logger."""

RESERVED_KEYS: Final = frozenset(
    {"timestamp", "level", "message", "logger", LogField.EXCEPTION_STACKTRACE.value}
)
"""Keys a format writes from the record itself; an ``extra`` key of that name cannot take them."""

EXTRA_PREFIX: Final = "loom.extra."
"""Prefix under which a colliding ``extra`` key is written."""


def record_fields(record: logging.LogRecord) -> dict[str, object]:
    """The fields of *record*: what was passed as ``extra`` and what a filter stamped."""
    return {
        str(key): value
        for key, value in record.__dict__.items()
        if key not in _STANDARD_ATTRIBUTES and not key.startswith("_")
    }


def _truncated(text: str) -> str:
    if len(text) <= MAX_VALUE_LENGTH:
        return text
    return text[: MAX_VALUE_LENGTH - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


def _scalar(value: object) -> object:
    """*value* with a secret masked, a string cut and a non-finite float written as text."""
    if isinstance(value, Secret | RedactedMap):
        return MASK
    if isinstance(value, Dsn):
        return _truncated(str(value))
    if isinstance(value, str):
        return _truncated(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def render_value(value: object) -> object:
    """What a formatter writes for a field *value*.

    A scalar is written as it is, a string cut at :data:`MAX_VALUE_LENGTH`; a
    short list of scalars is written as a list; a ``Secret`` is masked; any
    other object is written as its type name, never its ``repr``.
    """
    if value is None or isinstance(value, bool | int | float | str | Secret | RedactedMap | Dsn):
        return _scalar(value)
    if isinstance(value, list | tuple) and len(value) <= MAX_LIST_ITEMS:
        items = [_scalar(item) for item in value]
        if all(item is None or isinstance(item, bool | int | float | str) for item in items):
            return items
    return type(value).__name__


def _rendered_fields(record: logging.LogRecord) -> dict[str, object]:
    """The fields of *record* as a format writes them, in one pass.

    A key the format writes itself (:data:`RESERVED_KEYS`) is never taken from
    ``extra``: it is written under ``loom.extra.<key>``.
    """
    return {
        EXTRA_PREFIX + name if name in RESERVED_KEYS else name: render_value(value)
        for name, value in record_fields(record).items()
    }


def _stacktrace(formatter: logging.Formatter, record: logging.LogRecord) -> str | None:
    """The traceback of *record* from ERROR up, written once per exception.

    Every handler of the same record gets the same text, cached on the record as the
    standard library does; a later record of an exception already written carries its
    type and code only.
    """
    if record.levelno < _STACKTRACE_LEVEL or record.exc_info is None:
        return None
    exc = record.exc_info[1]
    if exc is None:
        return None
    if record.exc_text is None:
        if getattr(exc, _STACK_WRITTEN, False):
            return None
        record.exc_text = _truncated(formatter.formatException(record.exc_info))
        BaseException.__setattr__(exc, _STACK_WRITTEN, True)
    return record.exc_text


class JsonFormatter(logging.Formatter):
    """One JSON object per record, fields at the top level.

    The keys of Lambda's JSON format (``timestamp``, ``level``, ``message``,
    ``logger``) come first and cannot be overwritten by ``extra``. From ERROR up an
    exception adds ``exception.stacktrace``, once per exception. Values are rendered
    by :func:`render_value`.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line."""
        entry: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "message": _truncated(record.getMessage()),
            "logger": record.name,
            **_rendered_fields(record),
        }
        stacktrace = _stacktrace(self, record)
        if stacktrace is not None:
            entry[LogField.EXCEPTION_STACKTRACE.value] = stacktrace
        return json.dumps(entry, default=str)


_CONTINUATION: Final = "  | "


def _one_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


class TextFormatter(logging.Formatter):
    """A readable line: time, level, logger, the message, then ``key=value`` fields.

    A line break in the message or in a value is written escaped, so a value
    cannot forge a second record. From ERROR up the traceback follows the line,
    each of its rows prefixed so that the record's first line is the only unindented one.
    """

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as one line, its traceback after it from ERROR up."""
        head = f"{self.formatTime(record)} {record.levelname:<7} {record.name}"
        line = f"{head} {_one_line(_truncated(record.getMessage()))}"
        fields = " ".join(
            f"{key}={_one_line(str(value))}" for key, value in _rendered_fields(record).items()
        )
        if fields:
            line = f"{line} {fields}"
        stacktrace = _stacktrace(self, record)
        if stacktrace is None:
            return line
        return "\n".join([line, *(f"{_CONTINUATION}{row}" for row in stacktrace.splitlines())])


class LogFormat(Protocol):
    """How and where the process writes its records."""

    def install(self, root: logging.Logger, level: str) -> list[logging.Handler]:
        """Put the format on *root* at *level*; return the handlers loom stamps."""
        ...


@dataclass(frozen=True, slots=True)
class StdoutFormat:
    """Loom's own handler on stdout, with *formatter*; it sets the root level.

    The handler is named :data:`HANDLER_NAME` and replaces the one a previous
    installation put; a handler somebody else added stays.

    Attributes:
        formatter: What each record looks like.
    """

    formatter: logging.Formatter

    @classmethod
    def json(cls) -> StdoutFormat:
        """JSON lines: services, containers and workers."""
        return cls(JsonFormatter())

    @classmethod
    def text(cls) -> StdoutFormat:
        """Readable lines: development."""
        return cls(TextFormatter())

    def install(self, root: logging.Logger, level: str) -> list[logging.Handler]:
        """Replace loom's handler on *root* with one on stdout and set the level."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self.formatter)
        handler.set_name(HANDLER_NAME)
        for installed in [h for h in root.handlers if h.get_name() == HANDLER_NAME]:
            root.removeHandler(installed)
        root.addHandler(handler)
        root.setLevel(level.upper())
        return [handler]


@dataclass(frozen=True, slots=True)
class LambdaFormat:
    """The Lambda runtime's handler and level stay; loom gives it a JSON layout when needed.

    With ``AWS_LAMBDA_LOG_FORMAT=JSON`` the runtime writes each field at the top level,
    so its formatter stays. With the default ``Text`` format it writes the message only
    and drops every field, so its handler gets :class:`JsonFormatter`.

    Attributes:
        host: What the runtime says about the process.
    """

    host: LambdaHost

    @classmethod
    def from_process(cls) -> LambdaFormat:
        """The format for the running process."""
        return cls(LambdaHost.from_process())

    def install(self, root: logging.Logger, level: str) -> list[logging.Handler]:
        """Lay out the runtime's handler when it needs it; *level* stays the function's."""
        del level
        if not self.host.has_json_logs:
            for handler in root.handlers:
                if type(handler).__name__ in LAMBDA_RUNTIME_HANDLERS:
                    handler.setFormatter(JsonFormatter())
        return list(root.handlers)
