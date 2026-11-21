"""The formats loom ships: what a record looks like on the console or the collector.

A format is a callable from the :class:`~loom.core.logger.settings.LogSettings`
to the handler that writes the records, or to ``None`` when something else
already does. An extra or an application adds its own under the
``loom.logging.formats`` entry-point group.
"""

from __future__ import annotations

import json
import logging
import math
import sys
from datetime import UTC, datetime
from typing import Final

from loom.core.logger.field import LogField
from loom.core.logger.settings import LogSettings
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

_STANDARD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered message or text value; longer ones end in the marker."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

_STACKTRACE_LEVEL: Final = logging.ERROR


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
    fields = {key: render_value(value) for key, value in record_fields(record).items()}
    if record.exc_info is not None and record.exc_info[0] is not None:
        exc_type, exc, _ = record.exc_info
        fields[LogField.EXCEPTION_TYPE.value] = exc_type.__qualname__
        fields[LogField.EXCEPTION_MESSAGE.value] = render_value(str(exc))
    return fields


def _stacktrace(formatter: logging.Formatter, record: logging.LogRecord) -> str | None:
    """The traceback of *record* when it is an error and carries one."""
    if record.levelno < _STACKTRACE_LEVEL or record.exc_info is None or record.exc_info[0] is None:
        return None
    return _truncated(formatter.formatException(record.exc_info))


class JsonFormatter(logging.Formatter):
    """One JSON object per record, fields at the top level.

    The keys of Lambda's JSON format (``timestamp``, ``level``, ``message``,
    ``logger``) come first. An exception is written as ``exception.type`` and
    ``exception.message``, plus ``exception.stacktrace`` from ERROR up. Values
    are rendered by :func:`render_value`.
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


def _one_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


class TextFormatter(logging.Formatter):
    """A readable line: time, level, logger, the message, then ``key=value`` fields.

    A line break in the message or in a value is written escaped, so a value
    cannot forge a second record. From ERROR up the traceback follows the line.
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
        return line if stacktrace is None else f"{line}\n{stacktrace}"


def _stdout_handler(formatter: logging.Formatter) -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    return handler


def json_format(settings: LogSettings) -> logging.Handler:
    """Write JSON lines to stdout: services, containers and workers."""
    del settings
    return _stdout_handler(JsonFormatter())


def text_format(settings: LogSettings) -> logging.Handler:
    """Write readable lines to stdout: development."""
    del settings
    return _stdout_handler(TextFormatter())


def lambda_format(settings: LogSettings) -> None:
    """Install nothing: the Lambda runtime renders the records and sets the level."""
    del settings
