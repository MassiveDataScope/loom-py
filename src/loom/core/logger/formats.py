"""The formats loom ships: the layout of a record on the console, the collector or Lambda.

A format is a callable from the :class:`~loom.core.logger.settings.LogSettings`
to the handler that writes the records, or to ``None`` when the runtime's
handler writes them. A format only lays a record out: its exception and its
values are already safe when it runs (:mod:`loom.core.logger.record`,
:mod:`loom.core.logger.filter`). An extra or an application adds its own under
the ``loom.logging.formats`` entry-point group.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Final

from loom.core.errors.log_events import ErrorLogField
from loom.core.logger.filter import STANDARD_ATTRIBUTES, LogFilter
from loom.core.logger.settings import LogSettings
from loom.core.vocabulary import render_value

LAMBDA_RUNTIME_HANDLERS: Final = frozenset(
    {"LambdaLoggerHandler", "LambdaLoggerHandlerWithFrameType"}
)
"""Class names of the handler the Lambda Python runtime puts on the root logger."""

_STACKTRACE: Final = ErrorLogField.EXCEPTION_STACKTRACE.value
_CONTINUATION: Final = "  | "


def record_fields(record: logging.LogRecord) -> dict[str, object]:
    """The fields of *record*: what was passed as ``extra`` and what its stamp wrote."""
    return {
        key: value
        for key, value in record.__dict__.items()
        if key not in STANDARD_ATTRIBUTES and not key.startswith("_")
    }


class JsonFormatter(logging.Formatter):
    """One JSON object per record: its fields first, then the four keys of Lambda's JSON format.

    ``timestamp``, ``level``, ``message`` and ``logger`` are written last, so
    no field can overwrite them. A value JSON cannot hold is written as its
    type name.
    """

    @staticmethod
    def _type_name(value: object) -> str:
        return type(value).__name__

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line."""
        entry = record_fields(record)
        entry["timestamp"] = datetime.fromtimestamp(record.created, UTC).isoformat(
            timespec="milliseconds"
        )
        entry["level"] = record.levelname
        record.message = record.getMessage()
        entry["message"] = render_value(record.message)
        entry["logger"] = record.name
        return json.dumps(entry, default=self._type_name)


def _one_line(text: object) -> str:
    return str(text).replace("\r", "\\r").replace("\n", "\\n")


class TextFormatter(logging.Formatter):
    """A readable line: time, level, logger, the message, then ``key=value`` fields.

    Like the standard formatter, it leaves the message on the record as ``message``.

    A line break in the message or in a value is written escaped, so a value
    cannot forge a second record. ``exception.stacktrace`` follows the line, each
    of its rows indented, so the record's first line is its only unindented one.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as one line, its stack after it."""
        fields = record_fields(record)
        stacktrace = fields.pop(_STACKTRACE, None)
        record.message = record.getMessage()
        message = _one_line(render_value(record.message))
        rows = [
            f"{self.formatTime(record)} {record.levelname:<7} {record.name} {message}",
            *(f"{key}={_one_line(value)}" for key, value in fields.items()),
        ]
        line = " ".join(rows)
        if not isinstance(stacktrace, str):
            return line
        return "\n".join([line, *(f"{_CONTINUATION}{row}" for row in stacktrace.splitlines())])


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
    """Keep the Lambda runtime's handler and level; give it loom's JSON layout when it needs one.

    The runtime sets ``AWS_LAMBDA_LOG_FORMAT`` to ``JSON`` or ``Text``. With
    ``JSON`` it writes each field at the top level of its JSON line, so its
    formatter stays. With the default ``Text`` format it would write the message
    only and drop every field, so its handler gets :class:`JsonFormatter`; no
    other handler is touched.
    """
    del settings
    if os.environ.get("AWS_LAMBDA_LOG_FORMAT", "").upper() == "JSON":
        return
    for handler in ProcessHandlers().lambda_runtime():
        handler.setFormatter(JsonFormatter())


class ProcessHandlers:
    """The logging handlers of this process, looked up where they are installed.

    Args:
        root: The root logger whose tree is searched; the process's own by default.
    """

    __slots__ = ("_root",)

    def __init__(self, root: logging.Logger | None = None) -> None:
        self._root = root if root is not None else logging.getLogger()

    def lambda_runtime(self) -> tuple[logging.Handler, ...]:
        """The Lambda runtime's handlers on the root logger; none outside Lambda."""
        return tuple(
            handler
            for handler in self._root.handlers
            if type(handler).__name__ in LAMBDA_RUNTIME_HANDLERS
        )

    def unfiltered(self) -> tuple[logging.Handler, ...]:
        """The handlers, on the root logger or any other, that do not render values."""
        return tuple(
            handler
            for handler in self._every()
            if not any(isinstance(f, LogFilter) for f in handler.filters)
        )

    def _every(self) -> list[logging.Handler]:
        loggers = [self._root] + [
            logger
            for logger in self._root.manager.loggerDict.values()
            if isinstance(logger, logging.Logger)
        ]
        return [handler for logger in loggers for handler in logger.handlers]
