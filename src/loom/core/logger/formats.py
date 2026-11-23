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
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Final

from loom.core.environment import lambda_writes_json_logs
from loom.core.logger.field import LogField
from loom.core.logger.settings import LogSettings
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

_STANDARD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered message or text value; longer ones end in the marker."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

_STACKTRACE: Final = LogField.EXCEPTION_STACKTRACE.value

RESERVED_KEYS: Final = frozenset(
    {
        "timestamp",
        "level",
        "message",
        "logger",
    }
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
    ``extra``: it is written under ``loom.extra.<key>``. The stack is left out: each
    format writes it whole, in its own layout.
    """
    return {
        EXTRA_PREFIX + name if name in RESERVED_KEYS else name: render_value(value)
        for name, value in record_fields(record).items()
        if name != _STACKTRACE
    }


def _stack_of(record: logging.LogRecord) -> str | None:
    """The stack the context filter wrote on *record*, if any."""
    stack = record.__dict__.get(_STACKTRACE)
    return stack if isinstance(stack, str) else None


class JsonFormatter(logging.Formatter):
    """One JSON object per record, fields at the top level.

    The keys of Lambda's JSON format (``timestamp``, ``level``, ``message``,
    ``logger``) come first and cannot be overwritten by ``extra``. Values are
    rendered by :func:`render_value`; what the record says about its exception
    comes from the context filter.
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
        stack = _stack_of(record)
        if stack is not None:
            entry[_STACKTRACE] = stack
        return json.dumps(entry, default=str)


_CONTINUATION: Final = "  | "


def _one_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


class TextFormatter(logging.Formatter):
    """A readable line: time, level, logger, the message, then ``key=value`` fields.

    A line break in the message or in a value is written escaped, so a value
    cannot forge a second record. The stack the context filter wrote follows the
    line, each of its rows prefixed so that the record's first line is the only unindented one.
    """

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as one line, its stack after it when it carries one."""
        head = f"{self.formatTime(record)} {record.levelname:<7} {record.name}"
        line = f"{head} {_one_line(_truncated(record.getMessage()))}"
        fields = " ".join(
            f"{key}={_one_line(str(value))}" for key, value in _rendered_fields(record).items()
        )
        if fields:
            line = f"{line} {fields}"
        stack = _stack_of(record)
        if stack is None:
            return line
        return "\n".join([line, *(f"{_CONTINUATION}{row}" for row in stack.splitlines())])


_LAMBDA_JSON_FRAMES: Final = {
    logging.NOTSET: bytes.fromhex("a55a0002"),
    logging.DEBUG: bytes.fromhex("a55a000a"),
    logging.INFO: bytes.fromhex("a55a000e"),
    logging.WARNING: bytes.fromhex("a55a0012"),
    logging.ERROR: bytes.fromhex("a55a0016"),
    logging.CRITICAL: bytes.fromhex("a55a001a"),
}
"""The frame type the Lambda runtime gives a JSON record of each level (awslambdaric 4.2.3)."""


class LambdaFormatter(JsonFormatter):
    """Loom's JSON line as the Lambda runtime's handler takes it (awslambdaric 4.2.3).

    It ends in a line break, as the runtime's own formatters end their records.
    Under the function's ``JSON`` log format it also states the record's JSON
    frame type, which the runtime's handler reads; without it the platform would
    take the line for text.

    Args:
        frames: The frame type of each level; empty under the ``Text`` log format.
    """

    def __init__(self, frames: Mapping[int, bytes]) -> None:
        super().__init__()
        self._frames = frames

    @classmethod
    def for_function(cls) -> LambdaFormatter:
        """The formatter for this function's log format."""
        return cls(_LAMBDA_JSON_FRAMES if lambda_writes_json_logs() else {})

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line, ended by a line break."""
        frame = self._frames.get(min(record.levelno, logging.CRITICAL) // 10 * 10)
        if frame is not None:
            record.__dict__["_frame_type"] = frame
        return super().format(record) + "\n"


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


LAMBDA_RUNTIME_HANDLERS: Final = frozenset(
    {"LambdaLoggerHandler", "LambdaLoggerHandlerWithFrameType"}
)
"""Class names of the Lambda runtime's handlers (awslambdaric 4.2.3), which loom cannot import."""


def lambda_format(settings: LogSettings) -> logging.Handler | None:
    """Give the Lambda runtime's handlers loom's JSON layout, or write JSON to stdout without them.

    The formatter follows the function's log format, so the runtime frames each
    record as that format expects. Handlers that are not the runtime's are left
    alone. With no runtime handler on the root logger, loom writes its own JSON
    lines to stdout, which Lambda captures, rather than lose the fields.
    """
    runtime = [
        handler
        for handler in logging.getLogger().handlers
        if type(handler).__name__ in LAMBDA_RUNTIME_HANDLERS
    ]
    if not runtime:
        return json_format(settings)
    formatter = LambdaFormatter.for_function()
    for handler in runtime:
        handler.setFormatter(formatter)
    return None
