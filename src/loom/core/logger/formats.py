"""The formatters loom ships: what a record looks like on the console or the collector."""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Collection, Iterator
from datetime import UTC, datetime
from typing import Final, Literal

from loom.core.log_names import LogField
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered message or text value; longer ones end in the marker."""

MAX_LIST_ITEMS: Final = 20
"""Items a list value keeps; a longer list is cut to its first items."""

TRUNCATION_MARKER: Final = "[truncated]"
"""What a cut text says on the side it lost, next to an ellipsis."""

_ELLIPSIS: Final = "..."

_STACKTRACE: Final = LogField.EXCEPTION_STACKTRACE.value

RECORD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}
"""Attributes the stdlib gives every record; none of them is a field."""

STAMP_MARKER: Final = "_loom_stamped"
"""Attribute the context filter sets on a record it stamped; private, so never a field."""

_NOT_FIELDS: Final = RECORD_ATTRIBUTES | {_STACKTRACE}

_AS_IS: Final = frozenset({bool, int, type(None)})

_OPAQUE: Final = object()

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


def truncated(text: str, limit: int, *, keep: Literal["start", "end"] = "start") -> str:
    """*text*, or *limit* characters of it marked on the side that was cut away.

    Keeping the start ends the text in ``...[truncated]``; keeping the end, where a
    stack says what failed, starts it with ``[truncated]...``.
    """
    if len(text) <= limit:
        return text
    room = limit - len(TRUNCATION_MARKER) - len(_ELLIPSIS)
    if keep == "end":
        return f"{TRUNCATION_MARKER}{_ELLIPSIS}{text[-room:]}"
    return f"{text[:room]}{_ELLIPSIS}{TRUNCATION_MARKER}"


def _scalar(value: object) -> object:
    """*value* as a format writes a scalar, or ``_OPAQUE`` when it is none.

    A secret is masked, a string cut and a non-finite float written as text.
    """
    if type(value) in _AS_IS:
        return value
    if isinstance(value, str):
        return truncated(value, MAX_VALUE_LENGTH)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Secret | RedactedMap):
        return MASK
    if isinstance(value, Dsn):
        return truncated(str(value), MAX_VALUE_LENGTH)
    return _OPAQUE


def render_value(value: object) -> object:
    """What a formatter writes for a field *value*.

    A scalar is written as it is, a string cut at :data:`MAX_VALUE_LENGTH`; a
    list of scalars is written as a list of at most :data:`MAX_LIST_ITEMS`
    items; a ``Secret`` is masked; any other object is written as its type
    name, never its ``repr``.
    """
    if type(value) is str:
        return truncated(value, MAX_VALUE_LENGTH)
    if type(value) in _AS_IS:
        return value
    scalar = _scalar(value)
    if scalar is not _OPAQUE:
        return scalar
    if isinstance(value, list | tuple):
        items = [_scalar(item) for item in value[:MAX_LIST_ITEMS]]
        if _OPAQUE not in items:
            return items
    return type(value).__name__


class RecordFields:
    """The fields of one record as loom's outputs write them, read without changing it.

    The fields are what was passed as ``extra`` and what a filter stamped. Each
    output renders its own copy, so every handler sees the record as it was logged.
    """

    __slots__ = ("_record",)

    def __init__(self, record: logging.LogRecord) -> None:
        self._record = record

    @property
    def timestamp(self) -> str:
        """When the record was made, in UTC, ISO 8601 to the millisecond."""
        return datetime.fromtimestamp(self._record.created, UTC).isoformat(timespec="milliseconds")

    @property
    def message(self) -> str:
        """The record's message, cut at :data:`MAX_VALUE_LENGTH`."""
        return truncated(self._record.getMessage(), MAX_VALUE_LENGTH)

    def fields(self) -> dict[str, object]:
        """Every field but the stack, under its own name, as :func:`render_value` writes it."""
        return self._rendered(frozenset())

    def laid_out(self) -> dict[str, object]:
        """Every field but the stack under the name the JSON and text formats write it.

        A key those formats write from the record itself (:data:`RESERVED_KEYS`) is
        never taken from ``extra``: the field is written under ``loom.extra.<key>``.
        """
        return self._rendered(RESERVED_KEYS)

    def items(self) -> Iterator[tuple[str, object]]:
        """Each field but the stack under the key it was passed with, its value unrendered."""
        return (
            (key, value)
            for key, value in self._record.__dict__.items()
            if key not in _NOT_FIELDS and not str(key).startswith("_")
        )

    def logfmt(self, without: Collection[str] = ()) -> str:
        """The :meth:`laid_out` fields as logfmt ``key=value`` pairs, quoted where needed.

        Args:
            without: Names of the fields to leave out.
        """
        return " ".join(
            f"{key}={_logfmt_value(str(value))}"
            for key, value in self.laid_out().items()
            if key not in without
        )

    def _rendered(self, reserved: frozenset[str]) -> dict[str, object]:
        return {
            EXTRA_PREFIX + name if (name := str(key)) in reserved else name: render_value(value)
            for key, value in self.items()
        }

    @property
    def stack(self) -> str | None:
        """The stack the context filter wrote on the record, if any; each format writes it whole."""
        stack = self._record.__dict__.get(_STACKTRACE)
        return stack if isinstance(stack, str) else None


class JsonFormatter(logging.Formatter):
    """One JSON object per record, fields at the top level.

    The keys of Lambda's JSON format (``timestamp``, ``level``, ``message``,
    ``logger``) come first and cannot be overwritten by ``extra``. The formatter
    renders the values itself, so even on a handler without the context filter it
    writes no ``repr`` and no secret.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line."""
        fields = RecordFields(record)
        entry: dict[str, object] = {
            "timestamp": fields.timestamp,
            "level": record.levelname,
            "message": fields.message,
            "logger": record.name,
            **fields.laid_out(),
        }
        stack = fields.stack
        if stack is not None:
            entry[_STACKTRACE] = stack
        return json.dumps(entry)


_CONTINUATION: Final = "  | "

_NEEDS_QUOTES: Final = re.compile(r'[\s="]')


def _one_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


def _logfmt_value(text: str) -> str:
    """*text* bare, or quoted when a space, ``=`` or ``"`` in it could forge a field."""
    if _NEEDS_QUOTES.search(text) is None:
        return text
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{_one_line(escaped)}"'


class TextFormatter(logging.Formatter):
    """A readable line: UTC time, level, logger, the message, then logfmt ``key=value`` fields.

    A value holding a space, ``=`` or ``"`` is quoted, so it cannot forge a field; a
    line break in the message or in a value is written escaped, so it cannot forge a
    second record. The stack the context filter wrote follows the line, each of its
    rows prefixed so that the record's first line is the only unindented one.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as one line, its stack after it when it carries one."""
        fields = RecordFields(record)
        head = f"{fields.timestamp} {record.levelname:<7} {record.name}"
        line = f"{head} {_one_line(fields.message)}"
        pairs = fields.logfmt()
        if pairs:
            line = f"{line} {pairs}"
        stack = fields.stack
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
    """Loom's JSON line for the Lambda runtime's handler, ended by a line break as the runtime's.

    Args:
        json_frames: Whether the function uses the ``JSON`` log format, under which
            the runtime's handler reads the record's frame type; under ``Text`` it
            frames the line as text.
    """

    def __init__(self, *, json_frames: bool) -> None:
        super().__init__()
        self._json_frames = json_frames

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line, ended by a line break."""
        if self._json_frames:
            level = min(max(record.levelno, 0), logging.CRITICAL) // 10 * 10
            record.__dict__["_frame_type"] = _LAMBDA_JSON_FRAMES[level]
        return super().format(record) + "\n"
