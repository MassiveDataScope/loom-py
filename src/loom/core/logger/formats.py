"""The formatters loom ships: what a record looks like on the console or the collector."""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Collection
from datetime import UTC, datetime
from itertools import chain
from typing import Final, Literal

from loom.core.errors.summary import ErrorSummary
from loom.core.log_names import LogField
from loom.core.logger.context import current_log_fields
from loom.core.logger.identity import LogIdentity
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret
from loom.core.tracing.ids import TraceIds

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered message or text value; longer ones end in the marker."""

MAX_LIST_ITEMS: Final = 20
"""Items a list value keeps; a longer list is cut to its first items."""

TRUNCATION_MARKER: Final = "[truncated]"
"""What a cut text says on the side it lost, next to an ellipsis."""

_ELLIPSIS: Final = "..."

MAX_STACK_LENGTH: Final = 16_384
"""Characters of a written stack; a longer one keeps its end, where the failure is."""

_STACKTRACE: Final = LogField.EXCEPTION_STACKTRACE.value

_STACK_FORMATTER: Final = logging.Formatter()

_NOT_FIELDS: Final = frozenset(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
    _STACKTRACE,
}
"""The attributes the stdlib gives every record, and the stack: none of them is a field."""

_AS_IS: Final = frozenset({bool, int, type(None)})

_OPAQUE: Final = object()

EXTRA_PREFIX: Final = "loom.extra."
"""Prefix under which a field is written when its name is a key the JSON layout owns."""


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
    """One record as loom's outputs write it, derived when written; no context is stored on it.

    The fields are, later ones winning, where the call was made, the identity, the
    fields of the running :func:`~loom.core.logger.context.log_context`, what was passed
    as ``extra``, and the ids of the trace; the failure of ``exc_info`` replaces any
    ``extra`` of its names. Each output builds its own view in the thread that emitted
    the record, so every handler sees the record as it was logged.

    Args:
        record: The record to write.
        identity: The service and process the record comes from; none writes no identity.
    """

    __slots__ = ("_identity", "_record")

    def __init__(self, record: logging.LogRecord, identity: LogIdentity | None = None) -> None:
        self._record = record
        self._identity = identity

    @property
    def timestamp(self) -> str:
        """When the record was made, in UTC, ISO 8601 to the millisecond."""
        return datetime.fromtimestamp(self._record.created, UTC).isoformat(timespec="milliseconds")

    @property
    def message(self) -> str:
        """The record's message, cut at :data:`MAX_VALUE_LENGTH`."""
        return truncated(self._record.getMessage(), MAX_VALUE_LENGTH)

    def extras(self) -> dict[str, object]:
        """Each ``extra`` of the record under the key it was passed with, its value unrendered."""
        return {
            name: value
            for name, value in self._record.__dict__.items()
            if name not in _NOT_FIELDS and not str(name).startswith("_")
        }

    def fields(self) -> dict[str, object]:
        """Every field but the stack, under its own name, as :func:`render_value` writes it."""
        record = self._record
        derived: dict[str, object] = {
            LogField.CODE_FUNCTION: record.funcName,
            LogField.CODE_LINE: record.lineno,
            LogField.CODE_FILE: record.pathname,
        }
        if self._identity is not None:
            derived.update(self._identity.fields())
        derived.update(current_log_fields())
        extras = self.extras()
        derived.update(extras)
        passed = extras.get(LogField.TRACE_ID)
        derived.update(TraceIds.current(passed if isinstance(passed, str) else None).fields())
        if record.exc_info is not None and record.exc_info[1] is not None:
            derived.update(ErrorSummary.of(record.exc_info[1]).fields(LogField.EXCEPTION_TYPE))
        return {str(key): render_value(value) for key, value in derived.items()}

    def logfmt(self, without: Collection[str] = ()) -> str:
        """The :meth:`fields` as logfmt ``key=value`` pairs, quoted where needed.

        Args:
            without: Names of the fields to leave out.
        """
        return " ".join(
            f"{Logfmt.token(key)}={Logfmt.token(str(value))}"
            for key, value in self.fields().items()
            if key not in without
        )

    def body(self, without: Collection[str] = ()) -> str:
        """The message, its :meth:`logfmt` fields, then the stack, each of its rows prefixed.

        The message is written on one line, so only the first line of the body is
        unindented; a record's stack cannot forge a second record.

        Args:
            without: Names of the fields to leave out.
        """
        line = " ".join(filter(None, (Logfmt.one_line(self.message), self.logfmt(without=without))))
        stack = self.stack
        if stack is None:
            return line
        return "\n".join([line, *(f"{_CONTINUATION}{row}" for row in stack.splitlines())])

    @property
    def stack(self) -> str | None:
        """The traceback of ``exc_info`` from ERROR up, keeping its end when it is long.

        It is formatted once per record into the standard ``exc_text`` cache, which every
        handler reuses; an ``extra`` of its name is never written.
        """
        record = self._record
        exc_info = record.exc_info
        if exc_info is None or exc_info[1] is None or record.levelno < logging.ERROR:
            return None
        record.exc_text = record.exc_text or _STACK_FORMATTER.formatException(exc_info)
        return truncated(record.exc_text, MAX_STACK_LENGTH, keep="end")


class JsonFormatter(logging.Formatter):
    """One JSON object per record, fields at the top level.

    The keys of Lambda's JSON format (``timestamp``, ``level``, ``message``,
    ``logger``) come first and cannot be overwritten by a field of the same name,
    which is written under ``loom.extra.<key>``. The formatter
    derives every field itself, so used alone on any handler it writes the code origin,
    the running context, the trace ids, the failure's type and stack, and no ``repr``
    and no secret.

    Args:
        identity: The service and process to write on every record; none writes no identity.
    """

    def __init__(self, *, identity: LogIdentity | None = None) -> None:
        super().__init__()
        self._identity = identity

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line."""
        fields = RecordFields(record, self._identity)
        entry: dict[str, object] = {
            "timestamp": fields.timestamp,
            "level": record.levelname,
            "message": fields.message,
            "logger": record.name,
        }
        for name, value in fields.fields().items():
            entry[EXTRA_PREFIX + name if name in entry else name] = value
        stack = fields.stack
        if stack is not None:
            entry[_STACKTRACE] = stack
        return json.dumps(entry)


_CONTINUATION: Final = "  | "


class Logfmt:
    """A piece of text as the text formats write it, whichever side of ``=`` it is on.

    Control characters and line breaks are always written escaped, so no text
    can start a second record; a key or value holding a space, ``=``, ``"`` or a
    backslash is quoted, so it cannot forge or merge fields. Text that needs
    neither is returned as it is.
    """

    _CONTROLS: Final = "".join(map(chr, chain(range(0x20), range(0x7F, 0xA0), (0x2028, 0x2029))))
    _ONE_LINE: Final = {
        ord(control): f"\\x{ord(control):02x}" if ord(control) <= 0xFF else f"\\u{ord(control):04x}"
        for control in _CONTROLS
    } | {ord("\n"): "\\n", ord("\r"): "\\r", ord("\t"): "\\t"}
    _QUOTED: Final = _ONE_LINE | {ord("\\"): "\\\\", ord('"'): '\\"'}
    _HAS_CONTROL: Final = re.compile(f"[{re.escape(_CONTROLS)}]")
    _NEEDS_QUOTES: Final = re.compile(f'[\\s="\\\\{re.escape(_CONTROLS)}]')

    @classmethod
    def one_line(cls, text: str) -> str:
        """*text* with control characters and line breaks escaped, otherwise as it is."""
        if cls._HAS_CONTROL.search(text) is None:
            return text
        return text.translate(cls._ONE_LINE)

    @classmethod
    def token(cls, text: str) -> str:
        """*text* bare when it is a plain token, otherwise quoted and escaped."""
        if cls._NEEDS_QUOTES.search(text) is None:
            return text
        return f'"{text.translate(cls._QUOTED)}"'


class TextFormatter(logging.Formatter):
    """A readable line: UTC time, level, logger, the message, then logfmt ``key=value`` fields.

    A key or value holding a space, ``=``, ``"`` or a backslash is quoted, so it
    cannot forge a field; a line break or control character in the message, a key
    or a value is written escaped, so it cannot forge a second record. The stack of the
    record's exception follows the line, each of its rows prefixed so that the
    record's first line is the only unindented one.

    Args:
        identity: The service and process to write on every record; none writes no identity.
    """

    def __init__(self, *, identity: LogIdentity | None = None) -> None:
        super().__init__()
        self._identity = identity

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as one line, its stack after it when it carries one."""
        fields = RecordFields(record, self._identity)
        head = f"{fields.timestamp} {record.levelname:<7} {record.name}"
        return f"{head} {fields.body()}"


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
        identity: The service and process to write on every record.
    """

    def __init__(self, *, json_frames: bool, identity: LogIdentity | None = None) -> None:
        super().__init__(identity=identity)
        self._json_frames = json_frames

    def format(self, record: logging.LogRecord) -> str:
        """Render *record* as a JSON object on one line, ended by a line break."""
        if self._json_frames:
            level = min(max(record.levelno, 0), logging.CRITICAL) // 10 * 10
            record.__dict__["_frame_type"] = _LAMBDA_JSON_FRAMES[level]
        return super().format(record) + "\n"
