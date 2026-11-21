"""Record creation: a record's context and exception become fields once, where it is created.

:class:`LoomRecordFactory` wraps the process's record factory. It runs for
every record any logger creates, before any handler and in the emitting thread,
where the context variables are right. It stamps the context into one private
attribute and turns the exception into fields, then hands the record on with no
``exc_info`` and no ``stack_info``, so no handler, formatter, queue or runtime
can render an exception message.

The exception policy lives here: :func:`failure_fields` says what describes an
exception (its type, its code, its stack of frames and type names), and the
factory decides when the stack is written: from ERROR up, once per exception.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable, Mapping, MutableMapping
from contextlib import suppress
from types import TracebackType
from typing import Any, Final

from loom.core.errors.base import LoomError
from loom.core.logger.context import current_log_fields
from loom.core.logger.stamp import LogIdentity, LogRuntime
from loom.core.logger.vocabulary import LogField
from loom.core.tracing.context import get_trace_id

STAMP: Final = "_loom"
"""Attribute of a record that holds its :class:`RecordStamp` until a :class:`LogFilter` reads it."""

TraceIds = Callable[[], tuple[str | None, str | None]]
"""Reads the trace id and the span id of the running work, either ``None`` when unknown."""

_ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None]

_CAUSE: Final = "\nThe above exception was the direct cause of the following exception:\n\n"
_CONTEXT: Final = "\nDuring handling of the above exception, another exception occurred:\n\n"
_TRACEBACK_HEADER: Final = "Traceback (most recent call last):\n"

_STACK_WRITTEN: Final = "_loom_stack_written"


def is_logged(exc: BaseException) -> bool:
    """Whether a record already wrote the stack of *exc*: the one "already logged" mark.

    A scope or a boundary that sees a marked exception writes its own fact with
    the type only, and a handler that would only repeat it logs nothing. The
    mark is an attribute of the exception, which every exception can hold: a
    built-in exception cannot be weakly referenced, so no weak set can.
    """
    return getattr(exc, _STACK_WRITTEN, False) is True


def _mark_logged(exc: BaseException) -> None:
    with suppress(AttributeError):
        setattr(exc, _STACK_WRITTEN, True)


def is_expected(exc: BaseException) -> bool:
    """Whether *exc* declares itself an expected outcome, as :attr:`LoomError.expected` does."""
    return getattr(exc, "expected", False) is True


def failure_level(exc: BaseException) -> int:
    """The level of the record that reports *exc* ending some work.

    ``INFO`` for an expected failure, ``WARNING`` once its stack is written,
    ``ERROR`` for the first report of an unexpected one, which writes the stack.
    """
    if is_expected(exc):
        return logging.INFO
    if is_logged(exc):
        return logging.WARNING
    return logging.ERROR


def exception_type(kind: type[BaseException]) -> str:
    """The fully qualified name of an exception class; a builtin keeps its bare name."""
    if kind.__module__ == "builtins":
        return kind.__qualname__
    return f"{kind.__module__}.{kind.__qualname__}"


def _chain(exc: BaseException) -> list[tuple[BaseException, str]]:
    """*exc* and the exceptions behind it, newest first, each with what joins it to the newer."""
    links: list[tuple[BaseException, str]] = []
    seen: set[int] = set()
    link: BaseException | None = exc
    joint = ""
    while link is not None and id(link) not in seen:
        seen.add(id(link))
        links.append((link, joint))
        if link.__cause__ is not None:
            link, joint = link.__cause__, _CAUSE
        elif link.__context__ is not None and not link.__suppress_context__:
            link, joint = link.__context__, _CONTEXT
        else:
            link = None
    return links


def _frames(tb: TracebackType | None) -> str:
    if tb is None:
        return ""
    rows = (
        f'  File "{frame.f_code.co_filename}", line {line}, in {frame.f_code.co_name}\n'
        for frame, line in traceback.walk_tb(tb)
    )
    return _TRACEBACK_HEADER + "".join(rows)


def exception_stacktrace(exc: BaseException) -> str:
    """The stack of *exc* and its chain, oldest first, with frames and type names only.

    Every ``__cause__`` and ``__context__`` link is written as Python writes it,
    but with no message, no note and no member of an exception group: those are
    text an application or a driver fills with values.
    """
    parts: list[str] = []
    for link, joint in reversed(_chain(exc)):
        parts += (_frames(link.__traceback__), exception_type(type(link)), "\n", joint)
    return "".join(parts).rstrip("\n")


def failure_fields(exc: BaseException, *, stacktrace: bool) -> dict[str, str]:
    """The fields that describe *exc*: its type, its code for a ``LoomError``, and its stack.

    Args:
        exc: The exception.
        stacktrace: Whether to include ``exception.stacktrace``.
    """
    fields = {LogField.EXCEPTION_TYPE.value: exception_type(type(exc))}
    if isinstance(exc, LoomError):
        fields[LogField.ERROR_CODE.value] = str(exc.code)
    if stacktrace:
        fields[LogField.EXCEPTION_STACKTRACE.value] = exception_stacktrace(exc)
    return fields


def _record_failure(exc: BaseException, level: int) -> dict[str, str]:
    """The failure fields of a record at *level*; the stack is written once, from ERROR up."""
    writes_stack = level >= logging.ERROR and not is_logged(exc)
    if writes_stack:
        _mark_logged(exc)
    return failure_fields(exc, stacktrace=writes_stack)


def loom_trace_ids() -> tuple[str | None, str | None]:
    """The trace id of loom's tracing context, with no span id."""
    return get_trace_id(), None


class RecordStamp:
    """What a record carries from where it was created, until a filter writes it as fields.

    Args:
        identity: The emitter's fields, shared by every record.
        context: The :func:`~loom.core.logger.log_context` fields, shared by the block.
        trace_id: The trace of the running work.
        span_id: The span of the running work, when a span is recording.
        failure: The fields of the record's exception, if it carried one.
    """

    __slots__ = ("context", "failure", "identity", "span_id", "trace_id")

    def __init__(
        self,
        identity: Mapping[str, str],
        context: Mapping[str, object],
        trace_id: str | None,
        span_id: str | None,
        failure: Mapping[str, str] | None,
    ) -> None:
        self.identity = identity
        self.context = context
        self.trace_id = trace_id
        self.span_id = span_id
        self.failure = failure

    def write_into(
        self,
        fields: MutableMapping[str, Any],
        record: logging.LogRecord,
        render: Callable[[object], object],
    ) -> None:
        """Write the stamp into *fields*, the record's own attributes.

        A field already there, passed as ``extra``, wins over the trace, the
        context, the identity and the code origin, in that order. The failure
        fields win over everything, so ``extra`` cannot forge them.
        """
        if self.trace_id is not None:
            fields.setdefault(LogField.TRACE_ID.value, self.trace_id)
        if self.span_id is not None:
            fields.setdefault(LogField.SPAN_ID.value, self.span_id)
        for key, value in self.context.items():
            if key not in fields:
                fields[key] = render(value)
        for key, value in self.identity.items():
            fields.setdefault(key, value)
        fields.setdefault(LogField.CODE_FUNCTION.value, record.funcName)
        fields.setdefault(LogField.CODE_LINE.value, record.lineno)
        if self.failure is not None:
            fields.update(self.failure)


class LoomRecordFactory:
    """The record factory loom chains in front of the process's own.

    Every record is created by the factory it wraps, with no ``exc_info`` and no
    ``stack_info``, then stamped with a :class:`RecordStamp`. A record below a
    logger's level is never created, so it costs nothing here.

    Args:
        previous: The factory it wraps.
        identity: Who emits the records.
        trace_ids: Reads the ids of the running work.
    """

    __slots__ = ("_identity", "_identity_fields", "_previous", "_trace_ids")

    def __init__(
        self,
        previous: Callable[..., logging.LogRecord],
        identity: LogIdentity,
        trace_ids: TraceIds = loom_trace_ids,
    ) -> None:
        self._previous = previous
        self._identity = identity
        self._identity_fields = identity.fields()
        self._trace_ids = trace_ids

    @property
    def previous(self) -> Callable[..., logging.LogRecord]:
        """The factory this one wraps."""
        return self._previous

    def running_as(self, runtime: LogRuntime) -> None:
        """Stamp the following records as emitted by *runtime*."""
        self._identity = self._identity.with_runtime(runtime)
        self._identity_fields = self._identity.fields()

    def __call__(
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: object,
        args: Any,
        exc_info: _ExcInfo | None,
        func: str | None = None,
        sinfo: str | None = None,
        **kwargs: Any,
    ) -> logging.LogRecord:
        """Create the record, stamp it, and drop its exception and stack info."""
        record = self._previous(name, level, fn, lno, msg, args, None, func, None, **kwargs)
        failure = None
        if exc_info is not None and exc_info[1] is not None:
            failure = _record_failure(exc_info[1], level)
        trace_id, span_id = self._trace_ids()
        record.__dict__[STAMP] = RecordStamp(
            self._identity_fields, current_log_fields(), trace_id, span_id, failure
        )
        return record
