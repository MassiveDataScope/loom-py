"""Failure policy: how loom describes an exception, when it logs one, and what leaves the process.

Loom's log is the source of truth for a failure. Its type, its code, the fields
it declares and its stack of frames are written there once; everything else
gets its :func:`public_form` or a :class:`FailureReference` to that record. An
exception's message is never a payload: no record, answer or handoff renders
``str(exc)``, ``repr(exc)`` or ``exc.args``.

The one mark, *stack written*, is set on the exception the first time a record
writes its stack: an outer scope, a handoff or a handler that sees a marked
exception writes its type only, and the stack of a later exception stops at it.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Iterator
from enum import StrEnum
from types import TracebackType
from typing import Final

from loom.core.errors.base import LoomError
from loom.core.logger.log_events import LoggerLogEvent
from loom.core.logger.values import render_value
from loom.core.logger.vocabulary import LogField
from loom.core.tracing.context import get_trace_id

_log = logging.getLogger(__name__)

_STACK_WRITTEN: Final = "_loom_stack_written"
_CAUSE: Final = "\nThe above exception was the direct cause of the following exception:\n\n"
_CONTEXT: Final = "\nDuring handling of the above exception, another exception occurred:\n\n"
_TRACEBACK_HEADER: Final = "Traceback (most recent call last):\n"
_MEMBER_INDENT: Final = "  | "
MAX_GROUP_MEMBERS: Final = 15
"""Members of an exception group whose stack is written, as Python's own traceback does."""


def is_expected(exc: BaseException) -> bool:
    """Whether *exc* declares itself an expected outcome, through its ``expected`` attribute."""
    return getattr(exc, "expected", False) is True


def is_stack_written(exc: BaseException) -> bool:
    """Whether a record already wrote the stack of *exc*."""
    return getattr(exc, _STACK_WRITTEN, False) is True


def _mark_stack_written(exc: BaseException) -> None:
    BaseException.__setattr__(exc, _STACK_WRITTEN, True)


def should_report(exc: BaseException) -> bool:
    """Whether a handoff must log *exc* first: it is unexpected and its stack is not written."""
    return not is_expected(exc) and not is_stack_written(exc)


def failure_level(exc: BaseException) -> int:
    """The level of the record that reports *exc* ending some work.

    ``INFO`` for an expected failure, ``WARNING`` once its stack is written,
    ``ERROR`` for the first report of an unexpected one, which writes the stack.
    """
    if is_expected(exc):
        return logging.INFO
    if is_stack_written(exc):
        return logging.WARNING
    return logging.ERROR


def exception_type(kind: type[BaseException]) -> str:
    """The fully qualified name of an exception class; a builtin keeps its bare name."""
    if kind.__module__ == "builtins":
        return kind.__qualname__
    return f"{kind.__module__}.{kind.__qualname__}"


def _type_of(exc: BaseException) -> str:
    if isinstance(exc, FailureReference):
        return exc.exception_type
    return exception_type(type(exc))


def _code_of(exc: BaseException) -> str | None:
    if isinstance(exc, LoomError):
        return str(exc.code)
    if isinstance(exc, FailureReference):
        return exc.code
    return None


def _declared_fields(exc: BaseException) -> dict[str, object]:
    if not isinstance(exc, LoomError):
        return {}
    return {str(key): render_value(value) for key, value in exc.log_fields().items()}


def public_form(exc: BaseException) -> str:
    """What may leave the process to describe *exc*: never its message.

    A ``LoomError`` is its code and the fields it declares (``code key=value``);
    a :class:`FailureReference` its own text; any other exception its fully
    qualified type name.
    """
    if isinstance(exc, FailureReference):
        return str(exc)
    if not isinstance(exc, LoomError):
        return exception_type(type(exc))
    fields = "".join(f" {key}={value}" for key, value in _declared_fields(exc).items())
    return f"{exc.code}{fields}"


def _chain(exc: BaseException) -> list[tuple[BaseException, str]]:
    """*exc* and the exceptions behind it, newest first, each with what joins it to the newer.

    The walk stops after the first older link whose stack is already written.
    """
    links: list[tuple[BaseException, str]] = []
    seen: set[int] = set()
    link: BaseException | None = exc
    joint = ""
    while link is not None and id(link) not in seen:
        seen.add(id(link))
        links.append((link, joint))
        if link is not exc and is_stack_written(link):
            break
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


def _members(group: BaseException) -> Iterator[str]:
    """The stack of each member of an exception group, its rows indented."""
    if not isinstance(group, BaseExceptionGroup):
        return
    members = group.exceptions
    for member in members[:MAX_GROUP_MEMBERS]:
        for row in exception_stacktrace(member).splitlines():
            yield f"{_MEMBER_INDENT}{row}\n"
    if len(members) > MAX_GROUP_MEMBERS:
        yield f"{_MEMBER_INDENT}and {len(members) - MAX_GROUP_MEMBERS} more exceptions\n"


def exception_stacktrace(exc: BaseException) -> str:
    """The stack of *exc* and its chain, oldest first, with frames and type names only.

    Every ``__cause__`` and ``__context__`` link is written as Python writes it,
    but with no message and no note: text an application or a driver fills with
    values. A link whose stack is already written is its type alone and ends
    the chain. An exception group writes each member's frames and type.
    """
    parts: list[str] = []
    for link, joint in reversed(_chain(exc)):
        if link is not exc and is_stack_written(link):
            parts += (_type_of(link), "\n", joint)
            continue
        parts += (_frames(link.__traceback__), _type_of(link), "\n", *_members(link), joint)
    return "".join(parts).rstrip("\n")


def failure_fields(exc: BaseException, *, stacktrace: bool) -> dict[str, object]:
    """The fields that describe *exc*: its type, its code, the fields it declares, its stack.

    Args:
        exc: The exception.
        stacktrace: Whether to include ``exception.stacktrace``.
    """
    fields = _declared_fields(exc)
    fields[LogField.EXCEPTION_TYPE.value] = _type_of(exc)
    code = _code_of(exc)
    if code is not None:
        fields[LogField.ERROR_CODE.value] = code
    if stacktrace:
        fields[LogField.EXCEPTION_STACKTRACE.value] = exception_stacktrace(exc)
    return fields


def record_failure_fields(exc: BaseException, level: int) -> dict[str, object]:
    """The failure fields of a record at *level*: the stack once, from ERROR up, if unexpected."""
    writes_stack = level >= logging.ERROR and not is_stack_written(exc) and not is_expected(exc)
    fields = failure_fields(exc, stacktrace=writes_stack)
    if writes_stack:
        _mark_stack_written(exc)
    return fields


class FailureReference(Exception):
    """What leaves the process in place of a failure: its public form and the trace of its record.

    It holds no attribute that refers to the original exception, so nothing of
    it reaches a result backend, a pickle or an event. Every argument is a
    scalar, so the reference pickles and serialises as it is.

    Args:
        exception_type: The fully qualified type of the failure.
        code: Its ``LoomError`` code, or ``None``.
        trace_id: The trace of the record that describes it.
        expected: Whether the failure was an expected outcome.
        retryable: Whether running the work again could succeed.
    """

    def __init__(
        self,
        exception_type: str,
        code: str | None,
        trace_id: str | None,
        expected: bool,
        retryable: bool,
    ) -> None:
        super().__init__(exception_type, code, trace_id, expected, retryable)
        self.exception_type = exception_type
        self.code = code
        self.trace_id = trace_id
        self.expected = expected
        self.retryable = retryable

    def __str__(self) -> str:
        parts = [self.exception_type]
        if self.code is not None:
            parts.append(f"code={self.code}")
        if self.trace_id is not None:
            parts.append(f"trace_id={self.trace_id}")
        return " ".join(parts)


def reference(exc: BaseException) -> FailureReference:
    """The reference that leaves the process in place of *exc*, logging it first if it must.

    It logs ``logger.failure_handed_off`` when :func:`should_report` says so,
    and is idempotent: the reference of a reference is itself. The reference is
    born with its stack written, so a record that later sees it writes its type
    only. Build it inside the ``except`` and raise it after the block, so it
    has no ``__context__``.
    """
    if isinstance(exc, FailureReference):
        return exc
    if should_report(exc):
        _log.log(failure_level(exc), LoggerLogEvent.FAILURE_HANDED_OFF, exc_info=exc)
    handed_off = FailureReference(
        _type_of(exc),
        _code_of(exc),
        get_trace_id(),
        is_expected(exc),
        getattr(exc, "is_retryable", True) is True,
    )
    _mark_stack_written(handed_off)
    return handed_off


class Handoff(StrEnum):
    """Base of a runtime adapter's list of handoffs: every place a failure leaves the process.

    A handoff is a wire answer, a broker message, a result store, a runtime or
    a model provider. Each adapter declares its handoffs once, as members of a
    subclass, beside the code that hands off; the behavioural canary drives
    every member, so a handoff with no proof fails a test.
    """
