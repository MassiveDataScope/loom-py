"""What loom says about a failure, and what leaves loom in its place.

:class:`Failure` is the one owner of every question about an exception: its
type, its code, its declared attributes, whether it is expected, the level that
reports it, what a client reads, its frames-only stack and its translation at
the loom boundary. Ask ``Failure(exc)``, never the exception.

The one mark, *stack written*, is set on an exception the first time a record
writes its stack: an outer scope, the boundary or a handler that sees a marked
exception writes its type only, and the stack of a later exception stops at it.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Iterator
from types import MappingProxyType
from typing import Final

from loom.core.errors.base import ClientViolation, LoomError, SystemError
from loom.core.errors.log_events import ErrorLogEvent, ErrorLogField

_log = logging.getLogger(__name__)

_STACK_WRITTEN: Final = "_loom_stack_written"
_CAUSE: Final = "\nThe above exception was the direct cause of the following exception:\n\n"
_CONTEXT: Final = "\nDuring handling of the above exception, another exception occurred:\n\n"
_TRACEBACK_HEADER: Final = "Traceback (most recent call last):\n"
_MEMBER_INDENT: Final = "  | "
MAX_GROUP_MEMBERS: Final = 15
"""Members of an exception group whose stack is written, as Python's own traceback does."""


class UnexpectedError(SystemError):
    """What a foreign exception becomes when it leaves loom: its type name, never its text.

    Retry policies that matched the foreign type match ``original_type``.
    """

    template = "unexpected {original_type}"
    attributes = MappingProxyType({"original_type": ErrorLogField.ORIGINAL_TYPE})

    @property
    def original_type(self) -> str | None:
        """The fully qualified type of the exception it replaced, or ``None`` once rebuilt."""
        value = self.log_fields().get(ErrorLogField.ORIGINAL_TYPE)
        return value if isinstance(value, str) else None


class Failure:
    """The view of one exception that answers every failure question.

    Args:
        exc: The exception it describes.
    """

    __slots__ = ("exc",)

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc

    @property
    def type(self) -> str:
        """The fully qualified type of the exception; a builtin keeps its bare name."""
        kind = type(self.exc)
        if kind.__module__ == "builtins":
            return kind.__qualname__
        return f"{kind.__module__}.{kind.__qualname__}"

    @property
    def code(self) -> str | None:
        """Its ``LoomError`` code, or ``None`` for any other exception."""
        return str(self.exc.code) if isinstance(self.exc, LoomError) else None

    @property
    def is_expected(self) -> bool:
        """Whether the exception declares itself an expected outcome, through ``expected``."""
        return getattr(self.exc, "expected", False) is True

    @property
    def is_loom_own(self) -> bool:
        """Whether loom raised it: a ``LoomError``, or a class loom declares.

        Until every loom exception is a ``LoomError`` (spec 019, L2c), a class
        declared under the ``loom`` package (``AgentRunError``, ``ConfigError``,
        a job signal) is loom's own too, and crosses the boundary as itself.
        """
        return isinstance(self.exc, LoomError) or type(self.exc).__module__.startswith("loom.")

    @property
    def is_stack_written(self) -> bool:
        """Whether a record already wrote the stack of the exception."""
        return getattr(self.exc, _STACK_WRITTEN, False) is True

    @property
    def should_report(self) -> bool:
        """Whether the next record about it writes its stack: unexpected and not yet written."""
        return not self.is_expected and not self.is_stack_written

    @property
    def level(self) -> int:
        """The level of the record that reports it ending some work.

        ``INFO`` for an expected failure, ``WARNING`` once its stack is written,
        ``ERROR`` for the first report of an unexpected one, which writes the stack.
        """
        if self.is_expected:
            return logging.INFO
        if self.is_stack_written:
            return logging.WARNING
        return logging.ERROR

    @property
    def text(self) -> str:
        """The text that may describe it anywhere: a ``LoomError``'s message, any other's type.

        A ``LoomError`` holds no free text once its class declares a template;
        any other exception's message is never read.
        """
        return str(self.exc) if isinstance(self.exc, LoomError) else self.type

    @property
    def client_message(self) -> str | None:
        """What a client reads about it, or ``None`` when it answers with its code only."""
        return self.exc.client_message if isinstance(self.exc, LoomError) else None

    @property
    def client_detail(self) -> tuple[ClientViolation, ...]:
        """The violations only a client reads; never logged."""
        return self.exc.client_detail if isinstance(self.exc, LoomError) else ()

    def fields(self, *, stacktrace: bool = False) -> dict[str, object]:
        """The fields that describe it: declared attributes, type, code, violation codes, stack.

        Args:
            stacktrace: Whether to include ``exception.stacktrace``.
        """
        fields: dict[str, object] = {}
        if isinstance(self.exc, LoomError):
            fields.update({str(key): value for key, value in self.exc.log_fields().items()})
        fields[ErrorLogField.EXCEPTION_TYPE.value] = self.type
        code = self.code
        if code is not None:
            fields[ErrorLogField.ERROR_CODE.value] = code
        violations = self.client_detail
        if violations:
            fields[ErrorLogField.VIOLATION_COUNT.value] = len(violations)
            fields[ErrorLogField.VIOLATION_CODES.value] = sorted({v.code for v in violations})
        if stacktrace:
            fields[ErrorLogField.EXCEPTION_STACKTRACE.value] = self.stacktrace()
        return fields

    def record_fields(self, level: int) -> dict[str, object]:
        """The fields of a record at *level*: the stack once, from ERROR up, if it should report.

        The record factory is the one caller, so it is the stack's one writer.
        """
        writes_stack = level >= logging.ERROR and self.should_report
        fields = self.fields(stacktrace=writes_stack)
        if writes_stack:
            self.mark_stack_written()
        return fields

    def mark_stack_written(self) -> None:
        """Mark the exception as described: later records write its type only."""
        BaseException.__setattr__(self.exc, _STACK_WRITTEN, True)

    def report(self) -> None:
        """Log it once, at ERROR with its stack, unless it is expected or already written."""
        if self.should_report:
            _log.error(ErrorLogEvent.FAILURE_LEFT_LOOM, exc_info=self.exc)

    def as_loom_error(self) -> Exception:
        """The exception that leaves loom in its place, cleaned of every foreign text.

        Loom's own exception (:attr:`is_loom_own`) is itself; an exception
        group of a single ``LoomError`` is that member; any other exception is
        an :class:`UnexpectedError` naming its type, which takes its
        stack-written mark. The chain and the notes are
        cleared, so nothing a framework prints carries foreign text.
        """
        leaving = self._replacement()
        if leaving is not self.exc and self.is_stack_written:
            Failure(leaving).mark_stack_written()
        leaving.__cause__ = None
        leaving.__context__ = None
        leaving.__suppress_context__ = True
        if hasattr(leaving, "__notes__"):
            del leaving.__notes__
        return leaving

    def translate(self) -> Exception:
        """What the loom boundary raises in its place.

        :meth:`report`, then :meth:`as_loom_error`, in that order: the stack is
        written while the chain is still whole, then the chain is cleaned.
        """
        self.report()
        return self.as_loom_error()

    def _replacement(self) -> Exception:
        exc = self.exc
        if isinstance(exc, BaseExceptionGroup):
            members = exc.exceptions
            if len(members) == 1 and isinstance(members[0], LoomError):
                return members[0]
            return UnexpectedError(original_type=self.type)
        if self.is_loom_own and isinstance(exc, Exception):
            return exc
        return UnexpectedError(original_type=self.type)

    def stacktrace(self) -> str:
        """Its stack and its chain's, oldest first, with frames and type names only.

        Every ``__cause__`` and ``__context__`` link is written as Python writes
        it, but with no foreign message and no note: a declared ``LoomError``
        shows its message, which holds no free text, any other link its type.
        A link whose stack is already written is its type alone and ends the
        chain. An exception group writes each member's frames and type, up to
        :data:`MAX_GROUP_MEMBERS`.
        """
        parts: list[str] = []
        for link, joint in reversed(self._chain()):
            if link.is_stack_written and link.exc is not self.exc:
                parts += (link.type, "\n", joint)
                continue
            parts += (link._frames(), link._heading(), "\n", *link._member_rows(), joint)
        return "".join(parts).rstrip("\n")

    def _heading(self) -> str:
        exc = self.exc
        if isinstance(exc, LoomError) and exc.is_declared:
            return f"{self.type}: {exc.message}"
        return self.type

    def _chain(self) -> list[tuple[Failure, str]]:
        """It and the exceptions behind it, newest first, each with what joins it to the newer.

        The walk stops after the first older link whose stack is already written.
        """
        links: list[tuple[Failure, str]] = []
        seen: set[int] = set()
        link: BaseException | None = self.exc
        joint = ""
        while link is not None and id(link) not in seen:
            seen.add(id(link))
            failure = Failure(link)
            links.append((failure, joint))
            if link is not self.exc and failure.is_stack_written:
                break
            if link.__cause__ is not None:
                link, joint = link.__cause__, _CAUSE
            elif link.__context__ is not None and not link.__suppress_context__:
                link, joint = link.__context__, _CONTEXT
            else:
                link = None
        return links

    def _frames(self) -> str:
        tb = self.exc.__traceback__
        if tb is None:
            return ""
        rows = (
            f'  File "{frame.f_code.co_filename}", line {line}, in {frame.f_code.co_name}\n'
            for frame, line in traceback.walk_tb(tb)
        )
        return _TRACEBACK_HEADER + "".join(rows)

    def _member_rows(self) -> Iterator[str]:
        if not isinstance(self.exc, BaseExceptionGroup):
            return
        members = self.exc.exceptions
        for member in members[:MAX_GROUP_MEMBERS]:
            for row in Failure(member).stacktrace().splitlines():
                yield f"{_MEMBER_INDENT}{row}\n"
        if len(members) > MAX_GROUP_MEMBERS:
            yield f"{_MEMBER_INDENT}and {len(members) - MAX_GROUP_MEMBERS} more exceptions\n"
