"""Record creation: a record's context and exception become fields once, where it is created.

:class:`LoomRecordFactory` wraps the process's record factory. It runs for
every record any logger creates, before any handler and in the emitting thread,
where the context variables are right. It stamps the context into one private
attribute, turns the exception into fields through
:class:`~loom.core.errors.failure.Failure`, writing its frames-only stack once:
the factory is the stack's one writer. An exception passed as the message or
as an argument becomes the text ``Failure`` allows. It hands the record on with no
``exc_info`` and no ``stack_info``, so no handler, formatter, queue or runtime
can render an exception message.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, MutableMapping
from types import TracebackType
from typing import Any, Final, Self

from loom.core.errors.failure import Failure
from loom.core.logger.context import current_log_fields
from loom.core.logger.stamp import LogIdentity, LogRuntime
from loom.core.tracing.context import get_trace_id
from loom.core.vocabulary import LogField

STAMP: Final = "_loom"
"""Attribute of a record that holds its :class:`RecordStamp` until a :class:`LogFilter` reads it."""

WARNINGS_LOGGER: Final = "py.warnings"
"""The logger ``logging.captureWarnings`` writes a warning to."""

TraceIds = Callable[[], tuple[str | None, str | None]]
"""Reads the trace id and the span id of the running work, either ``None`` when unknown."""

_ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None]

_WARNING_CATEGORY: Final = re.compile(r":\d+: ([A-Za-z_][\w.]*): ")
_UNKNOWN_CATEGORY: Final = "Warning"


def loom_trace_ids() -> tuple[str | None, str | None]:
    """The trace id of loom's tracing context, with no span id."""
    return get_trace_id(), None


class RecordStamp:
    """What a record carries from where it was created, until a filter writes it as fields.

    Args:
        identity: The emitter's fields, shared by every record.
        context: The rendered :func:`~loom.core.logger.log_context` fields, shared by the block.
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
        failure: Mapping[str, object] | None,
    ) -> None:
        self.identity = identity
        self.context = context
        self.trace_id = trace_id
        self.span_id = span_id
        self.failure = failure

    def write_into(self, fields: MutableMapping[str, Any], record: logging.LogRecord) -> None:
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
            fields.setdefault(key, value)
        for key, value in self.identity.items():
            fields.setdefault(key, value)
        fields.setdefault(LogField.CODE_FUNCTION.value, record.funcName)
        fields.setdefault(LogField.CODE_LINE.value, record.lineno)
        if self.failure is not None:
            fields.update(self.failure)


class LoomRecordFactory:
    """The record factory loom chains in front of the process's own.

    Every record is created by the factory it wraps, with no ``exc_info`` and no
    ``stack_info``, an exception in its message or arguments as the text ``Failure`` allows
    and a captured warning as its category alone, then stamped with a
    :class:`RecordStamp`. A record below a logger's level is never created, so
    it costs nothing here.

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

    @classmethod
    def install(cls, identity: LogIdentity, trace_ids: TraceIds = loom_trace_ids) -> Self:
        """Chain a factory for *identity* in front of the process's factory, and return it.

        A second call replaces the first one's factory instead of stacking on it.
        """
        current = logging.getLogRecordFactory()
        previous = current.previous if isinstance(current, LoomRecordFactory) else current
        factory = cls(previous, identity, trace_ids)
        logging.setLogRecordFactory(factory)
        return factory

    @property
    def previous(self) -> Callable[..., logging.LogRecord]:
        """The factory this one wraps."""
        return self._previous

    @staticmethod
    def _safe(value: object) -> object:
        """*value*, or the text :class:`Failure` allows if it is an exception."""
        return Failure(value).text if isinstance(value, BaseException) else value

    @staticmethod
    def _safe_argument(value: object) -> object:
        if isinstance(value, Mapping):
            return {key: LoomRecordFactory._safe(item) for key, item in value.items()}
        return LoomRecordFactory._safe(value)

    @staticmethod
    def _warning_category(msg: object, args: Any) -> str:
        """The category of the warning ``logging.captureWarnings`` formatted into the record.

        The standard library logs the formatted warning as the message; an older
        one, or another caller, as the one argument of ``"%s"``.
        """
        text = args[0] if isinstance(args, tuple) and args else msg
        found = _WARNING_CATEGORY.search(text) if isinstance(text, str) else None
        return _UNKNOWN_CATEGORY if found is None else found.group(1)

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
        """Create the record safe, then stamp it."""
        if name == WARNINGS_LOGGER:
            msg, args = "%s", (self._warning_category(msg, args),)
        elif isinstance(msg, BaseException):
            msg = Failure(msg).text
        if args:
            args = tuple(self._safe_argument(item) for item in args)
        record = self._previous(name, level, fn, lno, msg, args, None, func, None, **kwargs)
        failure = None
        if exc_info is not None and exc_info[1] is not None:
            failure = Failure(exc_info[1]).record_fields(level)
        trace_id, span_id = self._trace_ids()
        record.__dict__[STAMP] = RecordStamp(
            self._identity_fields, current_log_fields(), trace_id, span_id, failure
        )
        return record
