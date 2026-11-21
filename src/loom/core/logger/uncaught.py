"""Uncaught exceptions: one record through loom, under the same policy as any other.

The interpreter, a thread, an unraisable exception and an asyncio loop each
print an uncaught exception with its message by default. These hooks write one
record instead, whose exception the record factory turns into its type, code
and frames; a warning is captured as a record of its category. A runtime entry
point installs them (Lambda init, the ASGI
lifespan, Celery worker init, the CLI), never ``configure_logging``, so a test
runner keeps its own; the asyncio handler is set by ``Application.running``.
"""

from __future__ import annotations

import logging
import sys
import threading
from asyncio import AbstractEventLoop
from collections.abc import Mapping
from types import TracebackType
from typing import Any

from loom.core.logger.failure import failure_level
from loom.core.logger.log_events import LoggerLogEvent, LoggerLogField

_log = logging.getLogger(__name__)


def log_uncaught_exception(
    kind: type[BaseException], exc: BaseException, tb: TracebackType | None
) -> None:
    """``sys.excepthook``: log the exception that ends the interpreter; Ctrl-C prints as usual."""
    if issubclass(kind, KeyboardInterrupt):
        sys.__excepthook__(kind, exc, tb)
        return
    _log.log(failure_level(exc), LoggerLogEvent.UNCAUGHT_EXCEPTION, exc_info=(kind, exc, tb))


def log_uncaught_thread_exception(args: threading.ExceptHookArgs) -> None:
    """``threading.excepthook``: log the exception that ends a thread; ``SystemExit`` is silent."""
    if args.exc_value is None or issubclass(args.exc_type, SystemExit):
        return
    _log.log(
        failure_level(args.exc_value),
        LoggerLogEvent.UNCAUGHT_EXCEPTION,
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        extra={LoggerLogField.THREAD_NAME: None if args.thread is None else args.thread.name},
    )


def log_asyncio_exception(loop: AbstractEventLoop, context: Mapping[str, Any]) -> None:
    """An asyncio exception handler: log the exception, never the context's message text."""
    del loop
    exc = context.get("exception")
    if not isinstance(exc, BaseException):
        _log.error(LoggerLogEvent.UNCAUGHT_EXCEPTION)
        return
    _log.log(failure_level(exc), LoggerLogEvent.UNCAUGHT_EXCEPTION, exc_info=exc)


def log_unraisable_exception(args: Any) -> None:
    """``sys.unraisablehook``: log an exception Python could not raise, never its message."""
    exc = args.exc_value
    if exc is None:
        return
    _log.log(failure_level(exc), LoggerLogEvent.UNRAISABLE_EXCEPTION, exc_info=exc)


def install_process_hooks() -> None:
    """Make the interpreter, every thread, unraisable exceptions and warnings report through loom.

    A warning becomes a ``py.warnings`` record that carries its category only.
    """
    sys.excepthook = log_uncaught_exception
    threading.excepthook = log_uncaught_thread_exception
    sys.unraisablehook = log_unraisable_exception
    logging.captureWarnings(True)
