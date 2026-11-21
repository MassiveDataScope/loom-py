"""Uncaught exceptions: one ERROR record through loom, under the same policy as any other.

The interpreter, a thread and an asyncio loop each print an uncaught exception
with its message by default. These hooks write one record instead, whose
exception the record factory turns into its type, code and frames.
"""

from __future__ import annotations

import logging
import sys
import threading
from asyncio import AbstractEventLoop
from collections.abc import Mapping
from types import TracebackType
from typing import Any

from loom.core.logger.log_events import LoggerLogEvent, LoggerLogField

_log = logging.getLogger(__name__)


def log_uncaught_exception(
    kind: type[BaseException], exc: BaseException, tb: TracebackType | None
) -> None:
    """``sys.excepthook``: log the exception that ends the interpreter; Ctrl-C prints as usual."""
    if issubclass(kind, KeyboardInterrupt):
        sys.__excepthook__(kind, exc, tb)
        return
    _log.error(LoggerLogEvent.UNCAUGHT_EXCEPTION, exc_info=(kind, exc, tb))


def log_uncaught_thread_exception(args: threading.ExceptHookArgs) -> None:
    """``threading.excepthook``: log the exception that ends a thread; ``SystemExit`` is silent."""
    if args.exc_value is None or issubclass(args.exc_type, SystemExit):
        return
    _log.error(
        LoggerLogEvent.UNCAUGHT_EXCEPTION,
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        extra={LoggerLogField.THREAD_NAME: None if args.thread is None else args.thread.name},
    )


def log_asyncio_exception(loop: AbstractEventLoop, context: Mapping[str, Any]) -> None:
    """An asyncio exception handler: log the exception, never the context's message text."""
    del loop
    exc = context.get("exception")
    _log.error(
        LoggerLogEvent.UNCAUGHT_EXCEPTION,
        exc_info=exc if isinstance(exc, BaseException) else None,
    )


def install_uncaught_hooks() -> None:
    """Make the interpreter and every thread report an uncaught exception through loom."""
    sys.excepthook = log_uncaught_exception
    threading.excepthook = log_uncaught_thread_exception
