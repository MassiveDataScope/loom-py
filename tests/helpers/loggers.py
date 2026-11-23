"""Stdlib logging doubles: a record-keeping handler and a view of caplog by event and fields."""

from __future__ import annotations

import logging
import traceback
from typing import Any

import pytest

from loom.core.logger.formats import record_fields


class CapturingHandler(logging.Handler):
    """Keeps the records it is given, after the filters of the handler ran."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class LambdaLoggerHandler(CapturingHandler):
    """Stand-in for the Lambda runtime's handler (awslambdaric 4.2.3), named like it.

    As the runtime's framed handler does, it formats each record, then reads the
    frame type the record states. It starts with the runtime's Text layout.
    """

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(logging.Formatter("[%(levelname)s]\t%(message)s\n"))
        self.lines: list[str] = []
        self.frames: list[bytes | None] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))
        self.frames.append(getattr(record, "_frame_type", None))
        super().emit(record)


class CapturedLog:
    """The events a logger put in a test's ``caplog``, as ``(event, fields)`` pairs.

    The event is the record's message; the fields are what the call passed as ``extra``.

    Args:
        caplog: The fixture that captured the records.
        loggers: The names of the loggers whose events are read; every logger when none.
    """

    def __init__(self, caplog: pytest.LogCaptureFixture, *loggers: str) -> None:
        self._caplog = caplog
        self._loggers = loggers

    @property
    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Each event logged so far, in order, with the fields it carried."""
        return [
            (record.getMessage(), record_fields(record))
            for record in self._caplog.records
            if not self._loggers or record.name in self._loggers
        ]

    @property
    def messages(self) -> list[str]:
        """The event of each call so far, in order."""
        return [event for event, _ in self.calls]

    @property
    def errors(self) -> list[tuple[str, dict[str, Any]]]:
        """Each event logged at ERROR or above, in order, with the fields it carried."""
        return [
            (record.getMessage(), record_fields(record))
            for record in self._caplog.records
            if record.levelno >= logging.ERROR
            and (not self._loggers or record.name in self._loggers)
        ]

    def named(self, event: str) -> list[dict[str, Any]]:
        """The fields of every event logged exactly as *event*, in order."""
        return [fields for logged, fields in self.calls if logged == event]

    def fields_for(self, substr: str) -> dict[str, Any]:
        """The fields of the first event containing *substr*, or ``{}``."""
        for event, fields in self.calls:
            if substr in event:
                return fields
        return {}


def rendered(record: logging.LogRecord) -> str:
    """The message of *record* followed by its fields, to look for a value in one string."""
    return f"{record.getMessage()} {record_fields(record)}"


def logged_text(caplog: pytest.LogCaptureFixture) -> str:
    """Every record of *caplog* rendered by :func:`rendered`, one per line."""
    return "\n".join(rendered(record) for record in caplog.records)


def exception_text(caplog: pytest.LogCaptureFixture) -> str:
    """The traceback of every record of *caplog* that carries an exception, one after another."""
    return "".join(
        "".join(traceback.format_exception(*record.exc_info))
        for record in caplog.records
        if record.exc_info is not None and record.exc_info[0] is not None
    )


def logged_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    """Each record of *caplog* as ``{"event": message, **fields, "level": ...}``, in order."""
    return [
        {"event": record.getMessage(), "level": record.levelname.lower(), **record_fields(record)}
        for record in caplog.records
    ]


__all__ = [
    "CapturedLog",
    "CapturingHandler",
    "exception_text",
    "logged_events",
    "logged_text",
    "rendered",
]
