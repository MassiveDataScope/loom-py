"""Stdlib logging doubles: a record-keeping handler and a view of caplog by event and fields."""

from __future__ import annotations

import logging
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


class CapturedLog:
    """The events a logger put in a test's ``caplog``, as ``(event, fields)`` pairs.

    The event is the record's message; the fields are what the call passed as ``extra``.

    Args:
        caplog: The fixture that captured the records.
        logger: The name of the logger whose events are read; every logger when ``None``.
    """

    def __init__(self, caplog: pytest.LogCaptureFixture, logger: str | None = None) -> None:
        self._caplog = caplog
        self._logger = logger

    @property
    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Each event logged so far, in order, with the fields it carried."""
        return [
            (record.getMessage(), record_fields(record))
            for record in self._caplog.records
            if self._logger is None or record.name == self._logger
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
            and (self._logger is None or record.name == self._logger)
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


def logged_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    """Each record of *caplog* as ``{"event": message, **fields, "level": ...}``, in order."""
    return [
        {"event": record.getMessage(), "level": record.levelname.lower(), **record_fields(record)}
        for record in caplog.records
    ]


__all__ = ["CapturedLog", "CapturingHandler", "logged_events", "logged_text", "rendered"]
