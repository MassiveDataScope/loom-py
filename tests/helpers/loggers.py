"""A ``LoggerPort`` that records what it is told, for asserting on structured log events."""

from __future__ import annotations

from typing import Any

import pytest


class RecordingLogger:
    """Keeps each ``(event, fields)`` it logs at any level, in order; ``bind`` returns itself."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> RecordingLogger:
        del fields
        return self

    def _record(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    debug = info = warning = error = exception = _record

    def named(self, event: str) -> list[dict[str, Any]]:
        """The fields of every event logged exactly as *event*, in order."""
        return [fields for logged, fields in self.calls if logged == event]

    def fields_for(self, substr: str) -> dict[str, Any]:
        """The fields of the first event containing *substr*, or ``{}``."""
        for event, fields in self.calls:
            if substr in event:
                return fields
        return {}


class CapturedLog:
    """The structured events a logger put in a test's ``caplog``, as ``(event, fields)`` pairs.

    Args:
        caplog: The fixture that captured the records.
        logger: The name of the logger whose events are read.
    """

    def __init__(self, caplog: pytest.LogCaptureFixture, logger: str) -> None:
        self._caplog = caplog
        self._logger = logger

    @property
    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Each structured event logged so far, in order, with the fields it carried."""
        return [
            (record.msg["event"], {k: v for k, v in record.msg.items() if k != "event"})
            for record in self._caplog.records
            if record.name == self._logger and isinstance(record.msg, dict)
        ]

    def named(self, event: str) -> list[dict[str, Any]]:
        """The fields of every event logged exactly as *event*, in order."""
        return [fields for logged, fields in self.calls if logged == event]


__all__ = ["CapturedLog", "RecordingLogger"]
