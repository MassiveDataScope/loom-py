"""A ``LoggerPort`` that records what it is told, for asserting on structured log events."""

from __future__ import annotations

from typing import Any


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


__all__ = ["RecordingLogger"]
