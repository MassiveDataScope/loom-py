"""Stdlib logging doubles: a handler that keeps records, a caplog view, the caller's key."""

from __future__ import annotations

import hmac
import logging
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from loom.core.log_names import SubKey
from loom.core.logger import subject as log_subject
from loom.core.logger.formats import RECORD_ATTRIBUTES, RecordFields
from loom.core.logger.subject import SubjectHasher
from loom.core.model.redaction import Secret

SUBJECT_KEY = "acme-subject-key"
"""The ``observability.log.subject_key`` of the tests that key the caller."""


def user_hash(subject: str) -> str:
    """The ``user.hash`` of *subject* under :data:`SUBJECT_KEY`, computed apart from loom."""
    subject_bytes = subject.encode("utf-8", "surrogatepass")
    return hmac.new(SUBJECT_KEY.encode(), subject_bytes, "sha256").hexdigest()[:32]


@pytest.fixture(autouse=True)
def keyless_subject_hasher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test keyless and already warned, as a process configured without a key."""
    monkeypatch.setattr(log_subject, "_installed", SubjectHasher())


def install_subject_key() -> None:
    """Key the caller of security records with :data:`SUBJECT_KEY` until the test ends."""
    SubjectHasher(Secret(SUBJECT_KEY)).install()


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
        logger: The name of the logger whose events are read; every logger when ``None``.
    """

    def __init__(self, caplog: pytest.LogCaptureFixture, logger: str | None = None) -> None:
        self._caplog = caplog
        self._logger = logger

    @property
    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Each event logged so far, in order, with the fields it carried."""
        return [(record.getMessage(), logged_fields(record)) for record in self._records()]

    def exception_types(self, event: str) -> list[type[BaseException] | None]:
        """The type of the ``exc_info`` each record logged exactly as *event* carries, in order."""
        return [
            record.exc_info[0] if record.exc_info else None
            for record in self._records()
            if record.getMessage() == event
        ]

    def _records(self) -> list[logging.LogRecord]:
        return [
            record
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
            (record.getMessage(), logged_fields(record))
            for record in self._records()
            if record.levelno >= logging.ERROR
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


def logged_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Each field *record* carries as it is on the record, keys as text but a ``SubKey``."""
    return {
        (key if isinstance(key, SubKey) else str(key)): value
        for key, value in record.__dict__.items()
        if key not in RECORD_ATTRIBUTES and not str(key).startswith("_")
    }


def rendered(record: logging.LogRecord) -> str:
    """The message of *record* followed by its fields, to look for a value in one string."""
    return f"{record.getMessage()} {logged_fields(record)}"


def logged_text(caplog: pytest.LogCaptureFixture) -> str:
    """Every record of *caplog* rendered by :func:`rendered`, one per line."""
    return "\n".join(rendered(record) for record in caplog.records)


def laid_out_text(caplog: pytest.LogCaptureFixture) -> str:
    """The fields of every record of *caplog* as loom's formats lay them out, stack left out."""
    return str([RecordFields(record).laid_out() for record in caplog.records])


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
        {"event": each.getMessage(), "level": each.levelname.lower(), **logged_fields(each)}
        for each in caplog.records
    ]


@contextmanager
def root_logger_kept() -> Iterator[logging.Logger]:
    """Lend the root logger, then give back its level, its handlers and their filters."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    filters = {handler: list(handler.filters) for handler in handlers}
    try:
        yield root
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for handler, kept in filters.items():
            handler.filters[:] = kept


__all__ = [
    "CapturedLog",
    "CapturingHandler",
    "exception_text",
    "logged_events",
    "logged_fields",
    "logged_text",
    "rendered",
    "root_logger_kept",
]
