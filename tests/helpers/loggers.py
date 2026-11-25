"""Stdlib logging doubles: a handler that keeps records, a caplog view, the caller's key."""

from __future__ import annotations

import hmac
import logging
import traceback
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pytest

from loom.core.environment import Environment
from loom.core.log_names import SubKey
from loom.core.logger import subject as log_subject
from loom.core.logger.formats import RecordFields
from loom.core.logger.identity import LogIdentity
from loom.core.logger.install import configure_logging
from loom.core.logger.settings import QUIET_LIBRARIES, LogSettings
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


def configured(
    settings: LogSettings,
    *,
    environment: Environment = Environment.PRODUCTION,
    extra_handlers: Sequence[logging.Handler] = (),
    service_name: str | None = None,
    service_version: str | None = None,
) -> LogIdentity:
    """Configure logging as a process of *environment* and give back the identity it shares."""
    identity = LogIdentity(
        environment=environment, service_name=service_name, service_version=service_version
    )
    configure_logging(settings, identity, extra_handlers=extra_handlers)
    return identity


def install_subject_key() -> None:
    """Key the caller of security records with :data:`SUBJECT_KEY` until the test ends."""
    SubjectHasher(Secret(SUBJECT_KEY)).install()


class CapturingHandler(logging.Handler):
    """Keeps the records it is given, and each one's fields rendered as it was emitted.

    The fields and the stack are derived in the context that emitted the record, as an
    output of loom does; ``named`` reads the fields back by event.

    Args:
        identity: The identity the records are rendered with; none writes no identity.
    """

    def __init__(self, identity: LogIdentity | None = None) -> None:
        super().__init__()
        self.identity = identity
        self.records: list[logging.LogRecord] = []
        self.rendered: list[tuple[str, dict[str, object]]] = []
        self.stacks: list[str | None] = []

    def emit(self, record: logging.LogRecord) -> None:
        view = RecordFields(record, self.identity)
        self.records.append(record)
        self.rendered.append((record.getMessage(), view.fields()))
        self.stacks.append(view.stack)

    @property
    def fields(self) -> list[dict[str, object]]:
        """The rendered fields of every record, in order."""
        return [fields for _, fields in self.rendered]

    def named(self, event: str) -> list[dict[str, object]]:
        """The rendered fields of every record whose message is exactly *event*, in order."""
        return [fields for message, fields in self.rendered if message == event]


@pytest.fixture
def rendering() -> CapturingHandler:
    """A handler on the root logger that keeps what reaches it, its fields rendered."""
    handler = CapturingHandler()
    logging.getLogger().addHandler(handler)
    return handler


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
    def events(self) -> list[tuple[str, dict[str, Any]]]:
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
        return [event for event, _ in self.events]

    @property
    def errors(self) -> list[tuple[str, dict[str, Any]]]:
        """Each event logged at ERROR or above, in order, with the fields it carried."""
        return [
            (record.getMessage(), logged_fields(record))
            for record in self._records()
            if record.levelno >= logging.ERROR
        ]

    def levels(self, event: str) -> list[int]:
        """The level of every record logged exactly as *event*, in order."""
        return [record.levelno for record in self._records() if record.getMessage() == event]

    def named(self, event: str) -> list[dict[str, Any]]:
        """The fields of every event logged exactly as *event*, in order."""
        return [fields for logged, fields in self.events if logged == event]

    def rendered(self, event: str | None = None) -> list[str]:
        """Each record's event and fields as loom's formats lay them out; only those of *event*."""
        return [
            f"{record.getMessage()} {RecordFields(record).fields()}"
            for record in self._records()
            if event is None or record.getMessage() == event
        ]

    @property
    def rendered_text(self) -> str:
        """Every record as :meth:`rendered` lays it out, one per line."""
        return "\n".join(self.rendered())

    @property
    def exception_text(self) -> str:
        """The traceback of every record that carries an exception, one after another."""
        return "".join(
            "".join(traceback.format_exception(*record.exc_info))
            for record in self._records()
            if record.exc_info is not None and record.exc_info[0] is not None
        )


def logged_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Each field *record* carries as it is on the record, keys as text but a ``SubKey``."""
    return {
        (key if isinstance(key, SubKey) else str(key)): value
        for key, value in RecordFields(record).extras().items()
    }


@contextmanager
def root_logger_kept() -> Iterator[logging.Logger]:
    """Lend the root logger, then give back its level, handlers and quiet libraries."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    libraries = {name: logging.getLogger(name).level for name in QUIET_LIBRARIES}
    try:
        yield root
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for name, kept_level in libraries.items():
            logging.getLogger(name).setLevel(kept_level)


__all__ = [
    "SUBJECT_KEY",
    "CapturedLog",
    "CapturingHandler",
    "LambdaLoggerHandler",
    "configured",
    "install_subject_key",
    "keyless_subject_hasher",
    "logged_fields",
    "rendering",
    "root_logger_kept",
    "user_hash",
]
