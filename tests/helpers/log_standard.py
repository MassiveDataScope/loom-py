"""The logging contract, enforced at every level on every ``loom.*`` logger.

Each rule is a :class:`Breach`; a record that breaks one fails its test. Records of
any other logger are never checked.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from enum import StrEnum
from typing import Final, TypeVar

import pytest

from loom.core.log_names import FailureForm, LogEvent, LogField, LogKey, SubKey, ValueKind
from loom.core.logger.formats import TextFormatter, render_value
from tests.helpers.loggers import logged_fields

_Name = TypeVar("_Name", LogEvent, LogKey)

_QUOTED_TEXT_MIN: Final = 8
"""Characters of exception text that count as quoted inside a longer field value."""


def loom_members(root: type[_Name]) -> list[_Name]:
    """The members of every subclass of *root* that loom declares, once imported.

    A subclass defined outside ``loom`` (a test's own) never counts.
    """
    pending, found = [root], []
    while pending:
        base = pending.pop()
        pending.extend(base.__subclasses__())
        if base.__module__.startswith("loom."):
            found.extend(base)
    return found


class Breach(StrEnum):
    """A rule of the log contract a record can break."""

    MESSAGE_NOT_EVENT = "message_not_event"
    MESSAGE_ARGUMENTS = "message_arguments"
    UNDECLARED_KEY = "undeclared_key"
    OPAQUE_VALUE = "opaque_value"
    WRONG_KIND = "wrong_kind"
    EXCEPTION_TEXT_IN_FIELD = "exception_text_in_field"
    EXCEPTION_TYPE_WITHOUT_STACK = "exception_type_without_stack"

    @classmethod
    def broken_by(cls, record: logging.LogRecord) -> list[Breach]:
        """The rules *record* breaks."""
        keys = loom_members(LogKey)
        declared = {key.value: key.kind for key in keys}
        bags = tuple(f"{key.value}." for key in keys if key.kind is ValueKind.OBJECT)
        fields = logged_fields(record)
        failure = record.exc_info[1] if record.exc_info else None
        text = str(failure) if failure else ""
        quoting = len(text) >= _QUOTED_TEXT_MIN
        checks = {
            cls.MESSAGE_NOT_EVENT: not isinstance(record.msg, LogEvent),
            cls.MESSAGE_ARGUMENTS: bool(record.args),
            cls.UNDECLARED_KEY: any(
                key not in declared and not (isinstance(key, SubKey) and key.startswith(bags))
                for key in fields
            ),
            cls.OPAQUE_VALUE: any(
                render_value(value) == type(value).__name__ and not isinstance(value, str)
                for value in fields.values()
            ),
            cls.WRONG_KIND: any(
                (kind := ValueKind.of(render_value(value))) is not None
                and declared.get(key, kind) != kind
                for key, value in fields.items()
            ),
            cls.EXCEPTION_TEXT_IN_FIELD: bool(text)
            and any(
                isinstance(value, str) and (value == text or (quoting and text in value))
                for value in fields.values()
            ),
            cls.EXCEPTION_TYPE_WITHOUT_STACK: LogField.EXCEPTION_TYPE in fields
            and failure is None
            and not (
                isinstance(record.msg, LogEvent)
                and record.msg.failure_form is FailureForm.SUMMARY_ONLY
            ),
        }
        return [breach for breach, broken in checks.items() if broken]


class LogContract(logging.Handler):
    """Keeps the records of one test with the contract rules each one breaks."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[tuple[logging.LogRecord, list[Breach]]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((record, Breach.broken_by(record)))

    def offences(self) -> list[str]:
        """One line per distinct record that broke the contract."""
        return sorted(
            {
                f"{record.name}: {record.msg} breaks {[str(breach) for breach in broken]}"
                for record, broken in self.records
                if broken
            }
        )


@pytest.fixture(autouse=True)
def loom_records_follow_the_contract() -> Iterator[None]:
    """Fail a test when a record of a ``loom`` logger, at any level, breaks the contract."""
    seen = LogContract()
    loom_logger = logging.getLogger("loom")
    level = loom_logger.level
    loom_logger.setLevel(logging.DEBUG)
    loom_logger.addHandler(seen)
    yield
    loom_logger.removeHandler(seen)
    loom_logger.setLevel(level)
    if offences := seen.offences():
        pytest.fail("records outside the log contract:\n" + "\n".join(offences))


@pytest.fixture(autouse=True)
def caplog_shows_fields(request: pytest.FixtureRequest) -> None:
    """Make ``caplog.text`` show each record with its fields, laid out as the text format does.

    Values stay raw: caplog's handler carries no ``ContextFilter``.
    """
    if "caplog" in request.fixturenames:
        request.getfixturevalue("caplog").handler.setFormatter(TextFormatter())
