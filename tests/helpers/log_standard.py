"""The logging contract, enforced across the whole suite on every ``loom.*`` logger.

A record of a ``loom`` logger must follow it: a :class:`~loom.core.log_names.LogEvent`
as message, no ``%`` arguments, every field key declared by a
:class:`~loom.core.log_names.LogKey` (or made by :meth:`~loom.core.log_names.LogKey.of`),
every value one the formats can write, and one kind of value per key. A record
that breaks it fails its test. Records of any other logger are never checked.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from enum import StrEnum
from typing import Final, TypeVar

import pytest

from loom.core.log_names import LogEvent, LogKey, SubKey
from loom.core.logger.formats import RecordFields, TextFormatter, render_value

_Name = TypeVar("_Name", LogEvent, LogKey)

_KEY_KINDS: Final = pytest.StashKey[dict[str, "ValueKind"]]()


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


class ValueKind(StrEnum):
    """The JSON kind a formatter writes for a field value."""

    BOOLEAN = "boolean"
    NUMBER = "number"
    STRING = "string"
    ARRAY = "array"

    @classmethod
    def of(cls, value: object) -> ValueKind | None:
        """The kind of ``render_value(value)``; ``None`` for a missing value."""
        rendered = render_value(value)
        if rendered is None:
            return None
        if isinstance(rendered, bool):
            return cls.BOOLEAN
        if isinstance(rendered, int | float):
            return cls.NUMBER
        if isinstance(rendered, list):
            return cls.ARRAY
        return cls.STRING


class Breach(StrEnum):
    """A rule of the log contract a record can break."""

    MESSAGE_NOT_EVENT = "message_not_event"
    MESSAGE_ARGUMENTS = "message_arguments"
    UNDECLARED_KEY = "undeclared_key"
    OPAQUE_VALUE = "opaque_value"
    KEY_RETYPED = "key_retyped"

    @classmethod
    def broken_by(cls, record: logging.LogRecord, kinds: dict[str, ValueKind]) -> list[Breach]:
        """The rules *record* breaks, given the kind of value each key carried so far."""
        declared = {key.value for key in loom_members(LogKey)}
        fields = {
            key: value
            for key, value in RecordFields(record).raw.items()
            if not isinstance(key, SubKey)
        }
        checks = {
            cls.MESSAGE_NOT_EVENT: not isinstance(record.msg, LogEvent),
            cls.MESSAGE_ARGUMENTS: bool(record.args),
            cls.UNDECLARED_KEY: any(key not in declared for key in fields),
            cls.OPAQUE_VALUE: any(
                render_value(value) == type(value).__name__ and not isinstance(value, str)
                for value in fields.values()
            ),
            cls.KEY_RETYPED: any(
                (kind := ValueKind.of(value)) is not None and kinds.get(key, kind) != kind
                for key, value in fields.items()
            ),
        }
        return [breach for breach, broken in checks.items() if broken]


class LogContract(logging.Handler):
    """Keeps the records of one test with the contract rules each one breaks."""

    def __init__(self, kinds: dict[str, ValueKind]) -> None:
        super().__init__()
        self.records: list[tuple[logging.LogRecord, list[Breach]]] = []
        self._kinds = kinds

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((record, Breach.broken_by(record, self._kinds)))
        for key, value in RecordFields(record).raw.items():
            kind = ValueKind.of(value)
            if kind is not None and not isinstance(key, SubKey):
                self._kinds.setdefault(key, kind)

    def offences(self) -> list[str]:
        """One line per distinct record that broke the contract."""
        return sorted(
            {
                f"{record.name}: {record.msg} breaks {[str(breach) for breach in broken]}"
                for record, broken in self.records
                if broken
            }
        )


def pytest_configure(config: pytest.Config) -> None:
    """Start the session's record of the kind each key carries."""
    config.stash[_KEY_KINDS] = {}


@pytest.fixture(autouse=True)
def loom_records_follow_the_contract(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test when a record of a ``loom`` logger breaks the contract."""
    seen = LogContract(request.config.stash[_KEY_KINDS])
    loom_logger = logging.getLogger("loom")
    loom_logger.addHandler(seen)
    yield
    loom_logger.removeHandler(seen)
    if offences := seen.offences():
        pytest.fail("records outside the log contract:\n" + "\n".join(offences))


@pytest.fixture(autouse=True)
def caplog_shows_fields(request: pytest.FixtureRequest) -> None:
    """Make ``caplog.text`` show each record with its fields, laid out as the text format does.

    Values stay raw: caplog's handler carries no ``ContextFilter``.
    """
    if "caplog" in request.fixturenames:
        request.getfixturevalue("caplog").handler.setFormatter(TextFormatter())
