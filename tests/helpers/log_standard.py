"""The logging contract, enforced across the whole suite on the ``loom`` logger.

A record whose message is a :class:`~loom.core.log_names.LogEvent` must follow
it: no ``%`` arguments, every field key declared by a
:class:`~loom.core.log_names.LogKey` (or made by :meth:`~loom.core.log_names.LogKey.of`),
every value one the formats can write, and one kind of value per key. Such a
record that breaks it fails its test. A record with a text message is only
counted: with ``LOOM_LOG_CONTRACT_REPORT`` set to a directory, each pytest
process writes the breaches it saw there as ``<pid>.json``.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from collections.abc import Iterator
from enum import StrEnum
from pathlib import Path
from typing import Final, TypeVar

import pytest

from loom.core.log_names import LogEvent, LogKey, SubKey
from loom.core.logger.formats import TextFormatter, is_opaque, record_fields, render_value

_Name = TypeVar("_Name", LogEvent, LogKey)

REPORT_DIRECTORY_VARIABLE: Final = "LOOM_LOG_CONTRACT_REPORT"
_BREACHES: Final = pytest.StashKey[Counter[str]]()

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
            for key, value in record_fields(record).items()
            if not isinstance(key, SubKey)
        }
        checks = {
            cls.MESSAGE_NOT_EVENT: not isinstance(record.msg, LogEvent),
            cls.MESSAGE_ARGUMENTS: bool(record.args),
            cls.UNDECLARED_KEY: any(key not in declared for key in fields),
            cls.OPAQUE_VALUE: any(is_opaque(value) for value in fields.values()),
            cls.KEY_RETYPED: any(
                (kind := ValueKind.of(value)) is not None and kinds.get(key, kind) != kind
                for key, value in fields.items()
            ),
        }
        return [breach for breach, broken in checks.items() if broken]


class LogContract(logging.Handler):
    """Keeps the records of one test and counts every breach by module."""

    def __init__(self, breaches: Counter[str], kinds: dict[str, ValueKind]) -> None:
        super().__init__()
        self.records: list[tuple[logging.LogRecord, list[Breach]]] = []
        self._breaches = breaches
        self._kinds = kinds

    def emit(self, record: logging.LogRecord) -> None:
        broken = Breach.broken_by(record, self._kinds)
        self.records.append((record, broken))
        self._breaches.update(f"{record.name}|{breach}" for breach in broken)
        if not isinstance(record.msg, LogEvent):
            return
        for key, value in record_fields(record).items():
            kind = ValueKind.of(value)
            if kind is not None and not isinstance(key, SubKey):
                self._kinds.setdefault(key, kind)


def pytest_configure(config: pytest.Config) -> None:
    """Start the session's breach count."""
    config.stash[_BREACHES] = Counter()
    config.stash[_KEY_KINDS] = {}


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Write this process's breach count when a report directory is set."""
    directory = os.environ.get(REPORT_DIRECTORY_VARIABLE)
    breaches = session.config.stash[_BREACHES]
    if not directory or not breaches:
        return
    target = Path(directory) / f"{os.getpid()}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dict(breaches), sort_keys=True))


@pytest.fixture(autouse=True)
def loom_events_follow_the_contract(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test when a ``loom`` record whose message is a ``LogEvent`` breaks the contract."""
    seen = LogContract(request.config.stash[_BREACHES], request.config.stash[_KEY_KINDS])
    loom_logger = logging.getLogger("loom")
    loom_logger.addHandler(seen)
    yield
    loom_logger.removeHandler(seen)
    offences = sorted(
        {
            f"{record.name}: {record.msg} breaks {[str(breach) for breach in broken]}"
            for record, broken in seen.records
            if isinstance(record.msg, LogEvent) and broken
        }
    )
    if offences:
        pytest.fail("records outside the log contract:\n" + "\n".join(offences))


@pytest.fixture(autouse=True)
def caplog_shows_fields(request: pytest.FixtureRequest) -> None:
    """Make ``caplog.text`` show each record with its fields, laid out as the text format does.

    Values stay raw: caplog's handler carries no ``ContextFilter``.
    """
    if "caplog" in request.fixturenames:
        request.getfixturevalue("caplog").handler.setFormatter(TextFormatter())
