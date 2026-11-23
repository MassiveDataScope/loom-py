"""The allow-list of modules still outside the logging standard only shrinks."""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loom.core.logger import LogEvent, LogField
from tests.helpers.log_standard import (
    _BREACHES,
    REPORT_DIRECTORY_VARIABLE,
    UNMIGRATED_MODULES,
    Breach,
    LogContract,
    foreign_keys,
    modules_with_foreign_extra,
    pytest_sessionfinish,
)


class _AcmeEvent(LogEvent):
    THING_DONE = "acme.thing_done"


def _record(
    msg: object = _AcmeEvent.THING_DONE, args: tuple[object, ...] = (), **extra: object
) -> logging.LogRecord:
    record = logging.makeLogRecord({"name": "loom.acme", "msg": msg, "args": args})
    record.__dict__.update(extra)
    return record


class TestForeignKeys:
    def test_log_field_keys_are_the_standard(self) -> None:
        assert foreign_keys(_record(**{LogField.JOB.value: "SendInvoice"})) == set()

    def test_any_other_key_is_foreign(self) -> None:
        assert foreign_keys(_record(amount=3, **{LogField.JOB.value: "x"})) == {"amount"}


class TestUnmigratedModules:
    def test_a_module_outside_the_standard_is_listed(self) -> None:
        missing = set(modules_with_foreign_extra()) - set(UNMIGRATED_MODULES)

        assert missing == set()

    def test_a_module_that_complies_is_removed_from_the_list(self) -> None:
        stale = set(UNMIGRATED_MODULES) - set(modules_with_foreign_extra())

        assert stale == set()


class TestBreach:
    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            pytest.param(_record(), [], id="event-with-log-fields"),
            pytest.param(_record("thing done"), [Breach.MESSAGE_NOT_EVENT], id="free-text"),
            pytest.param(
                _record("done %s", ("x",)),
                [Breach.MESSAGE_NOT_EVENT, Breach.MESSAGE_ARGUMENTS],
                id="percent-arguments",
            ),
            pytest.param(_record(amount=3), [Breach.FOREIGN_KEY], id="foreign-key"),
        ],
    )
    def test_a_record_breaks_the_rules_it_does_not_follow(
        self, record: logging.LogRecord, expected: list[Breach]
    ) -> None:
        assert Breach.broken_by(record) == expected


class TestLogContract:
    def test_counts_each_breach_under_its_module(self) -> None:
        breaches: Counter[str] = Counter()

        LogContract(breaches).emit(_record("thing done", amount=3))

        assert breaches == Counter({"loom.acme|message_not_event": 1, "loom.acme|foreign_key": 1})


class TestReport:
    @pytest.fixture
    def finish(self) -> Callable[[Counter[str]], None]:
        def run(breaches: Counter[str]) -> None:
            config = SimpleNamespace(stash={_BREACHES: breaches})
            pytest_sessionfinish(cast("pytest.Session", SimpleNamespace(config=config)))

        return run

    def test_writes_the_process_count_into_the_report_directory(
        self,
        finish: Callable[[Counter[str]], None],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(REPORT_DIRECTORY_VARIABLE, str(tmp_path))

        finish(Counter({"loom.acme|foreign_key": 2}))

        assert json.loads((tmp_path / f"{os.getpid()}.json").read_text()) == {
            "loom.acme|foreign_key": 2
        }

    def test_writes_nothing_when_no_record_broke_the_contract(
        self,
        finish: Callable[[Counter[str]], None],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(REPORT_DIRECTORY_VARIABLE, str(tmp_path))

        finish(Counter())

        assert list(tmp_path.iterdir()) == []

    def test_writes_nothing_without_a_report_directory(
        self,
        finish: Callable[[Counter[str]], None],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(REPORT_DIRECTORY_VARIABLE, raising=False)
        monkeypatch.chdir(tmp_path)

        finish(Counter({"loom.acme|foreign_key": 1}))

        assert list(tmp_path.iterdir()) == []
