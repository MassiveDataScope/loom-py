"""The log contract: which records break it, and what it counts and reports."""

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

from loom.core.log_names import LogEvent, LogField, LogKey
from tests.helpers.log_standard import (
    _BREACHES,
    REPORT_DIRECTORY_VARIABLE,
    Breach,
    LogContract,
    pytest_sessionfinish,
)


class _AcmeEvent(LogEvent):
    THING_DONE = "acme.thing_done"


class _AcmeKey(LogKey):
    AMOUNT = "loom.acme.amount"
    BAG = "loom.acme.bag"


def _record(
    msg: object = _AcmeEvent.THING_DONE,
    args: tuple[object, ...] = (),
    name: str = "loom.acme",
    **extra: object,
) -> logging.LogRecord:
    record = logging.makeLogRecord({"name": name, "msg": msg, "args": args})
    record.__dict__.update(extra)
    return record


class TestBreach:
    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            pytest.param(_record(), [], id="event-alone"),
            pytest.param(
                _record(**{_AcmeKey.AMOUNT.value: 3, LogField.JOB.value: "x"}),
                [],
                id="declared-keys",
            ),
            pytest.param(_record("thing done"), [Breach.MESSAGE_NOT_EVENT], id="free-text"),
            pytest.param(
                _record("done %s", ("x",)),
                [Breach.MESSAGE_NOT_EVENT, Breach.MESSAGE_ARGUMENTS],
                id="percent-arguments",
            ),
            pytest.param(_record(amount=3), [Breach.UNDECLARED_KEY], id="undeclared-key"),
            pytest.param(
                _record(**{_AcmeKey.AMOUNT.value: object()}),
                [Breach.OPAQUE_VALUE],
                id="opaque-value",
            ),
            pytest.param(_record(**{_AcmeKey.AMOUNT.value: ["a", "b"]}), [], id="list-of-scalars"),
        ],
    )
    def test_a_record_breaks_the_rules_it_does_not_follow(
        self, record: logging.LogRecord, expected: list[Breach]
    ) -> None:
        assert Breach.broken_by(record, {}) == expected

    def test_a_key_carrying_another_kind_of_value_is_retyped(self) -> None:
        record = _record(**{_AcmeKey.AMOUNT.value: "3"})

        assert Breach.broken_by(record, {_AcmeKey.AMOUNT.value: "int"}) == [Breach.KEY_RETYPED]

    def test_a_missing_value_does_not_retype_a_key(self) -> None:
        record = _record(**{_AcmeKey.AMOUNT.value: None})

        assert Breach.broken_by(record, {_AcmeKey.AMOUNT.value: "int"}) == []

    def test_a_subkey_is_never_checked(self) -> None:
        record = _record()
        record.__dict__[_AcmeKey.BAG.of("tenant")] = object()

        assert Breach.broken_by(record, {_AcmeKey.BAG.of("tenant"): "int"}) == []


class TestLogContract:
    def test_counts_each_breach_under_its_module(self) -> None:
        breaches: Counter[str] = Counter()

        LogContract(breaches, {}).emit(_record("thing done", amount=3))

        assert breaches == Counter(
            {"loom.acme|message_not_event": 1, "loom.acme|undeclared_key": 1}
        )

    def test_a_key_keeps_the_kind_of_its_first_value(self) -> None:
        breaches: Counter[str] = Counter()
        contract = LogContract(breaches, {})

        contract.emit(_record(**{_AcmeKey.AMOUNT.value: 3}))
        contract.emit(_record(**{_AcmeKey.AMOUNT.value: "3"}))

        assert breaches == Counter({"loom.acme|key_retyped": 1})

    def test_a_record_of_a_non_loom_logger_never_reaches_it(self) -> None:
        breaches: Counter[str] = Counter()
        handler = LogContract(breaches, {})
        loom_logger = logging.getLogger("loom")
        loom_logger.addHandler(handler)
        try:
            logging.getLogger("acme.other").warning("free text %s", "x", extra={"amount": 3})
        finally:
            loom_logger.removeHandler(handler)

        assert breaches == Counter()


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

        finish(Counter({"loom.acme|undeclared_key": 2}))

        assert json.loads((tmp_path / f"{os.getpid()}.json").read_text()) == {
            "loom.acme|undeclared_key": 2
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

        finish(Counter({"loom.acme|undeclared_key": 1}))

        assert list(tmp_path.iterdir()) == []
