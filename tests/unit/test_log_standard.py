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

from loom.core.log_names import LogEvent, LogField, SubKey
from loom.core.model.redaction import Secret
from tests.helpers.log_standard import (
    _BREACHES,
    REPORT_DIRECTORY_VARIABLE,
    Breach,
    LogContract,
    ValueKind,
    pytest_sessionfinish,
)

ATTEMPT = LogField.ATTEMPT.value
JOB = LogField.JOB.value
SUBKEY = SubKey("loom.acme.bag.tenant")
RecordFactory = Callable[..., logging.LogRecord]
KINDS = {ATTEMPT: ValueKind.NUMBER, SUBKEY: "int"}


class _AcmeEvent(LogEvent):
    THING_DONE = "acme.thing_done"


@pytest.fixture
def record() -> RecordFactory:
    def build(
        msg: object = _AcmeEvent.THING_DONE,
        args: tuple[object, ...] = (),
        fields: dict[str, object] | None = None,
    ) -> logging.LogRecord:
        return logging.makeLogRecord(
            {"name": "loom.acme", "msg": msg, "args": args, **(fields or {})}
        )

    return build


@pytest.fixture
def breaches() -> Counter[str]:
    return Counter()


class TestBreach:
    @pytest.mark.parametrize(
        ("built", "expected"),
        [
            pytest.param({}, [], id="event-alone"),
            pytest.param({"fields": {ATTEMPT: 3, JOB: "x"}}, [], id="declared-keys"),
            pytest.param({"msg": "thing done"}, [Breach.MESSAGE_NOT_EVENT], id="free-text"),
            pytest.param(
                {"msg": "done %s", "args": ("x",)},
                [Breach.MESSAGE_NOT_EVENT, Breach.MESSAGE_ARGUMENTS],
                id="percent-arguments",
            ),
            pytest.param({"fields": {"amount": 3}}, [Breach.UNDECLARED_KEY], id="undeclared-key"),
            pytest.param({"fields": {JOB: object()}}, [Breach.OPAQUE_VALUE], id="opaque-value"),
            pytest.param({"fields": {JOB: ["a", "b"]}}, [], id="list-of-scalars"),
            pytest.param({"fields": {ATTEMPT: "3"}}, [Breach.KEY_RETYPED], id="key-retyped"),
            pytest.param({"fields": {ATTEMPT: None}}, [], id="missing-value"),
            pytest.param({"fields": {SUBKEY: object()}}, [], id="subkey"),
        ],
    )
    def test_a_record_breaks_the_rules_it_does_not_follow(
        self, record: RecordFactory, built: dict[str, object], expected: list[Breach]
    ) -> None:
        assert Breach.broken_by(record(**built), KINDS) == expected


class TestLogContract:
    def test_counts_each_breach_under_its_module(
        self, record: RecordFactory, breaches: Counter[str]
    ) -> None:
        LogContract(breaches, {}).emit(record("thing done", fields={"amount": 3}))

        assert breaches == Counter(
            {"loom.acme|message_not_event": 1, "loom.acme|undeclared_key": 1}
        )

    @pytest.mark.parametrize(
        ("first", "second", "retyped"),
        [
            pytest.param(1, 12.5, False, id="int-then-float"),
            pytest.param(12.5, 1, False, id="float-then-int"),
            pytest.param(Secret("a"), "b", False, id="secret-is-a-string"),
            pytest.param("3", 3, True, id="string-then-int"),
            pytest.param(True, 1, True, id="boolean-then-int"),
            pytest.param("a", ["a"], True, id="string-then-array"),
        ],
    )
    def test_a_key_changes_kind_only_across_json_kinds(
        self,
        record: RecordFactory,
        breaches: Counter[str],
        first: object,
        second: object,
        retyped: bool,
    ) -> None:
        contract = LogContract(breaches, {})

        contract.emit(record(fields={ATTEMPT: first}))
        contract.emit(record(fields={ATTEMPT: second}))

        assert breaches == (Counter({"loom.acme|key_retyped": 1}) if retyped else Counter())

    def test_a_text_record_does_not_teach_a_key_its_kind(
        self, record: RecordFactory, breaches: Counter[str]
    ) -> None:
        contract = LogContract(breaches, {})

        contract.emit(record("thing done", fields={LogField.NAME.value: 7}))
        contract.emit(record(fields={LogField.NAME.value: "x"}))

        assert "loom.acme|key_retyped" not in breaches

    def test_a_record_of_a_non_loom_logger_never_reaches_it(self, breaches: Counter[str]) -> None:
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
    def report_directory(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        monkeypatch.setenv(REPORT_DIRECTORY_VARIABLE, str(tmp_path))
        return tmp_path

    @pytest.fixture
    def finish(self, report_directory: Path) -> Callable[[Counter[str]], None]:
        def run(breaches: Counter[str]) -> None:
            session = SimpleNamespace(config=SimpleNamespace(stash={_BREACHES: breaches}))
            pytest_sessionfinish(cast("pytest.Session", session))

        return run

    def test_writes_the_process_count_into_the_report_directory(
        self, finish: Callable[[Counter[str]], None], report_directory: Path
    ) -> None:
        finish(Counter({"loom.acme|undeclared_key": 2}))

        report = report_directory / f"{os.getpid()}.json"
        assert json.loads(report.read_text()) == {"loom.acme|undeclared_key": 2}

    def test_writes_nothing_when_no_record_broke_the_contract(
        self, finish: Callable[[Counter[str]], None], report_directory: Path
    ) -> None:
        finish(Counter())

        assert list(report_directory.iterdir()) == []

    def test_writes_nothing_without_a_report_directory(
        self,
        finish: Callable[[Counter[str]], None],
        report_directory: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(REPORT_DIRECTORY_VARIABLE)
        monkeypatch.chdir(report_directory)

        finish(Counter({"loom.acme|undeclared_key": 1}))

        assert list(report_directory.iterdir()) == []
