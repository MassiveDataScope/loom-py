"""The log contract: which records break it, and that a test that breaks it fails."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from loom.core.log_names import LogEvent, LogField, SubKey
from loom.core.model.redaction import Secret
from tests.helpers.log_standard import Breach, LogContract, ValueKind

ATTEMPT = LogField.ATTEMPT.value
JOB = LogField.JOB.value
SUBKEY = SubKey("loom.acme.bag.tenant")
RecordFactory = Callable[..., logging.LogRecord]
KINDS: dict[str, ValueKind] = {ATTEMPT: ValueKind.NUMBER, SUBKEY: ValueKind.NUMBER}


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
    def test_lists_each_record_that_breaks_the_contract_once(self, record: RecordFactory) -> None:
        contract = LogContract({})

        contract.emit(record("thing done", fields={"amount": 3}))
        contract.emit(record("thing done", fields={"amount": 3}))
        contract.emit(record())

        assert contract.offences() == [
            "loom.acme: thing done breaks ['message_not_event', 'undeclared_key']"
        ]

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
        self, record: RecordFactory, first: object, second: object, retyped: bool
    ) -> None:
        contract = LogContract({})

        contract.emit(record(fields={ATTEMPT: first}))
        contract.emit(record(fields={ATTEMPT: second}))

        assert bool(contract.offences()) is retyped


class TestStrictFixture:
    def test_a_loom_record_outside_the_contract_fails_the_test(
        self, pytester: pytest.Pytester
    ) -> None:
        repository = Path(__file__).parents[2]
        pytester.makeconftest(
            f"import sys\nsys.path[:0] = [{str(repository)!r}, {str(repository / 'src')!r}]\n"
            'pytest_plugins = ["tests.helpers.log_standard"]'
        )
        pytester.makepyfile(
            """
            import logging

            def test_free_text():
                logging.getLogger("loom.acme").warning("thing done")

            def test_other_logger():
                logging.getLogger("acme").warning("thing done")
            """
        )

        result = pytester.runpytest_subprocess("-p", "no:cacheprovider")

        result.assert_outcomes(passed=2, errors=1)
