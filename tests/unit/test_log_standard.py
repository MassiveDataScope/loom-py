"""The log contract: which records break it, and that a test that breaks it fails."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from loom.core.checks import ChecksKey
from loom.core.log_names import FailureForm, LogEvent, LogField, SubKey
from loom.core.model.redaction import Secret
from loom.core.observability.observer.log import LifecycleKey
from tests.helpers.log_standard import Breach, LogContract

ATTEMPT = LogField.ATTEMPT.value
JOB = LogField.JOB.value
EXCEPTION_TYPE = LogField.EXCEPTION_TYPE.value
SUBKEY = LifecycleKey.META.of("tenant")
RecordFactory = Callable[..., logging.LogRecord]
FAILURE = (ValueError, ValueError("card 4111"), None)


class _AcmeEvent(LogEvent):
    THING_DONE = "acme.thing_done"
    THING_REFUSED = "acme.thing_refused", FailureForm.SUMMARY_ONLY


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
            pytest.param({"fields": {ChecksKey.IDS: ["a", "b"]}}, [], id="list-of-scalars"),
            pytest.param({"fields": {ATTEMPT: 12.5}}, [], id="float-is-a-number"),
            pytest.param({"fields": {JOB: Secret("a")}}, [], id="secret-is-a-string"),
            pytest.param({"fields": {ATTEMPT: "3"}}, [Breach.WRONG_KIND], id="text-for-number"),
            pytest.param({"fields": {ATTEMPT: True}}, [Breach.WRONG_KIND], id="boolean-for-number"),
            pytest.param({"fields": {JOB: ["a"]}}, [Breach.WRONG_KIND], id="array-for-text"),
            pytest.param(
                {"fields": {LifecycleKey.META: "x"}}, [Breach.WRONG_KIND], id="bag-as-value"
            ),
            pytest.param({"fields": {ATTEMPT: None}}, [], id="missing-value"),
            pytest.param(
                {"fields": {SubKey(f"{JOB}.tenant"): 3}},
                [Breach.UNDECLARED_KEY],
                id="subkey-of-a-value-key",
            ),
            pytest.param({"fields": {SUBKEY: object()}}, [Breach.OPAQUE_VALUE], id="opaque-subkey"),
            pytest.param(
                {"fields": {EXCEPTION_TYPE: "ValueError"}},
                [Breach.EXCEPTION_TYPE_WITHOUT_STACK],
                id="exception-type-without-its-exception",
            ),
            pytest.param(
                {"msg": _AcmeEvent.THING_REFUSED, "fields": {EXCEPTION_TYPE: "ValueError"}},
                [],
                id="summary-only-event",
            ),
            pytest.param(
                {"fields": {JOB: "card 4111 refused", "exc_info": FAILURE}},
                [Breach.EXCEPTION_TEXT_IN_FIELD],
                id="exception-text-in-field",
            ),
            pytest.param(
                {
                    "fields": {
                        JOB: "billing-job",
                        "exc_info": (ValueError, ValueError("bill"), None),
                    }
                },
                [],
                id="short-text-inside-a-field",
            ),
        ],
    )
    def test_a_record_breaks_the_rules_it_does_not_follow(
        self, record: RecordFactory, built: dict[str, object], expected: list[Breach]
    ) -> None:
        assert Breach.broken_by(record(**built)) == expected


class TestLogContract:
    def test_lists_each_record_that_breaks_the_contract_once(self, record: RecordFactory) -> None:
        contract = LogContract()

        contract.emit(record("thing done", fields={"amount": 3}))
        contract.emit(record("thing done", fields={"amount": 3}))
        contract.emit(record())

        assert contract.offences() == [
            "loom.acme: thing done breaks ['message_not_event', 'undeclared_key']"
        ]


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

            def test_free_text_at_debug():
                logging.getLogger("loom.acme").debug("thing done")

            def test_other_logger():
                logging.getLogger("acme").warning("thing done")
            """
        )

        result = pytester.runpytest_subprocess("-p", "no:cacheprovider")

        result.assert_outcomes(passed=3, errors=2)
