"""The log contract: which records break it, and that a test that breaks it fails."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import partial
from pathlib import Path

import pytest

from loom.core.checks import ChecksKey
from loom.core.log_names import LogEvent, LogField
from loom.core.logger import log_context
from loom.core.model.redaction import Secret
from loom.core.observability.observer.log import LifecycleKey
from tests.helpers.log_standard import Breach, LogContract
from tests.helpers.loggers import a_record

ATTEMPT = LogField.ATTEMPT
JOB = LogField.JOB
SUBKEY = LifecycleKey.META.of("tenant")
RecordFactory = Callable[..., logging.LogRecord]


class _AcmeEvent(LogEvent):
    THING_DONE = "acme.thing_done"


@pytest.fixture
def record() -> RecordFactory:
    return partial(a_record, msg=_AcmeEvent.THING_DONE, name="loom.acme")


class TestBreach:
    @pytest.mark.parametrize(
        ("built", "expected"),
        [
            pytest.param({}, [], id="event-alone"),
            pytest.param({ATTEMPT: 3, JOB: "x"}, [], id="declared-keys"),
            pytest.param({"msg": "thing done"}, [Breach.MESSAGE_NOT_EVENT], id="free-text"),
            pytest.param(
                {"msg": "done %s", "args": ("x",)},
                [Breach.MESSAGE_NOT_EVENT, Breach.MESSAGE_ARGUMENTS],
                id="percent-arguments",
            ),
            pytest.param({"amount": 3}, [Breach.UNDECLARED_KEY], id="undeclared-key"),
            pytest.param({JOB.value: "x"}, [Breach.UNDECLARED_KEY], id="declared-key-as-text"),
            pytest.param({JOB: object()}, [Breach.OPAQUE_VALUE], id="opaque-value"),
            pytest.param({ChecksKey.IDS: ["a", "b"]}, [], id="list-of-scalars"),
            pytest.param({ATTEMPT: 12.5}, [], id="float-is-a-number"),
            pytest.param({JOB: Secret("a")}, [], id="secret-is-a-string"),
            pytest.param({ATTEMPT: "3"}, [Breach.WRONG_KIND], id="text-for-number"),
            pytest.param({ATTEMPT: True}, [Breach.WRONG_KIND], id="boolean-for-number"),
            pytest.param({JOB: ["a"]}, [Breach.WRONG_KIND], id="array-for-text"),
            pytest.param({LifecycleKey.META: "x"}, [Breach.WRONG_KIND], id="bag-as-value"),
            pytest.param({ATTEMPT: None}, [], id="missing-value"),
            pytest.param({SUBKEY: object()}, [Breach.OPAQUE_VALUE], id="opaque-subkey"),
            pytest.param(
                {LogField.EXCEPTION_TYPE: "ValueError"},
                [Breach.EXCEPTION_TYPE_BY_HAND],
                id="exception-type-by-hand",
            ),
            pytest.param(
                {JOB: "card 4111 refused", "failure": ValueError("card 4111")},
                [Breach.EXCEPTION_TEXT_IN_FIELD],
                id="exception-text-in-field",
            ),
            pytest.param(
                {JOB: "billing-job", "failure": ValueError("bill")},
                [],
                id="short-text-inside-a-field",
            ),
        ],
    )
    def test_a_record_breaks_the_rules_it_does_not_follow(
        self, record: RecordFactory, built: dict[str, object], expected: list[Breach]
    ) -> None:
        assert Breach.broken_by(record(**built)) == expected

    def test_a_key_of_the_log_context_is_judged_like_one_of_the_record(
        self, record: RecordFactory
    ) -> None:
        with log_context({"amount": 3, LogField.EXCEPTION_TYPE: "ValueError"}):
            broken = Breach.broken_by(record())

        assert broken == [Breach.UNDECLARED_KEY, Breach.EXCEPTION_TYPE_BY_HAND]


class TestLogContract:
    def test_lists_each_record_that_breaks_the_contract_once(self, record: RecordFactory) -> None:
        contract = LogContract()

        contract.emit(record(msg="thing done", amount=3))
        contract.emit(record(msg="thing done", amount=3))
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
