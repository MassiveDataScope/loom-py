"""The allow-list of modules still outside the logging standard only shrinks."""

from __future__ import annotations

import logging

import pytest

from loom.core.logger import LogEvent, LogField
from tests.helpers.log_standard import (
    UNMIGRATED_MODULES,
    Breach,
    foreign_keys,
    modules_with_foreign_extra,
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
        assert Breach.of(record) == expected
