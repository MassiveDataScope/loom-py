"""The allow-list of modules still outside the logging standard only shrinks."""

from __future__ import annotations

import logging

from loom.core.logger import LogField
from tests.helpers.log_standard import UNMIGRATED_MODULES, foreign_keys, modules_with_foreign_extra


def _record(**extra: object) -> logging.LogRecord:
    record = logging.makeLogRecord({"name": "loom.acme", "msg": "acme.thing_done"})
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
