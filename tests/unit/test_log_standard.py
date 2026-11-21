"""The static call-site guard: what it refuses, and the allow-list of code still outside it."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.testing.log_calls import log_call_offences
from tests.helpers.log_standard import (
    UNMIGRATED_PACKAGES,
    is_unmigrated,
    loom_offences,
    module_of,
)


def _rules(tmp_path: Path, body: str) -> list[str]:
    source = tmp_path / "sample.py"
    source.write_text(body)
    return [offence.rule for offence in log_call_offences([source])]


class TestCompliantCalls:
    @pytest.mark.parametrize(
        "body",
        [
            "_log.info(BillingLogEvent.INVOICE_ISSUED)\n",
            "_log.info(BillingLogEvent.ISSUED, extra={LogField.COUNT: n})\n",
            "self._logger.log(level, BillingLogEvent.ISSUED, extra={BillingLogField.ID: i})\n",
            "_log.warning(JobLogEvent.RETRIED, extra={**self.run_fields(), LogField.COUNT: 1})\n",
            "with log_context({LogField.JOB: name}):\n    pass\n",
            "with log_context(whereabouts.run_fields()):\n    pass\n",
            "runtime.span(Scope.JOB, n, attributes={LogField.COUNT: 1, **base})\n",
            (
                "try:\n    run()\nexcept Exception as exc:\n"
                "    _log.error(JobLogEvent.FAILED, exc_info=exc)\n"
            ),
            "math.log(2)\n",
        ],
    )
    def test_a_call_in_the_standard_has_no_offence(self, tmp_path: Path, body: str) -> None:
        assert _rules(tmp_path, body) == []


class TestRefusedCalls:
    @pytest.mark.parametrize(
        ("body", "rule"),
        [
            ('_log.info("invoice issued")\n', "the message is not a LogEvent member"),
            ("_log.info(E.ISSUED, invoice)\n", "positional arguments after the event"),
            ('_log.info(E.ISSUED, extra={"amount": 3})\n', "extra key is not a LogKey member"),
            ("_log.info(E.ISSUED, extra=fields)\n", "extra is not a dict literal"),
            (
                "_log.info(E.ISSUED, extra={**other})\n",
                "extra spreads a mapping outside the allowed list",
            ),
            ("with log_context(invoice=i):\n    pass\n", "log_context keyword 'invoice'"),
            ('span(S.JOB, n, attributes={"flow": f})\n', "attributes key is not a LogKey"),
        ],
    )
    def test_a_call_outside_the_standard_is_refused(
        self, tmp_path: Path, body: str, rule: str
    ) -> None:
        rules = _rules(tmp_path, body)

        assert any(found.startswith(rule) for found in rules), rules

    @pytest.mark.parametrize(
        ("value", "rule"),
        [
            ("str(exc)", "str(...)"),
            ("repr(item)", "repr(...)"),
            ('f"{n} rows"', "an f-string"),
            ('"{}".format(n)', "format(...)"),
            ('"%s" % n', "a % format"),
            ("exc", "the except-bound name 'exc'"),
            ("exc.args", "the except-bound name 'exc'"),
        ],
    )
    def test_text_made_from_a_value_is_refused(self, tmp_path: Path, value: str, rule: str) -> None:
        body = (
            "try:\n    run()\nexcept Exception as exc:\n"
            f"    _log.error(JobLogEvent.FAILED, extra={{LogField.REASON: {value}}})\n"
        )

        assert _rules(tmp_path, body) == [f"extra value is {rule}"]

    def test_a_span_attribute_made_from_the_exception_is_refused(self, tmp_path: Path) -> None:
        body = (
            "try:\n    run()\nexcept Exception as exc:\n"
            "    obs.span(S.JOB, n, attributes={LogField.REASON: str(exc)})\n"
        )

        assert _rules(tmp_path, body) == ["attributes value is str(...)"]


class TestLoomCallSites:
    def test_code_outside_the_allow_list_follows_the_standard(self) -> None:
        offences = [str(o) for o in loom_offences() if not is_unmigrated(module_of(o))]

        assert offences == []

    def test_every_allow_list_entry_still_has_an_offence(self) -> None:
        offending = {module_of(offence) for offence in loom_offences()}
        stale = [
            entry
            for entry in UNMIGRATED_PACKAGES
            if not any(module == entry or module.startswith(f"{entry}.") for module in offending)
        ]

        assert stale == []
