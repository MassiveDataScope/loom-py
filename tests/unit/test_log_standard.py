"""The static guards, logging calls and exception text, and the code still outside them."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.testing.log_calls import exception_text_offences, log_call_offences
from tests.helpers.log_standard import (
    EXCEPTION_TEXT_EXITS,
    UNMIGRATED_EXCEPTION_TEXT,
    UNMIGRATED_PACKAGES,
    exception_text_sites,
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


def _text_rules(tmp_path: Path, body: str) -> list[str]:
    source = tmp_path / "sample.py"
    source.write_text(body)
    return [offence.rule for offence in exception_text_offences([source])]


_EXCEPT = "try:\n    run()\nexcept Exception as exc:\n"


class TestExceptionText:
    @pytest.mark.parametrize(
        ("line", "rule"),
        [
            ("    raise ConfigError(f'bad: {exc}')\n", "an f-string of the exception 'exc'"),
            ("    payload = {'reason': str(exc)}\n", "str(...) of the exception 'exc'"),
            ("    body = repr(exc.args)\n", "repr(...) of the exception 'exc'"),
            ("    text = 'failed: %s' % (exc,)\n", "a % format of the exception 'exc'"),
            ("    text = 'failed: {}'.format(exc)\n", ".format(...) of the exception 'exc'"),
            ("    text = str(exc.args[0])\n", "str(...) of the exception 'exc'"),
            ("    text = traceback.format_exc()\n", "traceback.format_exc(...)"),
        ],
    )
    def test_the_text_of_an_except_bound_name_is_refused(
        self, tmp_path: Path, line: str, rule: str
    ) -> None:
        assert _text_rules(tmp_path, _EXCEPT + line) == [rule]

    def test_the_text_of_a_parameter_typed_as_an_exception_is_refused(self, tmp_path: Path) -> None:
        body = "def answer(error: LoomError | None) -> str:\n    return f'{error}'\n"

        assert _text_rules(tmp_path, body) == ["an f-string of the exception 'error'"]

    @pytest.mark.parametrize(
        "body",
        [
            _EXCEPT + "    reason = public_form(exc)\n",
            _EXCEPT + "    name = type(exc).__name__\n",
            _EXCEPT + "    code = f'{exc.code}'\n",
            "def mapping(error: JSONRPCError) -> str:\n    return str(error['message'])\n",
            _EXCEPT + "    pass\ntext = str(exc)\n",
        ],
        ids=["public-form", "type-name", "attribute", "subscript-of-a-mapping", "out-of-scope"],
    )
    def test_what_is_not_the_exception_text_passes(self, tmp_path: Path, body: str) -> None:
        assert _text_rules(tmp_path, body) == []


class TestLoomExceptionText:
    def test_every_module_formats_exception_text_only_where_it_is_listed(self) -> None:
        listed = {module: count for module, (count, _) in EXCEPTION_TEXT_EXITS.items()}
        for module, count in UNMIGRATED_EXCEPTION_TEXT.items():
            listed[module] = listed.get(module, 0) + count

        assert exception_text_sites() == listed
