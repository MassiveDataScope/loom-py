"""``ErrorSummary``: what a failure is, without its message."""

from __future__ import annotations

import pytest

from loom.core.errors import NotFound
from loom.core.logger import ErrorSummary, LogField


class TestErrorSummary:
    @pytest.mark.parametrize(
        ("error", "summary"),
        [
            (NotFound("Order", id=7), ErrorSummary(type="NotFound", code="not_found")),
            (ValueError("card 4111"), ErrorSummary(type="ValueError")),
        ],
        ids=["loom-error", "foreign"],
    )
    def test_it_keeps_the_type_and_the_code_and_never_the_message(
        self, error: Exception, summary: ErrorSummary
    ) -> None:
        assert ErrorSummary.of(error) == summary

    @pytest.mark.parametrize(
        ("summary", "described", "fields"),
        [
            (
                ErrorSummary(type="NotFound", code="not_found"),
                "NotFound (not_found)",
                {LogField.EXCEPTION_TYPE: "NotFound", LogField.ERROR_CODE: "not_found"},
            ),
            (
                ErrorSummary(type="ValueError"),
                "ValueError",
                {LogField.EXCEPTION_TYPE: "ValueError"},
            ),
        ],
        ids=["with-code", "without-code"],
    )
    def test_it_describes_itself_and_names_its_fields(
        self, summary: ErrorSummary, described: str, fields: dict[str, str]
    ) -> None:
        assert summary.describe() == described
        assert summary.fields() == fields
