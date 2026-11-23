"""``ErrorSummary``: what a failure is, without its message."""

from __future__ import annotations

import pytest

from loom.core.config.errors import ConfigError
from loom.core.errors import DomainError, NotFound
from loom.core.errors.codes import ErrorCode
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


class _UserDomainError(DomainError):
    """An application's own subclass of a loom error."""


class TestCite:
    @pytest.mark.parametrize(
        ("cause", "cited"),
        [
            (
                ConfigError("observability.log.format is unknown"),
                "observability.log.format is unknown",
            ),
            (NotFound("Order", id=7), str(NotFound("Order", id=7))),
            (TypeError("resolver must take a declaration"), "TypeError"),
            (ValueError("card 4111"), "ValueError"),
            (
                _UserDomainError("card 4111", code=ErrorCode.SYSTEM_ERROR),
                "_UserDomainError (system_error)",
            ),
            (ExceptionGroup("batch", [ValueError("card 4111")]), "ExceptionGroup"),
        ],
        ids=["loom-config", "loom-domain", "builtin", "foreign", "user-subclass", "group"],
    )
    def test_it_repeats_loom_text_and_names_anything_else_by_its_summary(
        self, cause: BaseException, cited: str
    ) -> None:
        assert ErrorSummary.cite(cause) == cited
