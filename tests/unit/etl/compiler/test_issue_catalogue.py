"""Every ETL compile-issue code renders, and every call site passes its params."""

from __future__ import annotations

import pytest

from loom.etl.compiler import ETLErrorCode
from tests.helpers.issue_catalogue import IssueCall, check_renders, issue_calls


@pytest.mark.parametrize("code", list(ETLErrorCode), ids=str)
def test_every_code_renders_from_exactly_its_declared_params(code: ETLErrorCode) -> None:
    check_renders(code)


@pytest.mark.parametrize("call", issue_calls(ETLErrorCode), ids=lambda call: call.location)
def test_every_call_site_passes_exactly_the_params_of_its_code(call: IssueCall) -> None:
    assert call.keywords == ETLErrorCode[call.member].params
