"""Every streaming compile-issue code renders, and every call site passes its params."""

from __future__ import annotations

import pytest

from loom.streaming.compiler import StreamingErrorCode
from tests.helpers.issue_catalogue import (
    INDIRECT_CALLS,
    IssueCall,
    check_renders,
    indirect_calls,
    issue_calls,
    missing_covering_tests,
)


@pytest.mark.parametrize("code", list(StreamingErrorCode), ids=str)
def test_every_code_renders_from_exactly_its_declared_params(code: StreamingErrorCode) -> None:
    check_renders(code)


@pytest.mark.parametrize("call", issue_calls(StreamingErrorCode), ids=lambda call: call.location)
def test_every_call_site_passes_exactly_the_params_of_its_code(call: IssueCall) -> None:
    assert call.keywords == StreamingErrorCode[call.member].params


def test_every_indirect_call_site_is_listed_with_tests_that_exist() -> None:
    listed = {site for site in INDIRECT_CALLS if site.startswith("streaming/")}
    assert indirect_calls("streaming") == listed
    assert missing_covering_tests("streaming") == []
