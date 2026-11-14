"""Every compile-issue catalogue keeps the contract of ADR 0003 part B.

The contract is in :mod:`tests.helpers.issue_catalogue`; each check runs over
one catalogue and lists every member or call site that breaks it. The tests a
single catalogue needs stay beside it, in its pillar's suite.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentErrorCode
from loom.core.errors.codes import IssueCode
from loom.etl.compiler import ETLErrorCode
from loom.streaming.compiler import StreamingErrorCode
from tests.helpers.issue_catalogue import (
    INDIRECT_CALLS,
    IssueCall,
    check_renders,
    indirect_calls,
    issue_calls,
    missing_covering_tests,
)

_CATALOGUES: dict[str, type[IssueCode]] = {
    "ai": AgentErrorCode,
    "etl": ETLErrorCode,
    "streaming": StreamingErrorCode,
}
"""Each pillar, by the directory under ``src/loom`` that holds its call sites."""

_EACH_CATALOGUE = pytest.mark.parametrize(
    ("pillar", "catalogue"), list(_CATALOGUES.items()), ids=list(_CATALOGUES)
)


def _call_mismatch(call: IssueCall, catalogue: type[IssueCode]) -> str | None:
    """Why *call* does not pass exactly its member's parameters, or ``None`` when it does."""
    member = catalogue.__members__.get(call.member)
    if member is None:
        return f"{call.location}: {catalogue.__name__} has no member {call.member}"
    if call.keywords == member.params:
        return None
    passed = "positional or ** arguments" if call.keywords is None else sorted(call.keywords)
    return f"{call.location}: {member} passes {passed}, declares {sorted(member.params)}"


class TestIssueCatalogue:
    @_EACH_CATALOGUE
    def test_every_code_renders_from_exactly_its_declared_params(
        self, pillar: str, catalogue: type[IssueCode]
    ) -> None:
        del pillar
        failures: list[str] = []
        for code in catalogue:
            try:
                check_renders(code)
            except Exception as error:  # noqa: BLE001 - every failing code is listed
                failures.append(f"{code}: {type(error).__name__}: {error}")

        assert failures == [], "\n".join(failures)

    @_EACH_CATALOGUE
    def test_every_call_site_passes_exactly_the_params_of_its_code(
        self, pillar: str, catalogue: type[IssueCode]
    ) -> None:
        del pillar
        mismatches = [
            mismatch
            for call in issue_calls(catalogue)
            if (mismatch := _call_mismatch(call, catalogue)) is not None
        ]

        assert mismatches == [], "\n".join(mismatches)

    @_EACH_CATALOGUE
    def test_every_indirect_call_site_is_listed_with_tests_that_exist(
        self, pillar: str, catalogue: type[IssueCode]
    ) -> None:
        del catalogue
        listed = {site for site in INDIRECT_CALLS if site.startswith(f"{pillar}/")}

        assert indirect_calls(pillar) == listed
        assert missing_covering_tests(pillar) == []
