"""Loom's log names are a public contract that may only grow, published in a file."""

from __future__ import annotations

import pytest

from scripts.log_contract import CONTRACT_FILE, DeclaredNames


@pytest.fixture(scope="module")
def declared() -> DeclaredNames:
    return DeclaredNames.from_loom()


@pytest.fixture(scope="module")
def published() -> frozenset[str]:
    return frozenset(CONTRACT_FILE.read_text().splitlines())


class TestLogContract:
    def test_every_declared_name_is_published(
        self, declared: DeclaredNames, published: frozenset[str]
    ) -> None:
        unpublished = sorted(declared.lines - published)

        assert not unpublished, (
            f"loom declares log names the contract file lacks: {unpublished}. "
            "Run `uv run poe log-contract` and commit docs/reference/log-contract.txt."
        )

    def test_no_published_name_is_removed_or_renamed(
        self, declared: DeclaredNames, published: frozenset[str]
    ) -> None:
        unchecked = declared.unchecked(published)
        gone = sorted(published - unchecked - declared.lines)
        not_checked = (
            f" Not checked, their module failed to import: {sorted(unchecked)}."
            if unchecked
            else ""
        )

        assert not gone, (
            f"published log names are no longer declared: {gone}. After 12.0 the contract "
            f"only grows: restore each name (a rename is a removal plus an addition).{not_checked}"
        )
        if unchecked:
            pytest.skip(
                f"the other lines were checked, but {sorted(declared.failed_modules)} failed to "
                "import; (b) fully runs on Python < 3.13 with all extras (CI's 3.11/3.12 legs)"
            )
