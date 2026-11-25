"""Loom's log names are a public contract that may only grow, published in a file."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from scripts.log_contract import CONTRACT_FILE, DeclaredNames, main

pytestmark = pytest.mark.slow

_EVERY_EXTRA_INSTALLS = sys.version_info < (3, 13)


@pytest.fixture(scope="module")
def declared() -> DeclaredNames:
    names = DeclaredNames.from_package()
    assert not names.crashed_modules, f"modules crashed on import: {sorted(names.crashed_modules)}"
    return names


@pytest.fixture
def declared_by_every_extra(declared: DeclaredNames) -> DeclaredNames:
    if declared.failed_modules and not _EVERY_EXTRA_INSTALLS:
        pytest.skip(f"no wheel of an extra for Python {sys.version_info[:2]}")
    return declared


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
        self, declared_by_every_extra: DeclaredNames, published: frozenset[str]
    ) -> None:
        declared = declared_by_every_extra
        assert not declared.failed_modules, (
            f"{sorted(declared.failed_modules)} failed to import: sync with "
            "`uv sync --all-extras --all-groups`"
        )
        gone = sorted(published - declared.lines)

        assert not gone, (
            f"published log names are no longer declared: {gone}. After 12.0 the contract "
            "only grows: restore each name (a rename is a removal plus an addition)."
        )

    def test_a_module_that_crashes_on_import_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "crashy").mkdir()
        (tmp_path / "crashy" / "__init__.py").touch()
        (tmp_path / "crashy" / "boom.py").write_text("raise RuntimeError")
        monkeypatch.syspath_prepend(tmp_path)
        monkeypatch.setitem(sys.modules, "crashy", importlib.import_module("crashy"))

        with pytest.raises(SystemExit, match="crashy.boom"):
            main(sys.modules["crashy"])
