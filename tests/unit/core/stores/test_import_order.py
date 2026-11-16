"""Every module of ``loom.core.stores`` imports first in a fresh interpreter.

The facade rule covers the repository and unit-of-work packages; the SQL port
imports its own ``abc`` leaf package.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[4] / "src"
_STORES = _SRC / "loom" / "core" / "stores"


def _dotted(path: Path) -> str:
    parts = list(path.relative_to(_SRC).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


_MODULES = sorted(_dotted(path) for path in _STORES.rglob("*.py"))
_FACADES = frozenset(
    _dotted(path)
    for path in _STORES.rglob("__init__.py")
    if "sql" not in path.relative_to(_STORES).parts
)


class TestStoresImportOrder:
    @pytest.mark.parametrize("module", _MODULES)
    def test_the_module_imports_first(self, module: str) -> None:
        result = subprocess.run(
            [sys.executable, "-I", "-c", f"import {module}"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, result.stderr

    def test_no_module_imports_a_package_facade(self) -> None:
        offenders = [
            f"{_dotted(path)} imports {node.module}"
            for path in sorted(_STORES.rglob("*.py"))
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.ImportFrom)
            and node.module in _FACADES
            and not any(alias.name == "checks" for alias in node.names)
        ]

        assert offenders == []
