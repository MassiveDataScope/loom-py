"""Placement lint for ``LoomError`` subclasses (ADR 0003, rule R3).

A class that extends anything ``loom.core.errors`` exports lives either under
``loom/core/errors/`` (shared across pillars) or in a module named
``errors.py`` / ``_errors.py`` (the owning pillar's errors module).  The test
reads ``src/loom`` with :mod:`ast` and imports nothing beyond the catalogue of
exported names, so it needs no optional extras and carries no allowlist.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

from loom.core import errors as core_errors

_SRC = Path(__file__).resolve().parents[4] / "src" / "loom"
_ERRORS_PACKAGE = _SRC / "core" / "errors"
_ERRORS_MODULES = frozenset({"errors.py", "_errors.py"})
_EXPORTED = frozenset(core_errors.__all__)


def _base_name(base: ast.expr) -> str | None:
    """Return the last name segment of a base expression, or ``None``."""
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return None


def _classes(path: Path) -> Iterator[ast.ClassDef]:
    """Yield every class definition in ``path``, nested ones included."""
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ClassDef):
            yield node


def _extends_core_error(node: ast.ClassDef) -> bool:
    """Return whether a base of ``node`` names a ``loom.core.errors`` export."""
    return any(_base_name(base) in _EXPORTED for base in node.bases)


def _misplaced() -> list[str]:
    """Return ``path:line class`` for every error class outside an errors module."""
    found: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        if path.is_relative_to(_ERRORS_PACKAGE) or path.name in _ERRORS_MODULES:
            continue
        found.extend(
            f"{path.relative_to(_SRC)}:{node.lineno} {node.name}"
            for node in _classes(path)
            if _extends_core_error(node)
        )
    return found


def test_core_error_subclasses_live_in_an_errors_module() -> None:
    """R3: a ``LoomError`` subclass lives under ``core/errors/`` or in an errors module."""
    assert _misplaced() == []
