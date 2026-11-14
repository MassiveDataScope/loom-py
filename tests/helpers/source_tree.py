"""The ``src/loom`` sources parsed once per session, for the tests that read the code.

Several repository checks walk the AST of every module under ``src/loom``.
Parsing the tree once per check cost about a second each; a file under
``src/loom`` is parsed once here and its tree shared. Callers only read the
trees: none may mutate one.
"""

from __future__ import annotations

import ast
from functools import cache
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "loom"


@cache
def source_files() -> tuple[Path, ...]:
    """Every ``.py`` file under ``src/loom``, sorted."""
    return tuple(sorted(SRC.rglob("*.py")))


def parsed(path: Path) -> ast.Module:
    """The AST of *path*: shared for a file under ``src/loom``, parsed afresh otherwise."""
    if path.resolve().is_relative_to(SRC):
        return _parsed_source(path.resolve())
    return ast.parse(path.read_text(encoding="utf-8"), str(path))


@cache
def _parsed_source(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), str(path))
