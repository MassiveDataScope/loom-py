from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_PACKAGE = Path(__file__).resolve().parents[4] / "src" / "loom" / "core" / "authz"


def _modules() -> list[Path]:
    return sorted(_PACKAGE.glob("*.py"))


_OWN = "loom.core.authz"


def _imported(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                names.add(node.module or "")
            elif node.level == 1:
                names.add(f"{_OWN}.{node.module}" if node.module else _OWN)
            else:
                names.add("." * node.level + (node.module or ""))
    return names


def _is_allowed(name: str) -> bool:
    return (
        name == "__future__"
        or name == _OWN
        or name.startswith(f"{_OWN}.")
        or name.split(".")[0] in sys.stdlib_module_names
    )


def test_imports_only_the_standard_library() -> None:
    foreign = {
        (path.name, name)
        for path in _modules()
        for name in _imported(ast.parse(path.read_text()))
        if not _is_allowed(name)
    }

    assert foreign == set()


def test_names_no_domain_concept() -> None:
    offenders = [path.name for path in _modules() if "tenant" in path.read_text().lower()]

    assert offenders == []


@pytest.mark.parametrize(
    ("source", "allowed"),
    [
        ("import dataclasses", True),
        ("from loom.core.authz._scope import Scope", True),
        ("from ._scope import Scope", True),
        ("from ..model import LoomType", False),
        ("from loom.core.authzx import thing", False),
        ("import pydantic", False),
    ],
)
def test_the_import_rule_itself(source: str, allowed: bool) -> None:
    assert all(_is_allowed(name) for name in _imported(ast.parse(source))) is allowed
