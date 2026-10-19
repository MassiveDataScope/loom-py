"""Only ``loom.core.environment`` names or reads loom's process variables.

Every other module receives the resolved environment and configuration from
it. A module naming ``LOOM_ENV``, ``ENVIRONMENT``, a ``LOOM_*`` variable or
the ``LOOM__`` override prefix on its own, or reading one of the names the
environment module defines through ``os.environ`` or ``os.getenv``, would be
a second reader of the process environment, which this containment forbids.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "loom"
_SELECTOR_MODULE = _SRC / "core" / "environment.py"
_SELECTOR_NAMES = re.compile(r"^([A-Z0-9_]*_?ENV(IRONMENT)?|LOOM_[A-Z0-9_]*)$")
# Literals shaped like a variable name that name something else, with why.
_NOT_A_VARIABLE = {
    "LOOM_AI_ENGINE_API": "the attribute an AI engine provider declares its version on",
}


def _exported_names(tree: ast.Module) -> set[int]:
    """Return the ids of the string nodes listed in ``__all__``."""
    return {
        id(element)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets)
        for element in ast.walk(node.value)
    }


def _selector_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    exported = _exported_names(tree)
    return [
        f"{path.relative_to(_SRC)}:{node.lineno}: {node.value!r}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _SELECTOR_NAMES.match(node.value)
        and node.value not in _NOT_A_VARIABLE
        and id(node) not in exported
    ]


def _environment_module_constants() -> frozenset[str]:
    tree = ast.parse(_SELECTOR_MODULE.read_text(encoding="utf-8"))
    return frozenset(
        target.id
        for node in tree.body
        if isinstance(node, ast.AnnAssign | ast.Assign)
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name) and target.id.isupper()
    )


def _is_environ_read(node: ast.AST) -> ast.expr | None:
    """Return the key of ``os.environ.get(k)``, ``os.getenv(k)`` or ``os.environ[k]``."""
    if isinstance(node, ast.Call) and node.args:
        func = ast.unparse(node.func)
        if func in {"os.environ.get", "os.getenv", "environ.get", "getenv"}:
            return node.args[0]
    if isinstance(node, ast.Subscript) and ast.unparse(node.value) in {"os.environ", "environ"}:
        return node.slice
    return None


def _reads_of(path: Path, names: frozenset[str]) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits = []
    for node in ast.walk(tree):
        key = _is_environ_read(node)
        if isinstance(key, ast.Name | ast.Attribute) and ast.unparse(key).split(".")[-1] in names:
            hits.append(f"{path.relative_to(_SRC)}:{node.lineno}: {ast.unparse(node)}")
    return hits


def test_the_selector_pattern_catches_the_usual_names() -> None:
    names = (
        "LOOM_ENV",
        "ENVIRONMENT",
        "ENV",
        "APP_ENV",
        "PYTHON_ENV",
        "DEPLOY_ENVIRONMENT",
        "LOOM__",
        "LOOM_CONFIG",
        "LOOM_STORAGE_CONFIG_PATH",
    )

    assert [name for name in names if not _SELECTOR_NAMES.match(name)] == []


def test_the_selector_module_is_the_one_naming_the_variables() -> None:
    assert _selector_literals(_SELECTOR_MODULE) != []


def test_no_other_module_names_a_loom_process_variable() -> None:
    offenders = [
        hit
        for path in sorted(_SRC.rglob("*.py"))
        if path != _SELECTOR_MODULE
        for hit in _selector_literals(path)
    ]

    assert offenders == []


def test_no_other_module_reads_a_variable_the_environment_module_names() -> None:
    names = _environment_module_constants()
    offenders = [
        hit
        for path in sorted(_SRC.rglob("*.py"))
        if path != _SELECTOR_MODULE
        for hit in _reads_of(path, names)
    ]

    assert "ENVIRONMENT_VARIABLE" in names
    assert offenders == []
