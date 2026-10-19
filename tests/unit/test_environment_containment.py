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

import pytest

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


_ENVIRON = frozenset({"os.environ", "environ"})
_READERS = frozenset(
    {"os.environ.get", "os.getenv", "os.environ.pop", "os.environ.setdefault", "environ.get"}
    | {"getenv", "environ.pop"}
)
_ENVIRON_ITERATIONS = frozenset(
    f"{base}{suffix}" for base in _ENVIRON for suffix in ("", ".items()", ".keys()")
)


def _loom_names(tree: ast.AST, constants: frozenset[str]) -> set[str]:
    """Names bound in the module to a constant of the environment module, aliases included."""
    names = set(constants)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "loom.core.environment":
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _names_a_loom_variable(
    nodes: list[ast.AST], names: set[str], constants: frozenset[str]
) -> bool:
    for root in nodes:
        for node in ast.walk(root):
            if isinstance(node, ast.Name) and node.id in names:
                return True
            if isinstance(node, ast.Attribute) and node.attr in constants:
                return True
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value.startswith("LOOM_")
            ):
                return True
    return False


def _environ_key(node: ast.AST) -> list[ast.AST] | None:
    """Return what keys an access to the process environment, or ``None`` if *node* is none."""
    if isinstance(node, ast.Call) and node.args and ast.unparse(node.func) in _READERS:
        return [node.args[0]]
    if isinstance(node, ast.Subscript) and ast.unparse(node.value) in _ENVIRON:
        return [node.slice]
    if (
        isinstance(node, ast.Compare)
        and any(isinstance(op, ast.In | ast.NotIn) for op in node.ops)
        and any(ast.unparse(c) in _ENVIRON for c in node.comparators)
    ):
        return [node.left]
    if isinstance(node, ast.comprehension) and ast.unparse(node.iter) in _ENVIRON_ITERATIONS:
        return list(node.ifs)
    if isinstance(node, ast.For) and ast.unparse(node.iter) in _ENVIRON_ITERATIONS:
        return list(node.body)
    return None


def _environ_reads(tree: ast.AST, constants: frozenset[str]) -> list[str]:
    names = _loom_names(tree, constants)
    return [
        f"{getattr(node, 'lineno', '?')}: {ast.unparse(node)}"
        for node in ast.walk(tree)
        if (keys := _environ_key(node)) is not None
        and _names_a_loom_variable(keys, names, constants)
    ]


def _reads_of(path: Path, constants: frozenset[str]) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [f"{path.relative_to(_SRC)}:{hit}" for hit in _environ_reads(tree, constants)]


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


_VIOLATIONS = {
    "aliased constant": (
        "import os\n"
        "from loom.core.environment import ETL_CONFIG_VARIABLE as WHERE\n"
        "value = os.environ.get(WHERE)\n"
    ),
    "module attribute": (
        "import os\nfrom loom.core import environment\n"
        "value = os.getenv(environment.CONFIG_VARIABLE)\n"
    ),
    "f-string key": "import os\nsuffix = 'CONFIG'\nvalue = os.environ[f'LOOM_{suffix}']\n",
    "prefix filter": (
        "import os\nfrom loom.core.environment import OVERRIDE_PREFIX\n"
        "found = {k: v for k, v in os.environ.items() if k.startswith(OVERRIDE_PREFIX)}\n"
    ),
    "membership": (
        "import os\nfrom loom.core.environment import CONFIG_VARIABLE\n"
        "present = CONFIG_VARIABLE in os.environ\n"
    ),
}
_ALLOWED = (
    "import os\nhome = os.environ.get('HOME')\n"
    "from loom.core.environment import ETL_CONFIG_VARIABLE\n"
    "job = {ETL_CONFIG_VARIABLE: 'uri'}\n"
)


@pytest.mark.parametrize("source", _VIOLATIONS.values(), ids=list(_VIOLATIONS))
def test_the_read_detector_catches_each_way_of_reading(source: str) -> None:
    assert _environ_reads(ast.parse(source), _environment_module_constants()) != []


def test_the_read_detector_allows_other_variables_and_plain_use_of_the_names() -> None:
    assert _environ_reads(ast.parse(_ALLOWED), _environment_module_constants()) == []
