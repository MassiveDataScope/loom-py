"""Only ``loom.core.environment`` names or touches loom's process variables.

Every other module receives the resolved environment and configuration from
it. Three rules keep it that way:

- no other module names ``LOOM_ENV``, ``ENVIRONMENT``, a similar selector, a
  ``LOOM_*`` variable or the ``LOOM__`` prefix as a string literal;
- closed world: outside the environment module, every access to
  ``os.environ``, ``environ``, ``os.getenv`` or ``getenv`` reads one variable
  named by a string literal that does not start with ``LOOM_``. A dynamic
  key, iteration, ``.copy()``, ``dict(os.environ)``, a write or membership
  tested on a name fails, unless the file is in a small allowlist with its
  reason;
- no source or document names a ``LOOM_*`` variable that loom does not own.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src" / "loom"
_SELECTOR_MODULE = _SRC / "core" / "environment.py"
_SELECTOR_NAMES = re.compile(r"^([A-Z0-9_]*_?ENV(IRONMENT)?|LOOM_[A-Z0-9_]*)$")
_LOOM_PREFIX = "LOOM_"
# Names shaped like a loom variable that are something else, with why.
_NOT_A_VARIABLE = {
    "LOOM_AI_ENGINE_API": "the attribute an AI engine provider declares its version on",
    "LOOM_ETL_META_ATTR": "the constant naming the attribute flows carry their metadata on",
}
# Files whose read of the process environment is dynamic by design, with why.
_DYNAMIC_READERS = {
    "core/config/_resolver_utils.py": "expands the %VAR% placeholders a configuration names",
    "ai/engines/pydantic_ai/providers/_shared.py": (
        "reads the variable an AI provider's credentials_ref names"
    ),
}
# LOOM_* names loom owns outside loom.core.environment, with why.
_OWNED_ELSEWHERE = {
    "LOOM_PG_IT_URI": "the database loom's own integration tests run against",
}
_ENVIRON = frozenset({"os.environ", "environ"})
_GETENV = frozenset({"os.getenv", "getenv"})


def _selector_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        f"{path.relative_to(_SRC)}:{node.lineno}: {node.value!r}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _SELECTOR_NAMES.match(node.value)
        and node.value not in _NOT_A_VARIABLE
    ]


def _literal_key(node: ast.AST | None) -> bool:
    """Whether *node* is a string literal naming a variable loom does not own."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and not node.value.startswith(_LOOM_PREFIX)
    )


def _reads_one_literal(node: ast.AST, parent: ast.AST | None, grand: ast.AST | None) -> bool:
    """Whether the ``os.environ`` reference *node* reads one literal, non-loom variable."""
    if isinstance(parent, ast.Subscript) and parent.value is node:
        return isinstance(parent.ctx, ast.Load) and _literal_key(parent.slice)
    if isinstance(parent, ast.Attribute) and parent.attr == "get" and isinstance(grand, ast.Call):
        return bool(grand.args) and _literal_key(grand.args[0])
    if isinstance(parent, ast.Compare) and node in parent.comparators:
        membership = all(isinstance(op, ast.In | ast.NotIn) for op in parent.ops)
        return membership and _literal_key(parent.left)
    return False


def _environ_violations(tree: ast.AST) -> list[str]:
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    hits = []
    for node in ast.walk(tree):
        parent = parents.get(node)
        if isinstance(node, ast.Call) and ast.unparse(node.func) in _GETENV:
            if not (node.args and _literal_key(node.args[0])):
                hits.append(f"{node.lineno}: {ast.unparse(node)}")
            continue
        if not isinstance(node, ast.Attribute | ast.Name) or ast.unparse(node) not in _ENVIRON:
            continue
        if isinstance(parent, ast.Attribute) and ast.unparse(parent) in _ENVIRON:
            continue
        grand = parents.get(parent) if parent is not None else None
        if not _reads_one_literal(node, parent, grand):
            hits.append(f"{node.lineno}: {ast.unparse(grand or parent or node)}")
    return hits


def test_the_selector_pattern_catches_the_usual_names() -> None:
    names = ("LOOM_ENV", "ENVIRONMENT", "ENV", "APP_ENV", "DEPLOY_ENVIRONMENT", "LOOM__")

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


_VIOLATIONS = {
    "dynamic key": "import os\nvalue = os.environ.get(name)\n",
    "loom literal": "import os\nvalue = os.getenv('LOOM_CONFIG')\n",
    "iteration": "import os\nfound = [k for k in os.environ if k.startswith(prefix)]\n",
    "copy": "import os\nsnapshot = os.environ.copy()\n",
    "dict": "import os\nsnapshot = dict(os.environ)\n",
    "membership on a name": "import os\npresent = name in os.environ\n",
    "write": "import os\nos.environ['HOME'] = '/tmp'\n",
}
_ALLOWED = (
    "import os\n"
    "home = os.environ.get('HOME')\n"
    "port = os.getenv('PORT', '8000')\n"
    "region = os.environ['AWS_REGION']\n"
    "debug = 'DEBUG' in os.environ\n"
)


@pytest.mark.parametrize("source", _VIOLATIONS.values(), ids=list(_VIOLATIONS))
def test_the_rule_refuses_each_kind_of_access(source: str) -> None:
    assert _environ_violations(ast.parse(source)) != []


def test_the_rule_allows_reading_one_literal_variable() -> None:
    assert _environ_violations(ast.parse(_ALLOWED)) == []


def test_every_access_outside_the_environment_module_reads_one_literal_variable() -> None:
    offenders = [
        f"{path.relative_to(_SRC)}:{hit}"
        for path in sorted(_SRC.rglob("*.py"))
        if path != _SELECTOR_MODULE and path.relative_to(_SRC).as_posix() not in _DYNAMIC_READERS
        for hit in _environ_violations(ast.parse(path.read_text(encoding="utf-8")))
    ]

    assert offenders == []


def _owned_names() -> frozenset[str]:
    tree = ast.parse(_SELECTOR_MODULE.read_text(encoding="utf-8"))
    return frozenset(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and re.fullmatch(r"LOOM_[A-Z][A-Z0-9_]*", node.value)
    )


_LOOM_NAME = re.compile(r"\bLOOM_[A-Z][A-Z0-9_]*\b")


def test_no_source_or_document_names_a_loom_variable_loom_does_not_own() -> None:
    known = _owned_names() | set(_NOT_A_VARIABLE) | set(_OWNED_ELSEWHERE)
    documents = [*_SRC.rglob("*.py"), *(_ROOT / "docs").rglob("*.md"), _ROOT / "README.md"]
    offenders = sorted(
        f"{path.relative_to(_ROOT)}: {name}"
        for path in documents
        for name in set(_LOOM_NAME.findall(path.read_text(encoding="utf-8")))
        if name not in known
    )

    assert offenders == []
