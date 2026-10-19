"""Only ``loom.core.environment`` reads the environment selector.

Every other module receives the resolved environment through configuration or
the container. A module reading ``LOOM_ENV``, ``ENVIRONMENT`` or a similar
variable on its own would be a second source of truth for the environment,
which is exactly what this containment forbids.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "loom"
_SELECTOR_MODULE = _SRC / "core" / "environment.py"
_SELECTOR_NAMES = re.compile(r"^[A-Z0-9_]*_?ENV(IRONMENT)?$")


def _selector_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        f"{path.relative_to(_SRC)}:{node.lineno}: {node.value!r}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _SELECTOR_NAMES.match(node.value)
    ]


def test_the_selector_pattern_catches_the_usual_names() -> None:
    names = ("LOOM_ENV", "ENVIRONMENT", "ENV", "APP_ENV", "PYTHON_ENV", "DEPLOY_ENVIRONMENT")

    assert [name for name in names if not _SELECTOR_NAMES.match(name)] == []


def test_the_selector_module_is_the_one_naming_the_variable() -> None:
    assert _selector_literals(_SELECTOR_MODULE) != []


def test_no_other_module_names_an_environment_selector_variable() -> None:
    offenders = [
        hit
        for path in sorted(_SRC.rglob("*.py"))
        if path != _SELECTOR_MODULE
        for hit in _selector_literals(path)
    ]

    assert offenders == []
