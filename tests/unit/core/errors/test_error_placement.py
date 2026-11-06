"""Placement lint for ``LoomError`` subclasses (ADR 0003, rules R1, R3 and R5).

R1: every ``code=`` an error class passes to ``super().__init__`` is a member
of a catalogue (``ErrorCode``, or an adapter's own ``*ErrorCode``) or the
``code`` parameter it received; a literal string is not a code.
R2: only ``DomainError`` and ``SystemError`` name ``LoomError`` as a base;
every other error picks a family.
R3: a class that extends ``LoomError``, directly or through any subclass,
lives either under ``loom/core/errors/`` (shared across pillars) or in a
module named ``errors.py`` / ``_errors.py`` (the owning pillar's errors
module). R5: nothing under ``loom/core`` imports ``http``, ``fastapi`` or
``starlette``. The test reads ``src/loom`` with :mod:`ast`, so it needs no
optional extras and carries no allowlist.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src" / "loom"
_ERRORS_PACKAGE = _SRC / "core" / "errors"
_ERRORS_MODULES = frozenset({"errors.py", "_errors.py"})
_ROOT = "LoomError"
_FAMILIES = frozenset({"DomainError", "SystemError"})
_WEB = frozenset({"http", "fastapi", "starlette"})


def _base_name(base: ast.expr) -> str | None:
    """The last name segment of a base expression, or ``None``."""
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return None


def _classes(path: Path) -> Iterator[ast.ClassDef]:
    """Every class definition in *path*, nested ones included."""
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ClassDef):
            yield node


def _children(by_name: dict[str, list[tuple[Path, ast.ClassDef]]]) -> dict[str, set[str]]:
    """Every class name to the names of the classes that name it as a base."""
    children: dict[str, set[str]] = {}
    for name, definitions in by_name.items():
        for _, node in definitions:
            for base in node.bases:
                parent = _base_name(base)
                if parent is not None:
                    children.setdefault(parent, set()).add(name)
    return children


def _error_classes() -> dict[str, list[tuple[Path, ast.ClassDef]]]:
    """Every class of ``src/loom`` that descends from ``LoomError``, by name.

    Bases are matched by name and closed transitively: a subclass of a
    pillar-local error counts as well. Names are not resolved through
    imports, so a class whose base shares its name with a loom error is
    read as one; none does today.
    """
    by_name: dict[str, list[tuple[Path, ast.ClassDef]]] = {}
    for path in sorted(_SRC.rglob("*.py")):
        for node in _classes(path):
            by_name.setdefault(node.name, []).append((path, node))
    children = _children(by_name)
    errors: set[str] = set()
    pending = [_ROOT]
    while pending:
        name = pending.pop()
        if name not in errors:
            errors.add(name)
            pending.extend(children.get(name, ()))
    return {name: by_name[name] for name in errors if name in by_name}


def _misplaced() -> list[str]:
    """``path:line class`` for every error class outside an errors module."""
    return [
        f"{path.relative_to(_SRC)}:{node.lineno} {node.name}"
        for definitions in _error_classes().values()
        for path, node in definitions
        if not (path.is_relative_to(_ERRORS_PACKAGE) or path.name in _ERRORS_MODULES)
    ]


def _web_imports_in_core() -> list[str]:
    """``path:line module`` for every import of a web package under ``loom/core``."""
    found: list[str] = []
    for path in sorted((_SRC / "core").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            found.extend(
                f"{path.relative_to(_SRC)}:{node.lineno} {name}"
                for name in names
                if name.split(".")[0] in _WEB
            )
    return found


def _is_catalogue_member(value: ast.expr) -> bool:
    """``<Something>ErrorCode.MEMBER``, or the ``code`` parameter passed through."""
    if isinstance(value, ast.Name):
        return value.id == "code"
    return (
        isinstance(value, ast.Attribute)
        and isinstance(value.value, ast.Name)
        and value.value.id.endswith("ErrorCode")
    )


def _literal_codes() -> list[str]:
    """``path:line`` for every ``code=`` of an error class that is no catalogue member."""
    found: list[str] = []
    for definitions in _error_classes().values():
        for path, node in definitions:
            for call in ast.walk(node):
                if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)):
                    continue
                if call.func.attr != "__init__":
                    continue
                found.extend(
                    f"{path.relative_to(_SRC)}:{keyword.lineno}"
                    for keyword in call.keywords
                    if keyword.arg == "code" and not _is_catalogue_member(keyword.value)
                )
    return found


def test_the_lint_sees_the_whole_hierarchy() -> None:
    """A subclass of a pillar error is an error class too; the catalogue is in."""
    names = _error_classes()
    assert {"LoomError", "DomainError", "SystemError", "NotFound"} <= names.keys()
    assert "UnsupportedFormatError" in names


def _outside_a_family() -> list[str]:
    """``path:line class`` for every class that names ``LoomError`` itself as a base."""
    return [
        f"{path.relative_to(_SRC)}:{node.lineno} {node.name}"
        for definitions in _error_classes().values()
        for path, node in definitions
        if node.name not in _FAMILIES and _ROOT in {_base_name(base) for base in node.bases}
    ]


def test_every_error_code_is_a_catalogue_member() -> None:
    """R1: a code is declared once, in a catalogue; no class spells one as a string."""
    assert _literal_codes() == []


def test_every_error_belongs_to_a_family() -> None:
    """R2: a transport tells a 4xx from a 5xx, and what to retry, by the family alone."""
    assert _outside_a_family() == []


def test_core_error_subclasses_live_in_an_errors_module() -> None:
    """R3: a ``LoomError`` subclass lives under ``core/errors/`` or in an errors module."""
    assert _misplaced() == []


def test_core_imports_no_web_package() -> None:
    """R5: core errors say what failed; a transport maps them."""
    assert _web_imports_in_core() == []
