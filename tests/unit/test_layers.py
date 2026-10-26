"""Each layer imports only the layers inside it (ADR 0006).

The check reads the source, so an import inside a function counts; one under
``if TYPE_CHECKING:`` loads nothing and does not. ``test_boundary`` in
``tests/unit/composition`` complements it at run time, catching what a module
pulls in transitively.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.helpers.imports import imported_modules

_SRC = Path(__file__).resolve().parents[2] / "src"

# package → packages it must not import at run time.
_FORBIDDEN: dict[str, tuple[str, ...]] = {
    # The core composes nothing: the composition root and the transports sit outside it.
    "loom.core": ("loom.composition", "loom.rest", "loom.celery", "loom.ai"),
    # The composition root reaches no transport; adapters reach it.
    "loom.composition": ("fastapi", "starlette", "celery", "loom.rest", "loom.celery"),
}


def _module_of(path: Path) -> tuple[str, str]:
    """Return the dotted module of *path* and the package its relative imports start from."""
    parts = list(path.relative_to(_SRC).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
        return ".".join(parts), ".".join(parts)
    return ".".join(parts), ".".join(parts[:-1])


def _within(module: str, package: str) -> bool:
    return module == package or module.startswith(package + ".")


def _offenders(layer: str, forbidden: tuple[str, ...]) -> list[str]:
    found = []
    for path in sorted((_SRC / Path(*layer.split("."))).rglob("*.py")):
        module, package = _module_of(path)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for imported in sorted(imported_modules(tree, package=package)):
            if any(_within(imported, banned) for banned in forbidden):
                found.append(f"{module} imports {imported}")
    return found


def test_the_core_imports_neither_the_composition_root_nor_a_transport() -> None:
    assert _offenders("loom.core", _FORBIDDEN["loom.core"]) == []


def test_the_composition_root_imports_no_transport() -> None:
    assert _offenders("loom.composition", _FORBIDDEN["loom.composition"]) == []


def test_the_check_sees_function_local_and_relative_imports() -> None:
    sample = ast.parse(
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from loom.rest.model import RestInterface\n"
        "def build():\n"
        "    from loom.celery.service import CeleryJobService\n"
        "from ..rest import errors\n"
        "from . import sibling\n"
    )

    assert imported_modules(sample, package="loom.core.engine") == {
        "typing",
        "loom.celery.service",
        "loom.core.rest",
        "loom.core.engine",
    }
