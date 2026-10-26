"""Each layer imports only the layers inside it (ADR 0006).

The check reads the source, so an import inside a function counts; one under
``if TYPE_CHECKING:`` loads nothing and does not. Two tests complement it at
run time, catching what a module pulls in transitively:
``tests/unit/test_core_transport_containment.py`` for ``loom.core`` and
``tests/unit/composition/test_boundary.py`` for ``loom.composition``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.helpers.imports import imported_modules

_SRC = Path(__file__).resolve().parents[2] / "src"

# layer → (packages it must not import at run time, modules allowed inside them).
_RULES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    # The core composes nothing: the composition root and the transports sit outside it.
    "loom.core": (("loom.composition", "loom.rest", "loom.celery", "loom.ai"), ()),
    # The composition root reaches no transport. The Celery job service and its
    # config are the outbound adapter of the JobService port, not the worker.
    "loom.composition": (
        ("fastapi", "starlette", "celery", "loom.rest", "loom.celery"),
        ("loom.celery.service", "loom.celery.config"),
    ),
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


@pytest.mark.parametrize(("layer", "rule"), _RULES.items(), ids=list(_RULES))
def test_a_layer_imports_nothing_outside_it(
    layer: str, rule: tuple[tuple[str, ...], tuple[str, ...]]
) -> None:
    forbidden, allowed = rule
    offenders = []
    for path in sorted((_SRC / Path(*layer.split("."))).rglob("*.py")):
        module, package = _module_of(path)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for imported in sorted(imported_modules(tree, package=package)):
            if any(_within(imported, banned) for banned in forbidden) and not any(
                _within(imported, kept) for kept in allowed
            ):
                offenders.append(f"{module} imports {imported}")

    assert offenders == []


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
