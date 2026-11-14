"""No entry point builds outside an execution: a static scan of ``src/loom``.

Use cases and jobs are built by ``RuntimeExecutor.run``, which receives the
factory's ``build`` and calls it inside the execution it opens; no adapter
calls ``factory.build(...)`` itself. The one other builder is a job callback,
built inside ``callback_execution()`` (D7). Any other ``factory.build(`` call
is a site that builds before the execution exists (spec 011 S1).
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

import pytest

import loom

SOURCE = Path(loom.__file__).parent
FACTORY_NAMES = {"factory", "_factory"}


def _is_factory_build(node: ast.AST) -> bool:
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return False
    receiver = node.func.value
    name = receiver.id if isinstance(receiver, ast.Name) else getattr(receiver, "attr", None)
    return node.func.attr == "build" and name in FACTORY_NAMES


def _opens_callback_execution(node: ast.With) -> bool:
    return any(
        isinstance(item.context_expr, ast.Call)
        and isinstance(item.context_expr.func, ast.Name)
        and item.context_expr.func.id == "callback_execution"
        for item in node.items
    )


def _builds_outside(node: ast.AST, *, inside: bool) -> Iterator[int]:
    """Line of each ``factory.build(`` call not lexically inside ``with callback_execution()``."""
    if _is_factory_build(node) and not inside:
        yield node.lineno  # type: ignore[attr-defined]
    opened = inside or (isinstance(node, ast.With) and _opens_callback_execution(node))
    for child in ast.iter_child_nodes(node):
        yield from _builds_outside(child, inside=opened)


@pytest.fixture(scope="module")
def offending() -> list[str]:
    """``path:line`` of every build outside an execution, parsed once (immutable)."""
    found: list[str] = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found += [
            f"{path.relative_to(SOURCE)}:{line}" for line in _builds_outside(tree, inside=False)
        ]
    return found


class TestBuildCallSites:
    def test_every_factory_build_runs_inside_an_execution(self, offending: list[str]) -> None:
        assert offending == []

    def test_the_scan_sees_a_build_outside_and_accepts_one_inside_a_callback_execution(
        self,
    ) -> None:
        sample = ast.parse(
            "factory.build(A)\n"
            "with callback_execution():\n"
            "    self._factory.build(B)\n"
            "self._factory.build(C)\n"
        )

        assert list(_builds_outside(sample, inside=False)) == [1, 4]
