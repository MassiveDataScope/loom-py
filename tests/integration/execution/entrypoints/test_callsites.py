"""No entry point builds outside an execution: a static scan of ``src/loom``.

Use cases and jobs are built by ``RuntimeExecutor.run``, which receives the
factory's ``build`` and calls it inside the execution it opens; no adapter
calls ``factory.build(...)`` itself. The one other builder is a job callback,
built inside ``callback_execution()`` (D7). Any other ``.build(`` call on a
factory is a site that builds before the execution exists (spec 011 S1).

What the scan sees: a call ``<receiver>.build(...)`` whose receiver ends in a
name containing ``factory`` (``factory``, ``self._factory``,
``application.kernel.factory``, ``use_case_factory``, ``UseCaseFactory``,
``make_factory()``), unless it is lexically inside ``with
callback_execution()``, plain or attribute-qualified
(``callback.callback_execution()``). ``.build(`` on anything else (a registry, a
span identity, a repository builder) is not a factory and is ignored.

Limits: a factory reached under a name without ``factory`` in it, a bound
method stored and called later (``build = factory.build; build(X)``, which is
how the executor receives it, on purpose), ``getattr(factory, "build")`` and a
``with`` entered through a variable (``scope = callback_execution(); with
scope:``) are not followed.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

import pytest

import loom

SOURCE = Path(loom.__file__).parent


def _last_name(node: ast.expr) -> str | None:
    """The name an expression ends in: ``a.b.c`` -> ``c``, ``f()`` -> ``f``."""
    match node:
        case ast.Name(id=name) | ast.Attribute(attr=name):
            return name
        case ast.Call(func=func):
            return _last_name(func)
        case _:
            return None


def _is_factory_build(node: ast.AST) -> bool:
    if not (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "build"
    ):
        return False
    name = _last_name(node.func.value)
    return name is not None and "factory" in name.lower()


def _opens_callback_execution(node: ast.With) -> bool:
    return any(
        isinstance(item.context_expr, ast.Call)
        and _last_name(item.context_expr.func) == "callback_execution"
        for item in node.items
    )


def _builds_outside(node: ast.AST, *, inside: bool) -> Iterator[int]:
    """Line of each factory ``.build(`` call not lexically inside ``with callback_execution()``."""
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

    def test_the_scan_flags_a_build_on_any_factory_outside_a_callback_execution(self) -> None:
        sample = ast.parse(
            "factory.build(A)\n"
            "self._factory.build(B)\n"
            "application.kernel.factory.build(C)\n"
            "use_case_factory.build(D)\n"
            "UseCaseFactory.build(factory, E)\n"
            "make_factory().build(F)\n"
        )

        assert list(_builds_outside(sample, inside=False)) == [1, 2, 3, 4, 5, 6]

    def test_the_scan_accepts_a_build_inside_a_plain_or_qualified_callback_execution(
        self,
    ) -> None:
        sample = ast.parse(
            "with callback_execution():\n"
            "    self._factory.build(A)\n"
            "with callback.callback_execution():\n"
            "    factory.build(B)\n"
        )

        assert list(_builds_outside(sample, inside=False)) == []

    def test_the_scan_ignores_build_on_what_is_not_a_factory(self) -> None:
        sample = ast.parse(
            "UseCaseRegistry.build(use_cases)\n"
            "SpanIdentity.build(name)\n"
            "builder.build(repository_type, model)\n"
            "self._deps.build(identity, container)\n"
        )

        assert list(_builds_outside(sample, inside=False)) == []
