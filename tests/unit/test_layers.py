"""Each layer imports only the layers inside it (ADR 0006).

The check reads the source, so an import inside a function counts; one under
``if TYPE_CHECKING:`` loads nothing and does not. Two tests complement it at
run time, catching what a module pulls in transitively:
``tests/unit/test_core_transport_containment.py`` for ``loom.core`` and
``tests/unit/composition/test_boundary.py`` for ``loom.composition``.
"""

from __future__ import annotations

import ast
from functools import cache
from pathlib import Path

import pytest

from tests.helpers.imports import imported_modules
from tests.helpers.source_tree import parsed, source_files

_SRC = Path(__file__).resolve().parents[2] / "src"

# layer → (packages it must not import at run time, modules allowed inside them).
_RULES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    # The core composes nothing: the composition root and the transports sit outside it.
    "loom.core": (
        ("loom.composition", "loom.rest", "loom.celery", "loom.ai", "loom.serverless", "loom.jobs"),
        (),
    ),
    # No transport but the JobService adapter and the ones app.entry selects, lazily.
    "loom.composition": (
        ("fastapi", "starlette", "celery", "loom.rest", "loom.celery", "loom.serverless"),
        (
            "loom.celery.service",
            "loom.celery.config",
            "loom.rest.fastapi.adapter",
            "loom.serverless.aws.lambda_.handler",
        ),
    ),
    # The acting identity sits under everything that acts: it reads no execution (spec 011, T09a).
    "loom.core.identity": (
        (
            "loom.core.execution",
            "loom.core.engine",
            "loom.core.di",
            "loom.core.use_case",
            "loom.core.uow",
            "opentelemetry",
        ),
        (),
    ),
    # The execution context sits under the container, the engine and the use cases (spec 011).
    "loom.core.execution": (
        ("loom.core.di", "loom.core.engine", "loom.core.use_case", "loom.ai"),
        (),
    ),
    # The container reads keys through an injected classifier, never the packages above it.
    "loom.core.di": (("loom.core.repository", "loom.core.use_case", "loom.ai"), ()),
    "loom.rest": (("loom.serverless",), ()),
    # A job backend raises its own errors; the composition root maps them.
    "loom.jobs": (("loom.composition",), ()),
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
        tree = parsed(path)
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
        "from loom import composition\n"
    )

    assert imported_modules(sample, package="loom.core.engine") == {
        "typing",
        "typing.TYPE_CHECKING",
        "loom.celery.service",
        "loom.celery.service.CeleryJobService",
        "loom.core.rest",
        "loom.core.rest.errors",
        "loom.core.engine",
        "loom.core.engine.sibling",
        "loom",
        "loom.composition",
    }


# Who may write the acting identity (spec 011, T09a): transports install it,
# work fixes it, and only the modules that touch each name may name it, matched
# exactly (a package entry is its ``__init__``, a re-export). ``_log`` is not
# fenced: it is the module-local logger's name in many modules.
_IDENTITY, _IDENTITY_CONTEXT = "loom.core.identity", "loom.core.identity.context"
_EXECUTION = "loom.core.execution.context"
_WRITERS: dict[str, tuple[str, ...]] = {
    "set_identity": (
        _IDENTITY,
        _IDENTITY_CONTEXT,
        "loom.rest.auth.middleware",
        "loom.core.job.envelope",
        "loom.core.job.service",
    ),
    "fix_identity": (_IDENTITY, _IDENTITY_CONTEXT, _EXECUTION),
    "enter": (
        "loom.core.execution",
        _EXECUTION,
        "loom.core.engine.executor",
        "loom.core.job.callback",
    ),
    "configure_delegation": (
        _IDENTITY,
        _IDENTITY_CONTEXT,
        "loom.composition.pipeline.registration",
    ),
    "_window": (_IDENTITY_CONTEXT,),
    "_acting": (_IDENTITY_CONTEXT, _EXECUTION),
    "_OUTSIDE_ACTING": (_EXECUTION,),
    "_CLOSED_ACTING": (_EXECUTION,),
}


def _read_names(tree: ast.AST) -> frozenset[str]:
    """Every name *tree* reads, as a name or an attribute, or imports under any alias."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    return frozenset(names)


@cache
def _names_in(path: Path) -> frozenset[str]:
    return _read_names(parsed(path))


class TestIdentityWriters:
    @pytest.mark.parametrize(("writer", "allowed"), _WRITERS.items(), ids=list(_WRITERS))
    def test_only_the_allowed_modules_write_the_acting_identity(
        self, writer: str, allowed: tuple[str, ...]
    ) -> None:
        offenders = []
        for path in source_files():
            module, _ = _module_of(path)
            if module not in allowed and writer in _names_in(path):
                offenders.append(module)

        assert offenders == []

    @pytest.mark.parametrize(
        "source",
        [
            "set_identity(a)",
            "identity.set_identity(a)",
            "install = set_identity",
            "install = identity.set_identity",
            "from loom.core.identity import set_identity as install",
            "from loom.core.identity.context import set_identity",
        ],
        ids=[
            "plain-call",
            "qualified-call",
            "name-load",
            "attribute-load",
            "aliased-import",
            "module-import",
        ],
    )
    def test_the_check_sees_every_way_to_reach_a_writer(self, source: str) -> None:
        assert "set_identity" in _read_names(ast.parse(source))

    def test_the_check_ignores_the_reset(self) -> None:
        assert "set_identity" not in _read_names(
            ast.parse("reset_identity(t)\nfrom x import reset_identity")
        )
