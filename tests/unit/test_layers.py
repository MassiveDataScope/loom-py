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
    # The core composes nothing: the composition root, transports and store adapters sit outside it.
    "loom.core": (
        (
            "loom.composition",
            "loom.rest",
            "loom.celery",
            "loom.ai",
            "loom.serverless",
            "loom.jobs",
            "loom.stores",
            "sqlalchemy",
        ),
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
            "loom.core.stores.uow",
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
    "loom.core.di": (
        ("loom.core.stores", "loom.core.use_case", "loom.ai"),
        (),
    ),
    "loom.core.engine": (("loom.core.observability", "opentelemetry"), ()),
    "loom.rest": (("loom.serverless",), ()),
    # A job transport raises its own errors; the composition root maps them.
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
        "    from loom.celery.service import CeleryJobTransport\n"
        "from ..rest import errors\n"
        "from . import sibling\n"
        "from loom import composition\n"
    )

    assert imported_modules(sample, package="loom.core.engine") == {
        "typing",
        "typing.TYPE_CHECKING",
        "loom.celery.service",
        "loom.celery.service.CeleryJobTransport",
        "loom.core.rest",
        "loom.core.rest.errors",
        "loom.core.engine",
        "loom.core.engine.sibling",
        "loom",
        "loom.composition",
    }


_NAMED_TRANSPORTS = ("loom.rest", "loom.serverless")


def _imported_by_name(tree: ast.AST) -> list[str]:
    """The string literals *tree* passes to ``importlib.import_module`` or ``import_module``."""
    names = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        function = node.func
        called = (
            function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", "")
        )
        argument = node.args[0]
        if (
            called == "import_module"
            and isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
        ):
            names.append(argument.value)
    return names


def test_the_composition_names_no_transport_in_an_import_by_name() -> None:
    offenders = [
        f"{path.relative_to(_SRC)} imports {name} by name"
        for path in sorted((_SRC / "loom" / "composition").rglob("*.py"))
        for name in _imported_by_name(parsed(path))
        if any(_within(name, transport) for transport in _NAMED_TRANSPORTS)
    ]

    assert offenders == []


def test_the_import_by_name_check_sees_both_call_forms() -> None:
    sample = ast.parse(
        "import importlib\n"
        "importlib.import_module('loom.rest')\n"
        "import_module('loom.serverless.x')\n"
        "importlib.import_module(name)\n"
    )

    assert _imported_by_name(sample) == ["loom.rest", "loom.serverless.x"]


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
        "loom.jobs.inline.transport",
    ),
    "pinned_for": (_IDENTITY_CONTEXT, _EXECUTION),
    "enter": (
        "loom.core.execution",
        _EXECUTION,
        "loom.core.engine.executor",
    ),
    "configure_delegation": (
        _IDENTITY,
        _IDENTITY_CONTEXT,
        "loom.composition.phases",
    ),
    "_max_delegation_seconds": (_IDENTITY_CONTEXT,),
    "_acting": (_IDENTITY_CONTEXT, _EXECUTION),
    "_NO_EXECUTION_ACTING": (_EXECUTION,),
    "_CLOSED_CONTEXT_ACTING": (_EXECUTION,),
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


class TestCoreNamesNoStoreIsland:
    @staticmethod
    def _islands(tree: ast.AST) -> list[str]:
        """Every string literal passed as the island of an ``import_optional`` call in *tree*."""
        islands: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            callee = node.func
            name = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", None)
            island = node.args[0]
            if name == "import_optional" and isinstance(island, ast.Constant):
                islands.append(str(island.value))
        return islands

    def test_no_core_module_loads_a_store_adapter_by_name(self) -> None:
        offenders = []
        for path in sorted((_SRC / "loom" / "core").rglob("*.py")):
            module, _ = _module_of(path)
            offenders.extend(
                f"{module} loads {island}"
                for island in self._islands(parsed(path))
                if _within(island, "loom.stores")
            )

        assert offenders == []

    @pytest.mark.parametrize(
        "source",
        [
            'import_optional("loom.stores.clickhouse", extra="clickhouse")',
            'optional.import_optional("loom.stores.mongo", extra="mongo")',
        ],
        ids=["plain-call", "qualified-call"],
    )
    def test_the_check_sees_the_island_literal(self, source: str) -> None:
        assert self._islands(ast.parse(source))[0].startswith("loom.stores.")

    def test_the_check_ignores_a_computed_island(self) -> None:
        assert self._islands(ast.parse('import_optional(name, extra="x")')) == []
