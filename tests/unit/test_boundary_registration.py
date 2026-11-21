"""Rule 1 of spec 019: what loom hands to a foreign framework goes through the loom boundary.

:data:`REGISTRATION_APIS` is the one place a foreign registration API is
declared: a Celery task or signal, a Prefect flow, task or hook, a Typer
command, a pydantic-ai tool, a Starlette exception handler, a SQLAlchemy
listener, a bytewax operator. Every call in ``src/loom`` that hands one of them
a callable must hand it ``boundary(...)``, inline or through a name the same
function assigned it to; a decorator must sit right above ``@boundary``.

A framework that calls the methods of a loom class instead (a bytewax
partition, source or sink, a pydantic-ai toolset) is declared in
:data:`FOREIGN_BASES`: every loom subclass of one of them derives from
:class:`~loom.core.errors.boundary.BoundaryClass`, which wraps those methods.
Starlette middleware classes sit inside ``BoundaryMiddleware``, which a REST
test pins as the outermost one.
"""

from __future__ import annotations

import ast
import functools
import importlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

from loom.core.errors.boundary import BoundaryClass
from tests.helpers.source_tree import SRC, source_files

APPLIED: Final = "applied"
"""The call's result takes the callable: ``app.task(...)(fn)`` or a decorator."""

_PREFECT_HOOKS: Final = ("on_failure", "on_completion", "on_crashed", "on_cancellation")

_BYTEWAX_OPERATORS: Final = (
    "branch",
    "filter",
    "filter_map",
    "filter_value",
    "flat_map",
    "flat_map_batch",
    "flat_map_value",
    "inspect",
    "key_on",
    "map",
    "map_value",
    "stateful_flat_map",
    "stateful_map",
)


@dataclass(frozen=True)
class Registration:
    """A foreign API that takes a callable: the tail of its dotted name and where the callable goes.

    Attributes:
        tail: The end of the called name, imports resolved: ``.task``, ``event.listen``.
        slots: Positional indexes, keyword names or :data:`APPLIED`.
    """

    tail: str
    slots: tuple[int | str, ...]

    def names(self, called: str) -> bool:
        """Whether *called*, a dotted name with its imports resolved, is this API."""
        return called == self.tail.lstrip(".") or called.endswith(self.tail)


REGISTRATION_APIS: Final[tuple[Registration, ...]] = (
    Registration(".task", (APPLIED, *_PREFECT_HOOKS)),
    Registration(".connect", (0,)),
    Registration("prefect.flow", (APPLIED, "flow_run_name", *_PREFECT_HOOKS)),
    Registration(".command", (APPLIED,)),
    Registration(".callback", (APPLIED,)),
    Registration("event.listen", (2,)),
    Registration("event.listens_for", (APPLIED,)),
    Registration(".add_exception_handler", (1,)),
    Registration("Tool.from_schema", (0,)),
    Registration("pydantic_ai.Tool", (0,)),
    *(Registration(f"bytewax.operators.{name}", (2,)) for name in _BYTEWAX_OPERATORS),
)
"""Every foreign registration API loom hands a callable to. Adding one declares a new framework."""

FOREIGN_BASES: Final[tuple[str, ...]] = (
    "bytewax.inputs.FixedPartitionedSource",
    "bytewax.inputs.StatefulSourcePartition",
    "bytewax.inputs.DynamicSource",
    "bytewax.inputs.StatelessSourcePartition",
    "bytewax.outputs.DynamicSink",
    "bytewax.outputs.StatelessSinkPartition",
    "bytewax.outputs.FixedPartitionedSink",
    "bytewax.outputs.StatefulSinkPartition",
    "pydantic_ai.toolsets.AbstractToolset",
)
"""Foreign classes whose methods their framework calls on a loom subclass."""


@dataclass(frozen=True)
class Unwrapped:
    """A callable handed to a foreign API without the boundary."""

    path: str
    line: int
    api: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.api} takes a callable not wrapped by boundary(...)"


class _RegistrationChecker(ast.NodeVisitor):
    """Find each registration call in one file and check what it is handed."""

    def __init__(self, path: str, tree: ast.Module) -> None:
        self.path = path
        self.offences: list[Unwrapped] = []
        self._imports = _imports_of(tree)
        self._wrapped_names: list[set[str]] = [set()]
        self._applied: dict[int, ast.expr] = {}
        self._decorators: dict[int, ast.expr | None] = {}

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        decorators = node.decorator_list
        for index, above in enumerate(decorators):
            self._decorators[id(above)] = (
                decorators[index + 1] if index + 1 < len(decorators) else None
            )
        self._wrapped_names.append(_names_assigned_boundary(node))
        self.generic_visit(node)
        self._wrapped_names.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Module(self, node: ast.Module) -> None:
        self._wrapped_names = [_names_assigned_boundary(node)]
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Call) and node.args:
            self._applied[id(node.func)] = node.args[0]
        called = self._qualified(node.func)
        for api in REGISTRATION_APIS:
            if called is not None and api.names(called):
                self._check(node, api)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self._check_bare_decorator(node)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        self._check_bare_decorator(node)

    def _check_bare_decorator(self, node: ast.Attribute | ast.Name) -> None:
        if id(node) not in self._decorators:
            return
        called = self._qualified(node)
        for api in REGISTRATION_APIS:
            if called is not None and api.names(called) and APPLIED in api.slots:
                self._expect(self._decorators[id(node)], node, api)

    def _check(self, node: ast.Call, api: Registration) -> None:
        for slot in api.slots:
            for handed in self._handed(node, slot):
                self._expect(handed, node, api)

    def _handed(self, node: ast.Call, slot: int | str) -> Iterator[ast.expr | None]:
        if slot == APPLIED:
            if id(node) in self._decorators:
                yield self._decorators[id(node)]
            elif id(node) in self._applied:
                yield self._applied[id(node)]
        elif isinstance(slot, int):
            if len(node.args) > slot:
                yield node.args[slot]
        else:
            yield from (k.value for k in node.keywords if k.arg == slot)

    def _expect(self, handed: ast.expr | None, node: ast.AST, api: Registration) -> None:
        if not self._is_wrapped(handed):
            self.offences.append(Unwrapped(self.path, getattr(node, "lineno", 0), api.tail))

    def _is_wrapped(self, node: ast.expr | None) -> bool:
        match node:
            case ast.Call(func=ast.Name(id="boundary")) | ast.Name(id="boundary"):
                return True
            case ast.Name(id=name):
                return any(name in scope for scope in self._wrapped_names)
            case ast.List(elts=items) | ast.Tuple(elts=items):
                return all(self._is_wrapped(item) for item in items)
            case ast.ListComp(elt=item):
                return self._is_wrapped(item)
            case ast.BoolOp(values=values):
                return all(self._is_wrapped(value) for value in values)
            case ast.Constant(value=None):
                return True
        return False

    def _qualified(self, node: ast.expr) -> str | None:
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name):
            return None
        parts.append(self._imports.get(node.id, node.id))
        return ".".join(reversed(parts))


def _imports_of(tree: ast.Module) -> dict[str, str]:
    """Each name a file imports, mapped to the dotted name it stands for."""
    imported: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            for alias in node.names:
                imported[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name] = alias.name
    return imported


def _names_assigned_boundary(scope: ast.AST) -> set[str]:
    """The names *scope* itself assigns ``boundary(...)`` to."""
    names: set[str] = set()
    for node in ast.iter_child_nodes(scope):
        match node:
            case ast.Assign(targets=targets, value=ast.Call(func=ast.Name(id="boundary"))):
                names.update(target.id for target in targets if isinstance(target, ast.Name))
            case ast.AnnAssign(
                target=ast.Name(id=name), value=ast.Call(func=ast.Name(id="boundary"))
            ):
                names.add(name)
    return names


def unwrapped_registrations(paths: list[Path]) -> list[Unwrapped]:
    """Every callable in *paths* handed to a registration API without the boundary."""
    offences: list[Unwrapped] = []
    for path in sorted(paths):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        checker = _RegistrationChecker(str(path), tree)
        checker.visit(tree)
        offences.extend(checker.offences)
    return offences


def _rules(tmp_path: Path, body: str) -> list[str]:
    source = tmp_path / "sample.py"
    source.write_text(body)
    return [offence.api for offence in unwrapped_registrations([source])]


class TestRegistrationRule:
    @pytest.mark.parametrize(
        ("body", "api"),
        [
            ("app.task(name='x')(run)\n", ".task"),
            ("signal.connect(on_ready, weak=False)\n", ".connect"),
            ("from sqlalchemy import event\nevent.listen(t, 'begin', on_begin)\n", "event.listen"),
            ("import prefect\nprefect.flow(on_failure=[hook])(boundary(body))\n", "prefect.flow"),
            ("app.add_exception_handler(KeyError, handle)\n", ".add_exception_handler"),
            ("from bytewax.operators import map as bw_map\nbw_map('s', up, step)\n", "bytewax"),
            (
                "from sqlalchemy import event\n"
                "@event.listens_for(engine, 'reset')\ndef reset() -> None: ...\n",
                "event.listens_for",
            ),
            ("from prefect import task\n@task\ndef mark() -> None: ...\n", ".task"),
        ],
        ids=[
            "applied",
            "positional",
            "listener",
            "hook",
            "handler",
            "operator",
            "decorator",
            "bare",
        ],
    )
    def test_a_callable_handed_without_the_boundary_is_refused(
        self, tmp_path: Path, body: str, api: str
    ) -> None:
        [found] = _rules(tmp_path, body)

        assert found.startswith(api)

    @pytest.mark.parametrize(
        "body",
        [
            "app.task(name='x')(boundary(run, pass_through=P))\n",
            "import prefect\nprefect.flow(on_failure=[boundary(h) for h in hooks])(boundary(b))\n",
            "def build() -> None:\n    body = boundary(run)\n    app.command('x')(body)\n",
            "from sqlalchemy import event\n"
            "@event.listens_for(engine, 'reset')\n@boundary\ndef reset() -> None: ...\n",
            "client.command('OPTIMIZE TABLE orders FINAL')\n",
            "async def go() -> None:\n    async with engine.connect() as c: ...\n",
        ],
        ids=["inline", "hooks", "assigned-name", "decorator", "not-applied", "no-callable"],
    )
    def test_a_wrapped_callable_or_a_lookalike_passes(self, tmp_path: Path, body: str) -> None:
        assert _rules(tmp_path, body) == []


class TestLoomRegistrations:
    def test_every_callable_loom_hands_a_foreign_framework_is_wrapped(self) -> None:
        assert [str(o) for o in unwrapped_registrations(list(source_files()))] == []

    @pytest.mark.parametrize("base", FOREIGN_BASES)
    def test_every_loom_subclass_of_a_foreign_base_wraps_its_methods(self, base: str) -> None:
        module, _, name = base.rpartition(".")
        foreign = getattr(pytest.importorskip(module), name)
        _import_modules_naming(name)

        unwrapped = [
            f"{sub.__module__}.{sub.__qualname__}"
            for sub in _subclasses(foreign)
            if sub.__module__.startswith("loom.") and not issubclass(sub, BoundaryClass)
        ]

        assert unwrapped == []


@functools.cache
def _sources() -> tuple[tuple[str, str], ...]:
    """Each module of ``src/loom`` with its source, read once for every foreign base."""
    return tuple(
        (".".join(path.relative_to(SRC.parent).with_suffix("").parts), path.read_text("utf-8"))
        for path in source_files()
    )


def _import_modules_naming(name: str) -> None:
    """Import every module of ``src/loom`` whose source names *name*, so its subclasses exist."""
    for dotted, source in _sources():
        if name in source:
            importlib.import_module(dotted.removesuffix(".__init__"))


def _subclasses(base: type) -> set[type]:
    found: set[type] = set()
    pending = [base]
    while pending:
        for sub in pending.pop().__subclasses__():
            if sub not in found:
                found.add(sub)
                pending.append(sub)
    return found
