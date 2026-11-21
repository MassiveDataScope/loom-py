"""Rule 1 of spec 019: a callable loom hands to code outside loom goes through the boundary.

Any function, lambda, bound method or closure passed as an argument, or
applied as a decorator, to a callee outside the ``loom`` package is wrapped by
``boundary(...)`` (:mod:`tests.helpers.foreign_calls` reads it from the
source). A framework that calls the methods of a loom class instead is
declared in :data:`FOREIGN_BASES`: every loom subclass of one of them derives
from :class:`~loom.core.errors.boundary.BoundaryClass`, and every method its
framework calls resolves to a wrapped one.
"""

from __future__ import annotations

import functools
import importlib
import inspect
from pathlib import Path
from typing import Final

import pytest

from loom.core.errors.boundary import BoundaryClass
from tests.helpers.foreign_calls import EXEMPT_CALLEES, ScanResult, scan
from tests.helpers.source_tree import SRC, source_files

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


@pytest.fixture(scope="module")
def loom_scan() -> ScanResult:
    """Every hand-off in ``src/loom``, scanned once."""
    return scan(list(source_files()))


def _offences(tmp_path: Path, body: str) -> list[str]:
    source = tmp_path / "sample.py"
    source.write_text(body)
    return [h.callee for h in scan([source], root=tmp_path).handoffs if h.offends]


class TestTheRule:
    @pytest.mark.parametrize(
        ("body", "callee"),
        [
            (
                "from pydantic_ai import Agent\n"
                "def register(agent: Agent) -> None:\n"
                "    @agent.output_validator\n"
                "    def validate(output: object) -> object:\n"
                "        return output\n",
                "pydantic_ai.Agent.output_validator",
            ),
            (
                "from pydantic_ai import RunContext\n"
                "def build(text: str) -> object:\n"
                "    async def render(ctx: RunContext) -> str:\n"
                "        return text\n"
                "    return render\n",
                "pydantic_ai.RunContext",
            ),
            (
                "from fastapi import FastAPI\n"
                "def mount(app: FastAPI) -> None:\n"
                "    async def serve() -> None: ...\n"
                "    app.add_api_route('/x', serve)\n",
                "fastapi.FastAPI.add_api_route",
            ),
            (
                "from celery import Celery\n"
                "app = Celery()\n"
                "def run() -> None: ...\n"
                "app.task(name='x')(run)\n",
                "celery.Celery.task",
            ),
            (
                "import threading\n"
                "class Bridge:\n"
                "    def start(self) -> None:\n"
                "        threading.Thread(target=self._drain).start()\n"
                "    def _drain(self) -> None: ...\n",
                "threading.Thread",
            ),
            (
                "from sqlalchemy import event\n"
                "def watch(engine: object) -> None:\n"
                "    event.listen(engine, 'begin', lambda connection: None)\n",
                "sqlalchemy.event.listen",
            ),
            (
                "def mount(app) -> None:\n    async def serve() -> None: ...\n    app.add(serve)\n",
                "<untyped receiver>",
            ),
        ],
        ids=[
            "decorator",
            "escaping-closure",
            "route",
            "applied",
            "bound-method",
            "lambda",
            "untyped-receiver",
        ],
    )
    def test_a_callable_handed_out_unwrapped_is_refused(
        self, tmp_path: Path, body: str, callee: str
    ) -> None:
        assert _offences(tmp_path, body) == [callee]

    @pytest.mark.parametrize(
        "body",
        [
            "from celery import Celery\n"
            "app = Celery()\n"
            "def run() -> None: ...\n"
            "app.task(name='x')(boundary(run, pass_through=P))\n",
            "from alembic.operations import Operations\n"
            "@Operations.implementation_for(A)\n"
            "@Operations.implementation_for(B)\n"
            "@boundary\n"
            "def apply(operations: object, op: object) -> None: ...\n",
            "from fastapi import FastAPI\n"
            "def mount(app: FastAPI) -> None:\n"
            "    async def serve() -> None: ...\n"
            "    handler = boundary(serve)\n"
            "    app.add_api_route('/x', handler)\n",
            "def order(rows: list[int]) -> list[int]:\n    return sorted(rows, key=lambda r: -r)\n",
            "from loom.core.di import LoomContainer\n"
            "def wire(container: LoomContainer) -> None:\n"
            "    def build() -> int:\n"
            "        return 1\n"
            "    container.register(build)\n",
        ],
        ids=["wrapped", "decorator-stack", "assigned-name", "exempt-callee", "loom-callee"],
    )
    def test_a_wrapped_callable_an_exempt_or_a_loom_callee_passes(
        self, tmp_path: Path, body: str
    ) -> None:
        assert _offences(tmp_path, body) == []


class TestLoomHandoffs:
    def test_every_callable_loom_hands_outside_loom_is_wrapped(self, loom_scan: ScanResult) -> None:
        assert loom_scan.offences == []

    def test_every_exempt_callee_is_used(self, loom_scan: ScanResult) -> None:
        used = {handoff.callee for handoff in loom_scan.handoffs}

        assert sorted(set(EXEMPT_CALLEES) - used) == []

    @pytest.mark.parametrize("base", FOREIGN_BASES)
    def test_every_framework_method_of_a_loom_subclass_is_wrapped(self, base: str) -> None:
        module, _, name = base.rpartition(".")
        foreign = getattr(pytest.importorskip(module), name)
        _import_modules_naming(name)

        unwrapped = [
            f"{sub.__qualname__}.{method}"
            for sub in _subclasses(foreign)
            if sub.__module__.startswith("loom.")
            for method in _framework_methods(sub)
            if not getattr(getattr(sub, method), "__loom_boundary__", False)
        ]

        assert unwrapped == []


def _framework_methods(cls: type) -> frozenset[str]:
    """The methods its framework calls on *cls*: every one if it is not a ``BoundaryClass``."""
    if not issubclass(cls, BoundaryClass):
        return frozenset(n for n, v in inspect.getmembers(cls) if inspect.isfunction(v))
    return frozenset(n for n in cls.BOUNDARY_METHODS if inspect.isfunction(getattr(cls, n, None)))


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
