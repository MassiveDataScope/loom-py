from __future__ import annotations

from typing import Any

from loom.core.authz.elevation import elevate_scope, elevated_scopes

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case import UseCase
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory

WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
DECISION = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))


class Elevating(UseCase[Any, list[str]]):
    async def execute(self, value: str) -> list[str]:
        await elevate_scope("editor", DECISION, at=BOUNDARY, permission=WRITE, catalog=CATALOG)
        return sorted(elevated_scopes())


class Nested(UseCase[Any, list[str]]):
    def __init__(self, executor: RuntimeExecutor) -> None:
        self._executor = executor

    async def execute(self, value: str) -> list[str]:
        await elevate_scope("editor", DECISION, at=BOUNDARY, permission=WRITE, catalog=CATALOG)
        inner: list[str] = await self._executor.execute(Elevating(), value=value)  # type: ignore[arg-type]
        return inner + sorted(elevated_scopes())


def _executor(sink: FakeSink, *types: type) -> RuntimeExecutor:
    compiler = UseCaseCompiler()
    for use_case in types:
        compiler.compile(use_case)
    return RuntimeExecutor(compiler, uow_factory=StubUnitOfWorkFactory(Log()), elevation_sink=sink)


async def test_an_execution_runs_inside_a_frame_that_closes_with_it() -> None:
    sink = FakeSink(open=True)
    executor = _executor(sink, Elevating)

    result = await executor.execute(Elevating(), value="x")  # type: ignore[arg-type]

    assert result == ["editor"]
    assert sink.set == ["editor"]
    assert elevated_scopes() == frozenset()


async def test_a_nested_execution_inherits_the_frame_and_the_outer_keeps_its_state() -> None:
    sink = FakeSink(open=True)
    executor = _executor(sink, Elevating, Nested)

    result = await executor.execute(Nested(executor), value="x")  # type: ignore[arg-type]

    assert result == ["editor", "editor"]
    assert sink.cleared == []
    assert elevated_scopes() == frozenset()


async def test_without_a_sink_the_frame_still_exists_and_elevation_waits_for_the_provider() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(Elevating)
    executor = RuntimeExecutor(compiler)

    result = await executor.execute(Elevating(), value="x")  # type: ignore[arg-type]

    assert result == ["editor"]
    assert elevated_scopes() == frozenset()
