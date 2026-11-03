from __future__ import annotations

from typing import Any

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.scope import NestedElevation
from loom.core.use_case import UseCase
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory
from tests.unit.core.uow._coordinator_fakes import Factory, Ledger, PhasedStore

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
        inner: list[str] = await self._executor.execute(Elevating(), params={"value": value})  # type: ignore[arg-type]
        return inner + sorted(elevated_scopes())


def _executor(sink: FakeSink, *types: type) -> RuntimeExecutor:
    compiler = UseCaseCompiler()
    for use_case in types:
        compiler.compile(use_case)
    return RuntimeExecutor(compiler, uow_factory=StubUnitOfWorkFactory(Log()), elevation_sink=sink)


async def test_an_execution_runs_inside_a_frame_that_closes_with_it() -> None:
    sink = FakeSink(open=True)
    executor = _executor(sink, Elevating)

    result = await executor.execute(Elevating(), params={"value": "x"})  # type: ignore[arg-type]

    assert result == ["editor"]
    assert sink.set == ["editor"]
    assert elevated_scopes() == frozenset()


async def test_a_nested_execution_after_an_elevation_is_refused_and_the_frame_closes() -> None:
    sink = FakeSink(open=True)
    executor = _executor(sink, Elevating, Nested)
    nested = Nested(executor)

    with pytest.raises(NestedElevation, match="Elevating cannot run inside Nested"):
        await executor.execute(nested, params={"value": "x"})  # type: ignore[arg-type]

    assert elevated_scopes() == frozenset()


async def test_without_a_sink_the_frame_still_exists_and_elevation_waits_for_the_provider() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(Elevating)
    executor = RuntimeExecutor(compiler)

    result = await executor.execute(Elevating(), params={"value": "x"})  # type: ignore[arg-type]

    assert result == ["editor"]
    assert elevated_scopes() == frozenset()


class _SeesElevation(PhasedStore):
    """Records the elevated scopes visible when the coordinator flushes and commits.

    The session-settings provider reads them in ``after_begin``, and a
    transaction whose first SQL is the coordinator's flush begins there.
    """

    def __init__(self) -> None:
        super().__init__(DEFAULT_STORE, Ledger())
        self.seen: dict[str, list[str]] = {}

    async def flush(self) -> None:
        self.seen["flush"] = sorted(elevated_scopes())
        await super().flush()

    async def commit(self) -> None:
        self.seen["commit"] = sorted(elevated_scopes())
        await super().commit()


async def _elevate_then_finish() -> _SeesElevation:
    store = _SeesElevation()
    compiler = UseCaseCompiler()
    compiler.compile(Elevating)
    executor = RuntimeExecutor(compiler, uow_factory=Factory(store))

    await executor.execute(Elevating(), params={"value": "x"})  # type: ignore[arg-type]

    return store


async def test_the_elevated_scopes_are_still_visible_when_the_coordinator_flushes() -> None:
    store = await _elevate_then_finish()

    assert store.seen["flush"] == ["editor"]


async def test_the_elevated_scopes_are_still_visible_when_the_coordinator_commits() -> None:
    store = await _elevate_then_finish()

    assert store.seen["commit"] == ["editor"]
