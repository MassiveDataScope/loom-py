"""Unit tests for RuntimeExecutor UoW lifecycle integration."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory
from loom.core.uow.scope import current_scope
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Helpers — mock UoW
# ---------------------------------------------------------------------------


def _make_uow_factory() -> tuple[MagicMock, MagicMock]:
    """Return (factory_mock, uow_mock) with async begin/commit/rollback."""
    uow = MagicMock(spec=UnitOfWork)
    type(uow).transactional = True  # a class attribute, as the protocol declares it
    uow.begin = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(spec=UnitOfWorkFactory)
    factory.create.return_value = uow

    return factory, uow


class _SimpleUC(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class _FailingUC(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        raise RuntimeError("intentional failure")


def _compiler(*uc_types: type[UseCase[Any, Any]]) -> UseCaseCompiler:
    c = UseCaseCompiler()
    for t in uc_types:
        c.compile(t)
    return c


# ---------------------------------------------------------------------------
# No UoW factory — backward compatible path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_uow_factory_runs_without_transaction() -> None:
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler)  # no uow_factory
    uc = _SimpleUC()
    result = await executor.execute(uc)
    assert result == "ok"


# ---------------------------------------------------------------------------
# UoW factory present — lifecycle on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uow_factory_creates_uow_per_execution() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    uc = _SimpleUC()
    await executor.execute(uc)

    factory.create.assert_called_once()


@pytest.mark.asyncio
async def test_uow_entered_through_the_context_manager_protocol() -> None:
    """FR-001: the executor never calls ``begin`` itself."""
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    await executor.execute(_SimpleUC())

    uow.__aenter__.assert_awaited_once()
    uow.begin.assert_not_awaited()


@pytest.mark.asyncio
async def test_uow_exited_cleanly_on_success() -> None:
    """FR-001: a clean ``__aexit__`` is the commit; the executor calls no method."""
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    await executor.execute(_SimpleUC())

    uow.__aexit__.assert_awaited_once_with(None, None, None)
    uow.commit.assert_not_awaited()
    uow.rollback.assert_not_awaited()


# ---------------------------------------------------------------------------
# UoW lifecycle on exception
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uow_exited_with_the_error_on_failure() -> None:
    """FR-001: the failure reaches ``__aexit__``; the adapter decides the rollback."""
    factory, uow = _make_uow_factory()
    compiler = _compiler(_FailingUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    failing_uc = _FailingUC()
    with pytest.raises(RuntimeError, match="intentional") as info:
        await executor.execute(failing_uc)

    uow.__aexit__.assert_awaited_once()
    exc_type, exc_val, _ = uow.__aexit__.await_args.args
    assert exc_type is RuntimeError
    assert exc_val is info.value
    uow.commit.assert_not_awaited()
    uow.rollback.assert_not_awaited()


# ---------------------------------------------------------------------------
# Nesting: inner execute reuses outer UoW
# ---------------------------------------------------------------------------


class _NestingUC(UseCase[Any, str]):
    """Runs ``_SimpleUC`` through the same executor from inside its own execution."""

    def __init__(self, executor: RuntimeExecutor) -> None:
        self._executor = executor

    async def execute(self, **kwargs: Any) -> str:
        return await self._executor.execute(_SimpleUC())


@pytest.mark.asyncio
async def test_nested_execution_does_not_open_second_uow() -> None:
    factory, _ = _make_uow_factory()
    executor = RuntimeExecutor(_compiler(_SimpleUC, _NestingUC), uow_factory=factory)

    await executor.execute(_NestingUC(executor))

    factory.create.assert_called_once()


@pytest.mark.asyncio
async def test_nested_execution_does_not_commit() -> None:
    factory, uow = _make_uow_factory()
    executor = RuntimeExecutor(_compiler(_SimpleUC, _NestingUC), uow_factory=factory)

    await executor.execute(_NestingUC(executor))

    uow.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_no_scope_is_published_after_execution() -> None:
    factory, _ = _make_uow_factory()
    executor = RuntimeExecutor(_compiler(_SimpleUC), uow_factory=factory)

    await executor.execute(_SimpleUC())

    assert current_scope() is None


@pytest.mark.asyncio
async def test_no_scope_is_published_after_failed_execution() -> None:
    factory, _ = _make_uow_factory()
    executor = RuntimeExecutor(_compiler(_FailingUC), uow_factory=factory)

    with pytest.raises(RuntimeError):
        await executor.execute(_FailingUC())

    assert current_scope() is None


# ---------------------------------------------------------------------------
# 4.5 — LoadById with profile
# ---------------------------------------------------------------------------


class _EntityUC(UseCase[Any, str]):
    async def execute(self, entity_id: str, **kwargs: Any) -> str:
        return f"loaded:{entity_id}"


@pytest.mark.asyncio
async def test_load_default_profile_passed_to_repo() -> None:
    from loom.core.use_case.markers import LoadById
    from loom.core.use_case.use_case import UseCase

    class MyEntity:
        pass

    class LoadUC(UseCase[Any, str]):
        async def execute(
            self,
            entity_id: str,
            entity: MyEntity = LoadById(MyEntity, by="entity_id"),
        ) -> str:
            return "ok"

    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=MyEntity())

    compiler = _compiler(LoadUC)
    executor = RuntimeExecutor(compiler)
    await executor.execute(
        LoadUC(),
        params={"entity_id": "42"},
        dependencies={MyEntity: repo},
    )
    repo.get_by_id.assert_awaited_once_with("42", profile="default")


@pytest.mark.asyncio
async def test_load_custom_profile_passed_to_repo() -> None:
    from loom.core.use_case.markers import LoadById
    from loom.core.use_case.use_case import UseCase

    class MyEntity:
        pass

    class LoadUC(UseCase[Any, str]):
        async def execute(
            self,
            entity_id: str,
            entity: MyEntity = LoadById(MyEntity, by="entity_id", profile="detail"),
        ) -> str:
            return "ok"

    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=MyEntity())

    compiler = _compiler(LoadUC)
    executor = RuntimeExecutor(compiler)
    await executor.execute(
        LoadUC(),
        params={"entity_id": "7"},
        dependencies={MyEntity: repo},
    )
    repo.get_by_id.assert_awaited_once_with("7", profile="detail")
