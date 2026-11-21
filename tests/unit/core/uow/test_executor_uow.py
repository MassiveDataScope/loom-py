"""Unit tests for UseCaseExecutor UoW lifecycle integration."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.stores.uow.scope import current_scope
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Helpers — mock UoW
# ---------------------------------------------------------------------------


class RecordingUnitOfWork:
    """A unit of work that records how the executor drives it."""

    is_transactional: ClassVar[bool] = True

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.exits: list[tuple[object, object, object]] = []

    async def begin(self) -> None:
        self.calls.append("begin")

    async def commit(self) -> None:
        self.calls.append("commit")

    async def rollback(self) -> None:
        self.calls.append("rollback")

    async def __aenter__(self) -> RecordingUnitOfWork:
        self.calls.append("enter")
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.calls.append("exit")
        self.exits.append((exc[0], exc[1], exc[2]))


class RecordingUnitOfWorkFactory:
    """A factory that keeps every unit of work it creates."""

    def __init__(self) -> None:
        self.created: list[RecordingUnitOfWork] = []

    def create(self) -> RecordingUnitOfWork:
        uow = RecordingUnitOfWork()
        self.created.append(uow)
        return uow


class RecordingRepository:
    """A repository that records the lookups it answers with one entity."""

    def __init__(self, entity: object) -> None:
        self._entity = entity
        self.lookups: list[tuple[str, str]] = []

    async def get_by_id(self, obj_id: str, profile: str = "default") -> object:
        self.lookups.append((obj_id, profile))
        return self._entity


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


class TestExecutorUnitOfWork:
    @pytest.mark.asyncio
    async def test_no_uow_factory_runs_without_transaction(self) -> None:
        compiler = _compiler(_SimpleUC)
        executor = UseCaseExecutor(compiler)  # no uow_factory
        uc = _SimpleUC()
        result = await executor.execute(uc)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_uow_factory_creates_uow_per_execution(self) -> None:
        factory = RecordingUnitOfWorkFactory()
        compiler = _compiler(_SimpleUC)
        executor = UseCaseExecutor(compiler, coordinator=OrderedCommits({DEFAULT_STORE: factory}))

        uc = _SimpleUC()
        await executor.execute(uc)

        assert len(factory.created) == 1

    @pytest.mark.asyncio
    async def test_uow_entered_through_the_context_manager_protocol(self) -> None:
        """FR-001: the executor never calls ``begin`` itself."""
        factory = RecordingUnitOfWorkFactory()
        compiler = _compiler(_SimpleUC)
        executor = UseCaseExecutor(compiler, coordinator=OrderedCommits({DEFAULT_STORE: factory}))

        await executor.execute(_SimpleUC())

        assert factory.created[0].calls == ["enter", "exit"]

    @pytest.mark.asyncio
    async def test_uow_exited_cleanly_on_success(self) -> None:
        """FR-001: a clean ``__aexit__`` is the commit; the executor calls no method."""
        factory = RecordingUnitOfWorkFactory()
        compiler = _compiler(_SimpleUC)
        executor = UseCaseExecutor(compiler, coordinator=OrderedCommits({DEFAULT_STORE: factory}))

        await executor.execute(_SimpleUC())

        uow = factory.created[0]
        assert uow.exits == [(None, None, None)]
        assert uow.calls == ["enter", "exit"]

    @pytest.mark.asyncio
    async def test_uow_exited_with_the_error_on_failure(self) -> None:
        """FR-001: the failure reaches ``__aexit__``; the adapter decides the rollback."""
        factory = RecordingUnitOfWorkFactory()
        compiler = _compiler(_FailingUC)
        executor = UseCaseExecutor(compiler, coordinator=OrderedCommits({DEFAULT_STORE: factory}))

        failing_uc = _FailingUC()
        with pytest.raises(RuntimeError, match="intentional") as info:
            await executor.execute(failing_uc)

        uow = factory.created[0]
        assert [(exc_type, exc_val) for exc_type, exc_val, _ in uow.exits] == [
            (RuntimeError, info.value)
        ]
        assert uow.calls == ["enter", "exit"]


# ---------------------------------------------------------------------------
# Nesting: inner execute reuses outer UoW
# ---------------------------------------------------------------------------


class _NestingUC(UseCase[Any, str]):
    """Runs ``_SimpleUC`` through the same executor from inside its own execution."""

    def __init__(self, executor: UseCaseExecutor) -> None:
        self._executor = executor

    async def execute(self, **kwargs: Any) -> str:
        return await self._executor.execute(_SimpleUC())


class TestNestedExecutionUnitOfWork:
    @pytest.mark.asyncio
    async def test_nested_execution_does_not_open_second_uow(self) -> None:
        factory = RecordingUnitOfWorkFactory()
        executor = UseCaseExecutor(
            _compiler(_SimpleUC, _NestingUC), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        await executor.execute(_NestingUC(executor))

        assert len(factory.created) == 1

    @pytest.mark.asyncio
    async def test_nested_execution_does_not_commit(self) -> None:
        factory = RecordingUnitOfWorkFactory()
        executor = UseCaseExecutor(
            _compiler(_SimpleUC, _NestingUC), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        await executor.execute(_NestingUC(executor))

        assert factory.created[0].exits == [(None, None, None)]

    @pytest.mark.asyncio
    async def test_no_scope_is_published_after_execution(self) -> None:
        factory = RecordingUnitOfWorkFactory()
        executor = UseCaseExecutor(
            _compiler(_SimpleUC), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        await executor.execute(_SimpleUC())

        assert current_scope() is None

    @pytest.mark.asyncio
    async def test_no_scope_is_published_after_failed_execution(self) -> None:
        factory = RecordingUnitOfWorkFactory()
        executor = UseCaseExecutor(
            _compiler(_FailingUC), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        use_case = _FailingUC()

        with pytest.raises(RuntimeError):
            await executor.execute(use_case)

        assert current_scope() is None


# ---------------------------------------------------------------------------
# 4.5 — LoadById with profile
# ---------------------------------------------------------------------------


class _EntityUC(UseCase[Any, str]):
    async def execute(self, entity_id: str, **kwargs: Any) -> str:
        return f"loaded:{entity_id}"


class TestLoadProfilePassedToRepo:
    @pytest.mark.asyncio
    async def test_load_default_profile_passed_to_repo(self) -> None:
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

        repo = RecordingRepository(MyEntity())

        compiler = _compiler(LoadUC)
        executor = UseCaseExecutor(compiler)
        await executor.execute(
            LoadUC(),
            params={"entity_id": "42"},
            dependencies={MyEntity: repo},
        )
        assert repo.lookups == [("42", "default")]

    @pytest.mark.asyncio
    async def test_load_custom_profile_passed_to_repo(self) -> None:
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

        repo = RecordingRepository(MyEntity())

        compiler = _compiler(LoadUC)
        executor = UseCaseExecutor(compiler)
        await executor.execute(
            LoadUC(),
            params={"entity_id": "7"},
            dependencies={MyEntity: repo},
        )
        assert repo.lookups == [("7", "detail")]
