"""Characterisation of today's single-store unit of work (ADR 0008, T039).

Pins the behaviour a store-catalog migration must keep: a use case commits on
success, rolls back when ``execute`` raises, a nested invocation joins that one
unit of work, a read outside it sees committed rows, and post-commit actions
run only after the commit.

Of the two faults the ADR names, fault A (a repository of another engine
writing into the active session) now fails closed with ``ForeignSessionError``
until the invocation scope gives each store its own unit of work. Fault B
stays a strict xfail, reproduced on the own-session path (see that test).
"""

from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import sqlite3
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from loom.composition import Application, CompositionSource, Recipe, build_application
from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes, elevation_scope
from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.post_commit import active_channel
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.abc import CursorCodec, PageParams, RepoFor
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.rls import SQLAlchemyElevationSink
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import ForeignSessionError, transactional
from loom.core.repository.sqlalchemy.uow import (
    SQLAlchemyUnitOfWork,
    SQLAlchemyUnitOfWorkFactory,
)
from loom.core.uow.scope import current_scope
from loom.core.use_case import Input, UseCase
from loom.core.use_case.invoker import ApplicationInvoker
from tests.helpers.composition import ProjectFactory, fixture_project

__all__ = ["fixture_project"]

_RECORD = "config_records_fixture"
_NOTE = "characterisation_notes"
_EXECUTE_FAILED = "execute failed after create"
_OUTER_FAILED = "outer fails after both creates"
_FAULT_B = "ADR 0008: a write to another store commits on its own, outside the unit of work"

_UseCases = Callable[[type[BaseModel]], tuple[type, ...]]


class _Named(Command, frozen=True):
    name: str


class _Note(BaseModel):
    __tablename__ = _NOTE

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class _NoteCommand(Command, frozen=True):
    name: str


class _SingleStoreAdapter:
    """Minimal adapter: the discovered project plus the test's own use cases."""

    def __init__(self, build_use_cases: _UseCases) -> None:
        self._build_use_cases = build_use_cases
        self.use_cases: tuple[type, ...] = ()

    def recipe(self, source: CompositionSource) -> Recipe:
        discovered = source.discover()
        model = _config_record(discovered)
        self.use_cases = self._build_use_cases(model)
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=dataclasses.replace(
                discovered, use_cases=(*discovered.use_cases, *self.use_cases)
            ),
            backend=source.backend(default="sqlalchemy"),
        )

    def bind(self, application: Application) -> Application:
        return application


def _config_record(discovered: DiscoveryResult) -> type[BaseModel]:
    records = [model for model in discovered.models if model.__name__ == "ConfigRecord"]
    (record,) = records
    return record


def _sqlite_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


def _count_rows(path: Path, table: str) -> int:
    """Row count through a connection that is not the repository's."""
    with sqlite3.connect(path) as connection:
        row = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    if row is None:
        raise RuntimeError(f"{table} returned no count")
    return int(row[0])


def _compose(
    config_path: str, build_use_cases: _UseCases
) -> tuple[Application, tuple[type, ...], type[BaseModel]]:
    adapter = _SingleStoreAdapter(build_use_cases)
    application = build_application(config_path, adapter=adapter, environment="test")
    factory = application.kernel.executor._uow_factory  # noqa: SLF001
    assert isinstance(factory, SQLAlchemyUnitOfWorkFactory)
    return application, adapter.use_cases, _config_record(application.discovered)


def _project(fixture_project: ProjectFactory, db_path: Path, name: str) -> str:
    return fixture_project(name, store={"backend": "sqlalchemy", "url": _sqlite_url(db_path)})


def _creating(*, fail: bool) -> _UseCases:
    def build(model: type[BaseModel]) -> tuple[type, ...]:
        class _Create(UseCase[model, int]):  # type: ignore[valid-type]
            async def execute(self, cmd: _Named = Input()) -> int:
                await self.main_repo.create(cmd)
                if fail:
                    raise RuntimeError(_EXECUTE_FAILED)
                return 1

        return (_Create,)

    return build


def _returning_id(model: type[BaseModel]) -> tuple[type, ...]:
    class _Create(UseCase[model, int]):  # type: ignore[valid-type]
        async def execute(self, cmd: _Named = Input()) -> int:
            created = await self.main_repo.create(cmd)
            return int(created.id)

    return (_Create,)


def _nesting(*, fail: bool) -> _UseCases:
    def build(model: type[BaseModel]) -> tuple[type, ...]:
        class _Inner(UseCase[model, int]):  # type: ignore[valid-type]
            async def execute(self, cmd: _Named = Input()) -> int:
                await self.main_repo.create(cmd)
                return 1

        class _Outer(UseCase[model, int]):  # type: ignore[valid-type]
            def __init__(self, main_repo: RepoFor[Any], app: ApplicationInvoker) -> None:
                super().__init__(main_repo)
                self._app = app

            async def execute(self, cmd: _Named = Input()) -> int:
                await self.main_repo.create(cmd)
                await self._app.invoke(_Inner, payload={"name": "inner"})
                if fail:
                    raise RuntimeError(_OUTER_FAILED)
                return 1

        # ``from __future__ import annotations`` would leave the local model
        # unresolved, so the factory would not inject this repository.
        _Outer.__init__.__annotations__ = {
            "main_repo": RepoFor[model],
            "app": ApplicationInvoker,
            "return": None,
        }
        return (_Outer, _Inner)

    return build


def _enqueueing(action: Callable[[], None], *, fail: bool) -> _UseCases:
    def build(model: type[BaseModel]) -> tuple[type, ...]:
        class _Create(UseCase[model, int]):  # type: ignore[valid-type]
            async def execute(self, cmd: _Named = Input()) -> int:
                await self.main_repo.create(cmd)
                channel = active_channel()
                assert channel is not None
                channel.enqueue(action)
                if fail:
                    raise RuntimeError(_EXECUTE_FAILED)
                return 1

        return (_Create,)

    return build


def _spy_commits(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    commits: list[int] = []
    original = AsyncSession.commit

    async def commit(self: AsyncSession) -> None:
        commits.append(1)
        await original(self)

    monkeypatch.setattr(AsyncSession, "commit", commit)
    return commits


def _spy_units(monkeypatch: pytest.MonkeyPatch, application: Application) -> list[int]:
    factory = application.kernel.executor._uow_factory  # noqa: SLF001
    assert isinstance(factory, SQLAlchemyUnitOfWorkFactory)
    opened: list[int] = []
    original = factory.create

    def create() -> Any:
        opened.append(1)
        return original()

    monkeypatch.setattr(factory, "create", create)
    return opened


async def test_a_use_case_that_creates_a_row_commits_it_on_success(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    application, (use_case,), _model = _compose(
        _project(fixture_project, db_path, "commit"), _creating(fail=False)
    )

    async with application.running():
        await application.kernel.app.invoke(use_case, payload={"name": "kept"})

    assert _count_rows(db_path, _RECORD) == 1


async def test_an_exception_in_execute_after_create_rolls_the_row_back(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    application, (use_case,), _model = _compose(
        _project(fixture_project, db_path, "rollback"), _creating(fail=True)
    )

    async def invoke_failing() -> None:
        async with application.running():
            await application.kernel.app.invoke(use_case, payload={"name": "lost"})

    with pytest.raises(RuntimeError, match=_EXECUTE_FAILED):
        await invoke_failing()

    assert _count_rows(db_path, _RECORD) == 0


async def test_a_nested_use_case_joins_the_outer_unit_of_work_and_commits_once(
    fixture_project: ProjectFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "store.db"
    application, (outer, _inner), _model = _compose(
        _project(fixture_project, db_path, "nested_commit"), _nesting(fail=False)
    )
    commits = _spy_commits(monkeypatch)
    opened = _spy_units(monkeypatch, application)

    async with application.running():
        await application.kernel.app.invoke(outer, payload={"name": "outer"})

    assert len(opened) == 1
    assert len(commits) == 1
    assert _count_rows(db_path, _RECORD) == 2


async def test_a_nested_write_rolls_back_when_the_outer_use_case_fails(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    application, (outer, _inner), _model = _compose(
        _project(fixture_project, db_path, "nested_rollback"), _nesting(fail=True)
    )

    async def invoke_failing() -> None:
        async with application.running():
            await application.kernel.app.invoke(outer, payload={"name": "outer"})

    with pytest.raises(RuntimeError, match=_OUTER_FAILED):
        await invoke_failing()

    assert _count_rows(db_path, _RECORD) == 0


async def test_a_read_outside_any_unit_of_work_sees_committed_rows(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    application, (use_case,), model = _compose(
        _project(fixture_project, db_path, "read"), _returning_id
    )

    async with application.running():
        created_id = await application.kernel.app.invoke(use_case, payload={"name": "kept"})
        assert current_scope() is None
        repository = application.kernel.container.resolve_repo(model)
        found = await repository.get_by_id(created_id)
        page = await repository.list_paginated(PageParams(page=1, limit=10))

    assert found is not None
    assert found.name == "kept"
    assert [item.name for item in page.items] == ["kept"]
    assert _count_rows(db_path, _RECORD) == 1


async def test_a_post_commit_action_runs_after_commit_and_sees_the_row(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    seen: list[int] = []

    def record() -> None:
        seen.append(_count_rows(db_path, _RECORD))

    application, (use_case,), _model = _compose(
        _project(fixture_project, db_path, "post_commit"), _enqueueing(record, fail=False)
    )

    async with application.running():
        await application.kernel.app.invoke(use_case, payload={"name": "kept"})

    assert seen == [1]


async def test_a_post_commit_action_does_not_run_when_execute_raises(
    fixture_project: ProjectFactory, tmp_path: Path
) -> None:
    db_path = tmp_path / "store.db"
    seen: list[int] = []
    application, (use_case,), _model = _compose(
        _project(fixture_project, db_path, "post_commit_fail"),
        _enqueueing(lambda: seen.append(_count_rows(db_path, _RECORD)), fail=True),
    )

    async def invoke_failing() -> None:
        async with application.running():
            await application.kernel.app.invoke(use_case, payload={"name": "lost"})

    with pytest.raises(RuntimeError, match=_EXECUTE_FAILED):
        await invoke_failing()

    assert seen == []
    assert _count_rows(db_path, _RECORD) == 0


def _session_manager(path: Path) -> SessionManager:
    return SessionManager(
        _sqlite_url(path),
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )


async def _create_schema(manager: SessionManager) -> None:
    compiled = get_compiled(_Note)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)


@pytest.fixture
async def two_stores(
    tmp_path: Path,
) -> AsyncIterator[tuple[SessionManager, SessionManager, Path, Path]]:
    compile_all(_Note)
    first_path = tmp_path / "db1.sqlite"
    second_path = tmp_path / "db2.sqlite"
    first = _session_manager(first_path)
    second = _session_manager(second_path)
    try:
        await _create_schema(first)
        await _create_schema(second)
        yield first, second, first_path, second_path
    finally:
        await first.dispose()
        await second.dispose()


def _note_repo(manager: SessionManager) -> RepositorySQLAlchemy[_Note, int]:
    return RepositorySQLAlchemy(
        session_manager=manager,
        model=_Note,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


async def test_a_repository_of_another_engine_refuses_the_active_session(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    first, second, first_path, second_path = two_stores
    other = _note_repo(second)

    note = _NoteCommand(name="audit")
    async with SQLAlchemyUnitOfWork(first):
        with pytest.raises(ForeignSessionError, match="cannot join the active session") as raised:
            await other.create(note)

    assert raised.value.code == "foreign_session"
    assert (_count_rows(first_path, _NOTE), _count_rows(second_path, _NOTE)) == (0, 0)


class _NoteService:
    """A service boundary over one store, opening its own transaction when none is active."""

    def __init__(self, manager: SessionManager) -> None:
        self.session_manager = manager
        self._notes = _note_repo(manager)

    @transactional
    async def record(self, name: str) -> None:
        await self._notes.create(_NoteCommand(name=name))


async def test_a_transactional_service_of_another_store_refuses_the_active_session(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    first, second, first_path, second_path = two_stores
    service = _NoteService(second)

    async with SQLAlchemyUnitOfWork(first):
        with pytest.raises(ForeignSessionError, match="cannot join the active session"):
            await service.record("audit")

    assert (_count_rows(first_path, _NOTE), _count_rows(second_path, _NOTE)) == (0, 0)


async def test_an_elevation_sink_of_another_store_refuses_to_set_a_flag(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    first, second, _, _ = two_stores
    sink = SQLAlchemyElevationSink(second)

    async with SQLAlchemyUnitOfWork(first):
        with pytest.raises(ForeignSessionError, match="cannot join the active session"):
            await sink.set_flag("editor")


async def test_an_elevation_sink_of_another_store_refuses_its_transaction(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    first, second, _, _ = two_stores
    sink = SQLAlchemyElevationSink(second)

    async with SQLAlchemyUnitOfWork(first):
        with pytest.raises(ForeignSessionError, match="cannot join the active session"):
            sink.in_transaction()


_WRITE = Permission("rows.write")
_CATALOG = RoleCatalog((_WRITE,), (Role("editor", (_WRITE,)),))
_BOUNDARY = Scope.of("b1")


async def test_an_elevation_refused_by_another_store_leaves_the_scope_unelevated(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    first, second, _, _ = two_stores
    decision = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=_BOUNDARY))

    async with (
        SQLAlchemyUnitOfWork(first),
        elevation_scope(owns_transaction=False, sink=SQLAlchemyElevationSink(second)),
    ):
        with pytest.raises(ForeignSessionError, match="cannot join the active session"):
            await elevate_scope(
                "editor", decision, at=_BOUNDARY, permission=_WRITE, catalog=_CATALOG
            )
        assert elevated_scopes() == frozenset()


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=_FAULT_B)
async def test_a_write_outside_the_active_session_does_not_commit_on_its_own(
    two_stores: tuple[SessionManager, SessionManager, Path, Path],
) -> None:
    # Inside SQLAlchemyUnitOfWork on the first engine a repository of the
    # second engine refuses the active session (fault A fails closed), so the
    # literal shape of fault B never writes. The own-session path is
    # RepositorySQLAlchemy._session_scope -> session_manager.session() ->
    # commit(), and it runs only when no active session is bound. A task
    # started with a fresh contextvars.Context() is that path: it stands in
    # for a store with its own context variable ("one context variable per
    # backend" in ADR 0008). The first engine's row must still roll back.
    first, second, first_path, second_path = two_stores
    primary = _note_repo(first)
    other = _note_repo(second)

    async def write_other() -> None:
        await other.create(_NoteCommand(name="other"))

    async def write_then_fail() -> None:
        async with SQLAlchemyUnitOfWork(first):
            await primary.create(_NoteCommand(name="primary"))
            await asyncio.create_task(write_other(), context=contextvars.Context())
            raise RuntimeError(_OUTER_FAILED)

    with pytest.raises(RuntimeError, match=_OUTER_FAILED):
        await write_then_fail()

    in_first = _count_rows(first_path, _NOTE)
    in_second = _count_rows(second_path, _NOTE)
    if in_first != 0:
        raise RuntimeError(f"db1 kept {in_first} row(s) after rollback")
    assert in_second == 0
