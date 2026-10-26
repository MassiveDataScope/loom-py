from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest

from loom.celery.auto import create_app
from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.command import Command, CommandField
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job import InlineJobService, JobService
from loom.core.job.callback import JobCallback
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.use_case import Input
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.markers import Exists, Load, LoadById, OnMissing
from loom.core.use_case.use_case import UseCase


class _Record(BaseModel):
    __tablename__ = "records_celery_integration"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


class _CreateRecordCommand(Command, frozen=True):
    name: str = CommandField()


class _RenameRecordCommand(Command, frozen=True):
    name: str = CommandField()


class _RecordLookupCommand(Command, frozen=True):
    name: str = CommandField()


class _DispatchRecordCommand(Command, frozen=True):
    name: str = CommandField()
    should_fail: bool = False


class _CreateRecordUseCase(UseCase[_Record, _Record]):
    async def execute(self, cmd: _CreateRecordCommand = Input()) -> _Record:
        return cast(_Record, await self.main_repo.create(cmd))


class _GetRecordByIdUseCase(UseCase[_Record, _Record]):
    async def execute(
        self,
        record_id: int,
        record: _Record = LoadById(_Record, by="record_id"),
    ) -> _Record:
        return record


class _RenameRecordUseCase(UseCase[_Record, _Record]):
    async def execute(
        self,
        record_id: int,
        cmd: _RenameRecordCommand = Input(),
        record: _Record = LoadById(_Record, by="record_id"),
    ) -> _Record:
        updated = await self.main_repo.update(record.id, cmd)
        if updated is None:
            raise RuntimeError("record must exist before update")
        return cast(_Record, updated)


class _LoadRecordByNameUseCase(UseCase[_Record, int]):
    async def execute(
        self,
        cmd: _RecordLookupCommand = Input(),
        record: _Record | None = Load(
            _Record,
            from_command="name",
            against="name",
            on_missing=OnMissing.RETURN_NONE,
        ),
    ) -> int:
        del cmd
        return int(record.id) if record is not None else -1


class _ExistsRecordByNameUseCase(UseCase[_Record, bool]):
    async def execute(
        self,
        cmd: _RecordLookupCommand = Input(),
        exists_record: bool = Exists(_Record, from_command="name", against="name"),
    ) -> bool:
        del cmd
        return exists_record


class _CreateRecordViaUseCaseJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, cmd: _CreateRecordCommand = Input()) -> int:
        created = await self._app.invoke(_CreateRecordUseCase, payload={"name": cmd.name})
        return int(created.id)


class _GetRecordViaLoadByIdJob(Job[str]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, record_id: int) -> str:
        loaded = await self._app.invoke(_GetRecordByIdUseCase, params={"record_id": record_id})
        return str(loaded.name)


class _RenameRecordViaLoadByIdJob(Job[str]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, record_id: int, cmd: _RenameRecordCommand = Input()) -> str:
        updated = await self._app.invoke(
            _RenameRecordUseCase,
            params={"record_id": record_id},
            payload={"name": cmd.name},
        )
        return str(updated.name)


class _LoadRecordByNameJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, cmd: _RecordLookupCommand = Input()) -> int:
        result = await self._app.invoke(_LoadRecordByNameUseCase, payload={"name": cmd.name})
        return int(result)


class _ExistsRecordByNameJob(Job[bool]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, cmd: _RecordLookupCommand = Input()) -> bool:
        result = await self._app.invoke(_ExistsRecordByNameUseCase, payload={"name": cmd.name})
        return bool(result)


class _CreateRecordInlineJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, cmd: _CreateRecordCommand = Input()) -> int:
        created = await self._app.invoke(_CreateRecordUseCase, payload={"name": cmd.name})
        return int(created.id)


class _AlwaysFailJob(Job[None]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self) -> None:
        raise RuntimeError("forced job failure")


class _CallbackSink:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[str] = []


class _RecordSuccessCallback(JobCallback):
    def __init__(self, sink: _CallbackSink) -> None:
        self._sink = sink

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self._sink.successes.append(f"{job_id}:{result}:{context.get('name', '')}")

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        del job_id, exc_type, exc_msg, context


class _RecordFailureCallback(JobCallback):
    def __init__(self, sink: _CallbackSink) -> None:
        self._sink = sink

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self._sink.failures.append(f"{job_id}:{exc_type}:{context.get('name', '')}:{exc_msg}")


class _DispatchChildJobUseCase(UseCase[_Record, bool]):
    def __init__(self, jobs: JobService) -> None:
        super().__init__()
        self._jobs = jobs

    async def execute(self, cmd: _DispatchRecordCommand = Input()) -> bool:
        on_success: type[JobCallback] | None = None
        on_failure: type[JobCallback] | None = None
        job_type: type[Job[Any]]

        if cmd.should_fail:
            job_type = _AlwaysFailJob
            on_failure = _RecordFailureCallback
        else:
            job_type = _CreateRecordInlineJob
            on_success = _RecordSuccessCallback

        self._jobs.dispatch(
            job_type,
            payload={"name": cmd.name},
            on_success=on_success,
            on_failure=on_failure,
        )
        return True


class _DispatchChildJobOrchestratorJob(Job[bool]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, cmd: _DispatchRecordCommand = Input()) -> bool:
        return bool(
            await self._app.invoke(
                _DispatchChildJobUseCase,
                payload={"name": cmd.name, "should_fail": cmd.should_fail},
            )
        )


class _RecordRepository(RepositorySQLAlchemy[_Record, int]):
    pass


async def _create_schema(db_url: str) -> None:
    manager = SessionManager(db_url)
    try:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().create_all)
    finally:
        await manager.dispose()


async def _drop_schema(db_url: str) -> None:
    manager = SessionManager(db_url)
    try:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
    finally:
        await manager.dispose()


@pytest.fixture
def celery_files(tmp_path: Path, monkeypatch: Any) -> Any:
    reset_registry()
    compile_all(_Record)

    db_path = tmp_path / "celery_integration.sqlite"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", db_url)

    config_path = Path(__file__).resolve().parent / "config" / "conf.celery.integration.yaml"

    asyncio.run(_create_schema(db_url))

    repo_session_manager = SessionManager(db_url)
    callback_sink = _CallbackSink()

    def _module(container: LoomContainer) -> None:
        container.register(
            SessionManager,
            lambda: repo_session_manager,
            scope=Scope.APPLICATION,
        )
        container.register(
            _RecordRepository,
            lambda: _RecordRepository(
                session_manager=container.resolve(SessionManager),
                model=_Record,
                cursor_codec=CursorCodec(signing="ephemeral"),
            ),
            scope=Scope.REQUEST,
        )
        container.register_repo(_Record, _RecordRepository)
        container.register(_CallbackSink, lambda: callback_sink, scope=Scope.APPLICATION)
        container.register(
            JobService,
            lambda: InlineJobService(
                container.resolve(UseCaseFactory),
                container.resolve(RuntimeExecutor),
            ),
            scope=Scope.APPLICATION,
        )

    try:
        yield str(config_path), db_path, _module, callback_sink
    finally:
        asyncio.run(repo_session_manager.dispose())
        asyncio.run(_drop_schema(db_url))
        reset_registry()
        if db_path.exists():
            db_path.unlink()


def test_create_app_executes_job_that_invokes_use_case(
    celery_files: Any,
) -> None:
    config_path, _db_path, module, callback_sink = celery_files

    celery_app = create_app(
        config_path,
        jobs=[
            _CreateRecordViaUseCaseJob,
            _GetRecordViaLoadByIdJob,
            _RenameRecordViaLoadByIdJob,
            _LoadRecordByNameJob,
            _ExistsRecordByNameJob,
            _CreateRecordInlineJob,
            _AlwaysFailJob,
            _DispatchChildJobOrchestratorJob,
        ],
        modules=[module],
        callbacks=[_RecordSuccessCallback, _RecordFailureCallback],
    )

    create_task = celery_app.tasks[f"loom.job.{_CreateRecordViaUseCaseJob.__qualname__}"]
    get_task = celery_app.tasks[f"loom.job.{_GetRecordViaLoadByIdJob.__qualname__}"]
    rename_task = celery_app.tasks[f"loom.job.{_RenameRecordViaLoadByIdJob.__qualname__}"]
    load_task = celery_app.tasks[f"loom.job.{_LoadRecordByNameJob.__qualname__}"]
    exists_task = celery_app.tasks[f"loom.job.{_ExistsRecordByNameJob.__qualname__}"]

    created = create_task.apply(kwargs={"payload": {"name": "alpha"}})
    assert created.result == 1
    assert get_task.apply(kwargs={"params": {"record_id": 1}}).result == "alpha"

    renamed = rename_task.apply(kwargs={"params": {"record_id": 1}, "payload": {"name": "omega"}})
    assert renamed.result == "omega"

    assert load_task.apply(kwargs={"payload": {"name": "omega"}}).result == 1
    assert load_task.apply(kwargs={"payload": {"name": "missing"}}).result == -1
    assert exists_task.apply(kwargs={"payload": {"name": "omega"}}).result is True
    assert exists_task.apply(kwargs={"payload": {"name": "missing"}}).result is False

    dispatch_task = celery_app.tasks[f"loom.job.{_DispatchChildJobOrchestratorJob.__qualname__}"]
    assert dispatch_task.apply(kwargs={"payload": {"name": "callback-ok"}}).result is True
    failed_dispatch = dispatch_task.apply(
        kwargs={"payload": {"name": "callback-fail", "should_fail": True}}
    )
    assert failed_dispatch.result is True  # fire-and-forget: exception silenced, job returns True

    assert any("callback-ok" in entry for entry in callback_sink.successes)
    assert any("callback-fail" in entry for entry in callback_sink.failures)
