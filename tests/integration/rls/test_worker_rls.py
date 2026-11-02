"""A worker job sees the rows the API sees for the same scope (US4, T028, SC-005).

The worker now composes on the shared recipe, so ``prepare_models`` binds the
row-level security settings in every process. The same use case runs through
the API's composition and through a job on a ``solo`` worker, against tables
with scopes and ``schema.mode: external``, and both return the same rows for
each caller.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from celery.signals import worker_init, worker_shutdown
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.celery.bootstrap import bootstrap_worker
from loom.celery.constants import TASK_JOB_PREFIX
from loom.composition import build_application
from loom.core.environment import Environment
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.identity.wire import encode_identity
from loom.core.job.job import Job
from loom.core.repository.sqlalchemy.rls import create_schema, register_scope_source
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources
from loom.core.repository.sqlalchemy.transactional import get_active_session
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.celery import WorkerDouble, isolated_signals
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"

_isolated_signals = isolated_signals(worker_init, worker_shutdown)


class ListNoteBodies(UseCase[notes.Note, list[str]]):
    """Read every note the caller's scope lets through."""

    async def execute(self) -> list[str]:
        session = get_active_session()
        if session is None:
            raise RuntimeError("no unit-of-work session")
        rows = await session.execute(text("SELECT body FROM notes ORDER BY body"))
        return [str(body) for body in rows.scalars()]


class ListNoteBodiesJob(Job[list[str]]):
    """Run :class:`ListNoteBodies` as the caller the task carries."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> list[str]:
        return list(await self._app.invoke(ListNoteBodies))


@pytest.fixture(autouse=True)
def _editor_source() -> Iterator[None]:
    clear_scope_sources()
    register_scope_source("editor", lambda: "ana")
    yield
    clear_scope_sources()


async def _seed(bypass_url: str) -> None:
    engine = create_async_engine(bypass_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO notes (owner_id, editor, body) VALUES "
                    f"('{U1}', 'ana', 'a'), ('{U1}', 'bob', 'b'), ('{U2}', 'zoe', 'z')"
                )
            )
    finally:
        await engine.dispose()


def _config(tmp_path: Path) -> str:
    """The product's configuration, discovering this module and serving both entry points."""
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{notes.SCHEMA}.yaml").read_text())
    document["app"]["discovery"]["modules"]["include"].append(__name__)
    document["celery"] = {
        "broker_url": "memory://",
        "result_backend": "cache+memory://",
        "task_always_eager": True,
    }
    document["persistence"] = {"cursors": {"signing": "ephemeral"}}
    path = tmp_path / "both.yaml"
    path.write_text(yaml.safe_dump(document))
    return str(path)


async def _through_the_api(config: str, subject: str) -> list[str]:
    application = build_application(config, adapter=FastAPIAdapter(), environment="test")
    token = set_identity(Identity(subject=subject))
    try:
        async with application.running():
            return list(await application.kernel.app.invoke(ListNoteBodies))
    finally:
        reset_identity(token)


def _through_the_worker(config: str, subjects: tuple[str, ...]) -> dict[str, list[str]]:
    result = bootstrap_worker(config, environment=Environment.TEST)
    task = result.celery_app.tasks[f"{TASK_JOB_PREFIX}.{ListNoteBodiesJob.__qualname__}"]
    worker_init.send(sender=WorkerDouble())
    try:
        return {
            subject: task.apply(
                kwargs={"identity": encode_identity(Identity(subject=subject))}
            ).get()
            for subject in subjects
        }
    finally:
        worker_shutdown.send(sender=None)


async def test_a_job_returns_the_rows_the_api_returns_for_each_scope(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(notes.SCHEMA)
    application = application_for(notes, database, tmp_path)
    await create_schema(database.migrator, application)
    await _seed(database.bypass)
    config = _config(tmp_path)

    api = {subject: await _through_the_api(config, subject) for subject in (U1, U2)}
    worker = _through_the_worker(config, (U1, U2))

    assert api == {U1: ["a", "b"], U2: ["z"]}
    assert worker == api
