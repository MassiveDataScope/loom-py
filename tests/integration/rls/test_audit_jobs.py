"""Both actor paths of a job reach the audit row unchanged (FR-001b, FR-016, T026).

A job enqueued by a person runs as the identity its envelope carries; a job an
event or a schedule starts runs as the system identity its Lambda trigger
declares. The worker writes an audited row in each case and the audit log
names that actor, mechanism included.
"""

from __future__ import annotations

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
from loom.core.environment import Environment
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.job import Job
from loom.core.repository.sqlalchemy.rls import create_schema
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.serverless.aws.lambda_.functions import S3Trigger, TriggerIdentity
from loom.serverless.aws.lambda_.handler import _identity
from tests.helpers.celery import WorkerDouble, isolated_signals
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

SCHEMA = "books_jobs"
_isolated_signals = isolated_signals(worker_init, worker_shutdown)


class AddMemo(UseCase[bookkeeping.Memo, None]):
    """Write one audited memo as the caller."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> None:
        session = self._session_manager.active_session
        if session is None:
            raise RuntimeError("no unit-of-work session")
        await session.execute(text("INSERT INTO memos (body) VALUES ('from a job')"))


class AddMemoJob(Job[None]):
    """Run :class:`AddMemo` as the caller the task carries."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> None:
        await self._app.invoke(AddMemo)


def _config(tmp_path: Path) -> str:
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{SCHEMA}.yaml").read_text())
    document["app"]["discovery"]["modules"]["include"].append(__name__)
    document["celery"] = {
        "broker_url": "memory://",
        "result_backend": "cache+memory://",
        "task_always_eager": True,
    }
    document["persistence"] = {"cursors": {"signing": "ephemeral"}}
    path = tmp_path / "worker.yaml"
    path.write_text(yaml.safe_dump(document))
    return str(path)


def _run(config: str, identity: dict[str, Any] | None) -> None:
    result = bootstrap_worker(config, environment=Environment.TEST)
    task = result.celery_app.tasks[f"{TASK_JOB_PREFIX}.{AddMemoJob.__qualname__}"]
    worker_init.send(sender=WorkerDouble())
    try:
        task.apply(kwargs={"identity": identity}).get()
    finally:
        worker_shutdown.send(sender=None)


async def test_a_job_records_the_person_who_enqueued_it_and_an_event_its_system_identity(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(SCHEMA)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    )
    config = _config(tmp_path)
    trigger = S3Trigger(job="AddMemoJob", identity=TriggerIdentity(subject="svc:importer"))

    _run(config, encode_identity(Identity(subject="ana", mechanism="jwt")))
    _run(config, _identity("importer", "s3", trigger))

    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(
                text(
                    f"SELECT subject, mechanism, on_behalf_of FROM {SCHEMA}.audit_log "
                    "WHERE table_name = 'memos' ORDER BY id"
                )
            )
            recorded = [tuple(row) for row in rows]
    finally:
        await engine.dispose()
    assert recorded == [
        ("ana", "jwt", ""),
        ("svc:importer", "lambda:importer:s3", ""),
    ]
