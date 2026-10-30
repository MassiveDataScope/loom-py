"""A worker with no model checks its Postgres connection and leaves none open.

The prefork parent runs the check before Celery forks, so the check must not
leave a connection the children would inherit, nor create the tables the
process-wide metadata holds for models this worker does not serve. Nor may it
connect as a role that ignores row-level security: the worker connects as an
ordinary role created for the test, and a superuser fails the check.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import event, inspect, make_url, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, compose_application
from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.model.types import Integer
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.helpers.composition import composition_source

URI_ENV = "LOOM_PG_IT_URI"
_TABLE = "connection_only_unserved"

pytestmark = pytest.mark.integration


class _Unserved(BaseModel):
    """Compiled by someone else in the process; not a model of this worker."""

    __tablename__ = _TABLE
    id: int = ColumnField(Integer, primary_key=True)


@pytest.fixture(autouse=True)
def _restored_registry() -> Iterator[None]:
    """Drop the unserved model from the process-wide metadata after the test."""
    yield
    reset_registry()


class _RawSqlJob(Job[int]):
    def execute(self) -> int:
        return 1


def _database() -> str:
    database = os.environ.get(URI_ENV)
    if not database:
        pytest.skip(f"{URI_ENV} is not set")
    return database


async def _admin(database: str, statement: str) -> None:
    engine = create_async_engine(database, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


@pytest.fixture
async def worker_url() -> AsyncIterator[str]:
    """The URL of an ordinary login role: neither superuser nor bypassing row-level security."""
    database = _database()
    role = f"loom_worker_{secrets.token_hex(4)}"
    password = secrets.token_hex(12)
    await _admin(
        database, f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOBYPASSRLS"
    )
    try:
        yield (
            make_url(database)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
        )
    finally:
        await _admin(database, f'DROP ROLE IF EXISTS "{role}"')


def _worker(tmp_path: Path, url: str) -> Any:
    source = composition_source(
        ConfigContext.from_dict(
            {
                "database": {"url": url},
                "celery": {"broker_url": "memory://", "result_backend": "cache+memory://"},
            }
        ),
        app=AppConfig(name="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    return compose_application(source, CeleryAdapter(jobs=[_RawSqlJob]))


async def _connections(database: str, name: str) -> int:
    engine = create_async_engine(database, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text("SELECT count(*) FROM pg_stat_activity WHERE application_name = :name"),
                {"name": name},
            )
            return int(result.scalar_one())
    finally:
        await engine.dispose()


async def _settled_connections(database: str, name: str) -> int:
    """Poll briefly: a backend leaves ``pg_stat_activity`` just after its client closes."""
    deadline = time.monotonic() + 5
    count = await _connections(database, name)
    while count and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        count = await _connections(database, name)
    return count


async def _has_table(database: str) -> bool:
    engine = create_async_engine(database, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(lambda sync: inspect(sync).has_table(_TABLE))
    finally:
        await engine.dispose()


async def test_the_check_probes_once_and_leaves_no_connection(
    tmp_path: Path, worker_url: str
) -> None:
    database = _database()
    compile_all(_Unserved)
    application = _worker(tmp_path, worker_url)
    manager = application.kernel.container.resolve(SessionManager)
    name = f"connection-only-{uuid.uuid4().hex[:12]}"
    opened: list[str] = []

    @event.listens_for(manager.engine.sync_engine, "do_connect")
    def _label(dialect: Any, record: Any, cargs: Any, cparams: dict[str, Any]) -> None:
        cparams.setdefault("server_settings", {})["application_name"] = name
        opened.append(name)

    try:
        await application.check()

        pool: Any = manager.engine.sync_engine.pool
        assert opened == [name]
        assert (pool.checkedout(), pool.checkedin()) == (0, 0)
        assert await _settled_connections(database, name) == 0
        assert not await _has_table(database)
    finally:
        await manager.dispose()


async def test_a_superuser_fails_the_check_and_leaves_no_connection(tmp_path: Path) -> None:
    """The test container's admin is a superuser: it would ignore row-level security."""
    database = _database()
    application = _worker(tmp_path, database)
    manager = application.kernel.container.resolve(SessionManager)

    try:
        with pytest.raises(ConfigError, match="superuser or a bypass role"):
            await application.check()

        pool: Any = manager.engine.sync_engine.pool
        assert (pool.checkedout(), pool.checkedin()) == (0, 0)
    finally:
        await manager.dispose()
