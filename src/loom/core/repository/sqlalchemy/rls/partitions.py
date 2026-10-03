"""Create and retire the partitions of a range-partitioned scoped table, through the guard.

Partitions are created ahead of time by the migrator, never by the
application: the guard creates each one, registers it and protects it with
its parent's scopes and privileges in the caller's transaction, and refuses
anyone who is not the schema's owner. Retention detaches a partition after
unprotecting it and may drop it. Names and bounds are derived by
:func:`~loom.core.model.partition.range_partitions` and travel as bound
parameters; the guard quotes them.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import (
    APP_FIRST,
    ASSERT_SCHEMA,
    CREATE_RANGE_PARTITION,
    DETACH_PARTITION,
    GUARD_FIRST,
    LANDING,
    LOCK_TIMEOUT,
    SCHEMA_LOCK,
    create_partition_parameters,
    detach_partition_parameters,
    validate_timeout,
)
from loom.core.config import ConfigError
from loom.core.model.introspection import get_table_name
from loom.core.model.partition import Interval, PartitionRange, range_partitions
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig
from loom.core.repository.sqlalchemy.rls.guard_manifest import PARTITION_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import require_guard_revision

if TYPE_CHECKING:
    from loom.core.locator import Application

_CREATE_RANGE_PARTITION: Final = text(CREATE_RANGE_PARTITION)
_DETACH_PARTITION: Final = text(DETACH_PARTITION)
_GUARD_FIRST: Final = text(GUARD_FIRST)
_APP_FIRST: Final = text(APP_FIRST)
_ASSERT_SCHEMA: Final = text(ASSERT_SCHEMA)
_LANDING: Final = text(LANDING)
_LOCK_TIMEOUT: Final = text(LOCK_TIMEOUT)
_SCHEMA_LOCK: Final = text(SCHEMA_LOCK)


async def ensure_range_partitions(
    target: str | AsyncConnection,
    application: Application,
    model: type | str,
    start: dt.date,
    end: dt.date,
    *,
    interval: Interval = "month",
    lock_timeout: str = "5s",
) -> tuple[str, ...]:
    """Create the partitions of ``model`` covering ``[start, end)`` that do not exist yet.

    ``target`` is the migrator's URL, run in its own transaction, or an open
    connection of the migrator, run inside the caller's transaction. Each
    partition is created, registered and protected by the guard in that
    transaction.

    Returns:
        The names of the partitions created, in order.

    Raises:
        ConfigError: When ``model`` is not a range-partitioned scoped table of
            the application, the connection does not act as the schema's owner
            inside it, or the guard predates partition support.
        ValueError: When ``interval`` is unknown or a partition name is too long.
    """
    scoped = _partitioned(application, model)
    partitions = range_partitions(scoped.name, start, end, interval)
    async with _guarded(target, application, lock_timeout) as (connection, schema):
        created = [
            partition.name
            for partition in partitions
            if await _create(connection, schema, scoped.name, partition)
        ]
    return tuple(created)


async def detach_range_partitions(
    target: str | AsyncConnection,
    application: Application,
    model: type | str,
    start: dt.date,
    end: dt.date,
    *,
    interval: Interval = "month",
    drop: bool = False,
    lock_timeout: str = "5s",
) -> tuple[str, ...]:
    """Detach, and with ``drop`` drop, the partitions of ``model`` covering ``[start, end)``.

    Each partition is unprotected and detached by the guard; a partition that
    no longer exists is skipped. A detached table keeps its rows without
    row-level security and without group grants; only the owner and the
    bypass users reach it.

    Returns:
        The names of the partitions detached, in order.

    Raises:
        ConfigError: As :func:`ensure_range_partitions`.
        ValueError: As :func:`ensure_range_partitions`.
    """
    scoped = _partitioned(application, model)
    names = [p.name for p in range_partitions(scoped.name, start, end, interval)]
    async with _guarded(target, application, lock_timeout) as (connection, schema):
        detached = await _detach(connection, schema, scoped.name, names, drop=drop)
    return detached


async def _detach(
    connection: AsyncConnection, schema: str, table: str, names: Sequence[str], *, drop: bool
) -> tuple[str, ...]:
    detached: list[str] = []
    for name in names:
        parameters = detach_partition_parameters(schema, table, name, drop=drop)
        if (await connection.execute(_DETACH_PARTITION, parameters)).scalar():
            detached.append(name)
    return tuple(detached)


async def _create(
    connection: AsyncConnection, schema: str, table: str, partition: PartitionRange
) -> bool:
    parameters = create_partition_parameters(schema, table, partition)
    return bool((await connection.execute(_CREATE_RANGE_PARTITION, parameters)).scalar())


def _partitioned(application: Application, model: type | str) -> ScopedTable:
    name = model if isinstance(model, str) else get_table_name(model)
    scoped = next((t for t in application.scoped.values() if t.name == name), None)
    if scoped is None or scoped.partition_by is None:
        raise ConfigError(f"{name!r} is not a range-partitioned scoped table of this application")
    return scoped


@asynccontextmanager
async def _guarded(
    target: str | AsyncConnection, application: Application, lock_timeout: str
) -> AsyncIterator[tuple[AsyncConnection, str]]:
    bootstrap = _bootstrap(application)
    try:
        validate_timeout(lock_timeout)
    except ValueError as exc:
        raise ConfigError(f"partitions: {exc}") from exc
    if isinstance(target, AsyncConnection):
        await _prepared(target, bootstrap, lock_timeout)
        aborted = False
        try:
            yield target, bootstrap.schema
        except DBAPIError:
            aborted = True  # the rollback of the aborted transaction undoes the local path
            raise
        finally:
            if not aborted:
                await target.execute(_APP_FIRST, _path(bootstrap))
        return
    engine = create_async_engine(target, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            yield await _prepared(connection, bootstrap, lock_timeout), bootstrap.schema
            await connection.execute(_ASSERT_SCHEMA)
    finally:
        await engine.dispose()


async def _prepared(
    connection: AsyncConnection, bootstrap: BootstrapConfig, lock_timeout: str
) -> AsyncConnection:
    schema, owner, guard = bootstrap.schema, bootstrap.roles.owner, bootstrap.names.guard
    await connection.execute(_LOCK_TIMEOUT, {"lock": lock_timeout})
    await connection.execute(_SCHEMA_LOCK, {"schema": schema})
    user, landed = (await connection.execute(_LANDING)).one()
    if user != owner or landed != schema:
        raise ConfigError(
            f"partitions are managed by the migrator acting as {owner!r} inside schema "
            f"{schema!r}, found {user!r} in {landed!r}"
        )
    await require_guard_revision(connection, guard, minimum=PARTITION_GUARD_REVISION)
    await connection.execute(_GUARD_FIRST, _path(bootstrap))
    return connection


def _path(bootstrap: BootstrapConfig) -> dict[str, str]:
    return {"schema": bootstrap.schema, "guard": bootstrap.names.guard}


def _bootstrap(application: Application) -> BootstrapConfig:
    bootstrap = application.bootstrap
    if bootstrap is None:
        raise ConfigError(
            "partitions need database.schema.name, roles and database_users in the configuration"
        )
    return bootstrap
