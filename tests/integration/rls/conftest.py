"""Postgres fixtures for the row-scoped schema integration tests.

Every test module gets its own database so bootstraps, guards and event
triggers never leak between modules. The ``scoped_database`` factory applies a
``BootstrapConfig`` for one schema and returns the connection URLs of the
users that bootstrap created, so tests connect exactly as the product would.
"""

from __future__ import annotations

import asyncio
import os
import secrets
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import Application, load_application

URI_ENV_VAR = "LOOM_PG_IT_URI"
FIXTURE_USERS = {"ro": "read", "rw": "write", "ops": "bypass"}
COMPOSE_COMMAND = "docker compose -f docker-compose.local.yaml up -d postgres"


async def _run(uri: str, *statements: str, autocommit: bool = False) -> None:
    engine = create_async_engine(
        uri,
        poolclass=NullPool,
        isolation_level="AUTOCOMMIT" if autocommit else None,
    )
    try:
        async with engine.connect() as conn:
            for statement in statements:
                await conn.execute(text(statement))
            if not autocommit:
                await conn.commit()
    finally:
        await engine.dispose()


async def scalar(url: str, sql: str, **params: Any) -> Any:
    """Return the first column of the first row of ``sql`` run as the user in ``url``."""
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            return (await conn.execute(text(sql), params)).scalar()
    finally:
        await engine.dispose()


async def execute(url: str, *statements: str) -> None:
    """Run ``statements`` in one committed transaction as the user in ``url``."""
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            for statement in statements:
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


async def _connection_problem(uri: str) -> str | None:
    try:
        await _run(uri, "SELECT 1")
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


@pytest.fixture(scope="session")
def pg_admin_uri() -> str:
    uri = os.environ.get(URI_ENV_VAR)
    if not uri:
        pytest.skip(
            f"{URI_ENV_VAR} is not set; start Postgres with `{COMPOSE_COMMAND}` and export it"
        )
    problem = asyncio.run(_connection_problem(uri))
    if problem is not None:
        pytest.skip(
            f"Postgres at ${URI_ENV_VAR} is not reachable ({problem});"
            f" start it with `{COMPOSE_COMMAND}`"
        )
    return uri


@pytest.fixture(scope="module")
def created_roles() -> set[str]:
    """Roles are cluster-wide: every role a module creates is dropped after its database."""
    return set()


@pytest.fixture(scope="module")
def module_database_uri(pg_admin_uri: str, created_roles: set[str]) -> Iterator[str]:
    name = f"loom_rls_{secrets.token_hex(4)}"
    asyncio.run(_run(pg_admin_uri, f'CREATE DATABASE "{name}"', autocommit=True))
    uri = make_url(pg_admin_uri).set(database=name).render_as_string(hide_password=False)
    try:
        yield uri
    finally:
        asyncio.run(_run(pg_admin_uri, f'DROP DATABASE "{name}" WITH (FORCE)', autocommit=True))
        drops = tuple(f'DROP ROLE IF EXISTS "{role}"' for role in sorted(created_roles))
        if drops:
            asyncio.run(_run(pg_admin_uri, *drops, autocommit=True))


@dataclass(frozen=True, slots=True)
class ScopedDatabase:
    """Connection URLs produced by one bootstrap, one per database user."""

    schema: str
    superuser: str
    migrator: str
    read: str
    write: str
    bypass: str

    def as_user(self, user: str, password: str) -> str:
        return (
            make_url(self.superuser)
            .set(username=user, password=password)
            .render_as_string(hide_password=False)
        )


BootstrapFactory = Callable[..., Coroutine[Any, Any, ScopedDatabase]]


@pytest.fixture
def scoped_database(module_database_uri: str, created_roles: set[str]) -> BootstrapFactory:
    """Apply a product bootstrap for ``schema`` and return its user URLs.

    The factory imports the bootstrap API lazily so this module stays
    importable while the mechanism is still red.
    """

    async def factory(schema: str, **options: Any) -> ScopedDatabase:
        from loom.core.repository.sqlalchemy.rls import (
            BootstrapConfig,
            DatabaseRoles,
            DatabaseUser,
            SchemaNames,
            apply_bootstrap,
        )

        passwords = {
            role: secrets.token_urlsafe(12)
            for role in (
                f"{schema}_migrator",
                f"{schema}_ro",
                f"{schema}_rw",
                f"{schema}_ops",
            )
        }
        config = BootstrapConfig(
            schema=schema,
            roles=DatabaseRoles(owner=f"{schema}_owner", migrator=f"{schema}_migrator"),
            database_users={
                f"{schema}_ro": DatabaseUser(login=True, access="read"),
                f"{schema}_rw": DatabaseUser(login=True, access="write"),
                f"{schema}_ops": DatabaseUser(login=True, access="bypass"),
            },
            names=options.pop("names", SchemaNames.derived(schema)),
            **options,
        )
        created_roles.update(
            {f"{schema}_owner", f"{schema}_readers", f"{schema}_writers", *passwords}
        )
        await apply_bootstrap(module_database_uri, config, passwords)
        base = ScopedDatabase(
            schema=schema,
            superuser=module_database_uri,
            migrator="",
            read="",
            write="",
            bypass="",
        )
        return ScopedDatabase(
            schema=schema,
            superuser=module_database_uri,
            migrator=base.as_user(f"{schema}_migrator", passwords[f"{schema}_migrator"]),
            read=base.as_user(f"{schema}_ro", passwords[f"{schema}_ro"]),
            write=base.as_user(f"{schema}_rw", passwords[f"{schema}_rw"]),
            bypass=base.as_user(f"{schema}_ops", passwords[f"{schema}_ops"]),
        )

    return factory


@pytest.fixture
async def admin_connection(module_database_uri: str) -> AsyncIterator[Any]:
    engine = create_async_engine(module_database_uri, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            yield conn
    finally:
        await engine.dispose()


def application_for(
    product: ModuleType, database: ScopedDatabase, tmp_path: Path, *, schema: str | None = None
) -> Application:
    """Load a synthetic product as its own ``Application`` from a generated configuration file."""
    name = schema or product.SCHEMA
    config = {
        "app": {
            "name": name,
            "discovery": {"mode": "modules", "modules": {"include": [product.__name__]}},
        },
        "database": {
            "url": database.write,
            "schema": {
                "mode": "external",
                "name": name,
                "roles": {"owner": f"{name}_owner", "migrator": f"{name}_migrator"},
                "database_users": {
                    f"{name}_{suffix}": {"login": True, "access": access}
                    for suffix, access in FIXTURE_USERS.items()
                },
                "scopes": dict(product.SCOPE_BINDINGS),
                "guard": f"loom_guard_{name}",
                "groups": {"readers": f"{name}_readers", "writers": f"{name}_writers"},
                "version_tables": {"structure": "alembic_version", "data": "alembic_version_data"},
            },
        },
    }
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(config))
    return load_application(str(path))
