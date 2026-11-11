"""Postgres fixtures for the row-scoped schema integration tests.

Every test gets its own database, cloned from an empty session template, so
bootstraps, guards and event triggers never leak between tests; a module that
shares one expensive store across its tests points ``database_uri`` at
``module_database_uri`` instead. The ``scoped_database`` factory applies a
``BootstrapConfig`` for one schema and returns the connection URLs of the
users that bootstrap created, so tests connect exactly as the product would.
"""

from __future__ import annotations

import asyncio
import os
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.product import clear_authz_product, register_authz_product
from loom.core.locator import Application, load_application
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey, store_of
from loom.core.repository.sqlalchemy.rls import create_schema, register_scope_source
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources

URI_ENV_VAR = "LOOM_PG_IT_URI"
URI_ENV_VAR_B = "LOOM_PG_IT_URI_B"
FIXTURE_USERS = {"ro": "read", "rw": "write", "ops": "bypass"}
COMPOSE_COMMAND = "docker compose -f docker-compose.local.yaml up -d postgres"
COMPOSE_COMMAND_B = "docker compose -f docker-compose.local.yaml up -d postgres-b"


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


def _admin_uri(env_var: str, compose_command: str) -> str:
    """The admin URI in *env_var*, or skip naming how to start that server."""
    uri = os.environ.get(env_var)
    if not uri:
        pytest.skip(f"{env_var} is not set; start Postgres with `{compose_command}` and export it")
    problem = asyncio.run(_connection_problem(uri))
    if problem is not None:
        pytest.skip(
            f"Postgres at ${env_var} is not reachable ({problem});"
            f" start it with `{compose_command}`"
        )
    return uri


TEMPLATE_STRATEGY = "FILE_COPY"
"""How a test database is cloned on Postgres 15+: faster than ``WAL_LOG`` to create and drop
for an empty template (measured for T12c); Postgres 14 knows no other way and no clause."""


def _database(admin_uri: str, created_roles: set[str], clone: str = "") -> Iterator[str]:
    """Create a database on the server of *admin_uri*; drop it and *created_roles* after.

    *clone*, when given, is the ``TEMPLATE ...`` clause it is created with.
    """
    name = f"loom_rls_{secrets.token_hex(4)}"
    create = f'CREATE DATABASE "{name}"{clone}'
    asyncio.run(_run(admin_uri, create, autocommit=True))
    uri = make_url(admin_uri).set(database=name).render_as_string(hide_password=False)
    try:
        yield uri
    finally:
        asyncio.run(_run(admin_uri, f'DROP DATABASE "{name}" WITH (FORCE)', autocommit=True))
        # Every bootstrap also claims ``<schema>_auditor``; derive it from the owner role.
        auditors = {
            role.removesuffix("_owner") + "_auditor"
            for role in created_roles
            if role.endswith("_owner")
        }
        drops = tuple(f'DROP ROLE IF EXISTS "{role}"' for role in sorted(created_roles | auditors))
        if drops:
            asyncio.run(_run(admin_uri, *drops, autocommit=True))


def _template(admin_uri: str) -> Iterator[str]:
    """The clause cloning an empty database, which nothing ever connects to, for each test."""
    version = int(asyncio.run(scalar(admin_uri, "SHOW server_version_num")))
    strategy = f" STRATEGY {TEMPLATE_STRATEGY}" if version >= 150000 else ""
    for uri in _database(admin_uri, set()):
        yield f' TEMPLATE "{make_url(uri).database}"{strategy}'


@pytest.fixture(scope="session")
def pg_admin_uri() -> str:
    return _admin_uri(URI_ENV_VAR, COMPOSE_COMMAND)


@pytest.fixture(scope="session")
def pg_admin_uri_b() -> str:
    """The second, independent Postgres server: two stores on separate servers (ADR 0008)."""
    return _admin_uri(URI_ENV_VAR_B, COMPOSE_COMMAND_B)


@pytest.fixture(scope="session")
def template_clone(pg_admin_uri: str) -> Iterator[str]:
    yield from _template(pg_admin_uri)


@pytest.fixture(scope="session")
def template_clone_b(pg_admin_uri_b: str) -> Iterator[str]:
    yield from _template(pg_admin_uri_b)


@pytest.fixture
def created_roles() -> set[str]:
    """Roles are cluster-wide: every role a test creates is dropped after its database."""
    return set()


@pytest.fixture
def created_roles_b() -> set[str]:
    """The roles a test creates on server B, whose cluster is not server A's."""
    return set()


@pytest.fixture
def database_uri(pg_admin_uri: str, template_clone: str, created_roles: set[str]) -> Iterator[str]:
    """The test's own database on server A."""
    yield from _database(pg_admin_uri, created_roles, template_clone)


@pytest.fixture
def database_uri_b(
    pg_admin_uri_b: str, template_clone_b: str, created_roles_b: set[str]
) -> Iterator[str]:
    """The test's own database on server B."""
    yield from _database(pg_admin_uri_b, created_roles_b, template_clone_b)


@pytest.fixture(scope="module")
def module_roles() -> set[str]:
    """The roles of :func:`module_database_uri`, dropped after it."""
    return set()


@pytest.fixture(scope="module")
def module_database_uri(pg_admin_uri: str, module_roles: set[str]) -> Iterator[str]:
    """One database for a whole module, for a module whose tests share an expensive store.

    Such a module overrides ``database_uri`` and ``created_roles`` with this and
    :func:`module_roles` with :func:`shared_module_database`.
    """
    yield from _database(pg_admin_uri, module_roles)


def shared_module_database() -> tuple[Any, Any]:
    """The ``database_uri`` and ``created_roles`` overrides of a module sharing one database.

    Assign them in the module: ``database_uri, created_roles = shared_module_database()``.
    """

    @pytest.fixture
    def database_uri(module_database_uri: str) -> str:
        return module_database_uri

    @pytest.fixture
    def created_roles(module_roles: set[str]) -> set[str]:
        return module_roles

    return database_uri, created_roles


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


@dataclass(frozen=True)
class _DocumentedBootstrap(BootstrapConfig):
    """A bootstrap whose ``configure`` document also carries *extra* keys.

    Tests drive guard keys the declaration does not model yet (or a value it
    would refuse) straight into ``configure``, as the bootstrap would send them.
    """

    extra: Mapping[str, object] = field(default_factory=dict)

    def document(self) -> dict[str, object]:
        return {**super().document(), **self.extra}


@pytest.fixture
def scoped_database(database_uri: str, created_roles: set[str]) -> BootstrapFactory:
    """Apply a product bootstrap for ``schema`` and return its user URLs."""
    return _bootstrap_factory(database_uri, created_roles)


@pytest.fixture
def scoped_database_b(database_uri_b: str, created_roles_b: set[str]) -> BootstrapFactory:
    """:func:`scoped_database` on server B."""
    return _bootstrap_factory(database_uri_b, created_roles_b)


def _bootstrap_factory(database_uri: str, created_roles: set[str]) -> BootstrapFactory:
    """The factory applying a bootstrap in *database_uri*, recording its roles.

    The factory imports the bootstrap API lazily so this module stays
    importable while the mechanism is still red.
    """

    async def factory(
        schema: str, *, document: Mapping[str, object] | None = None, **options: Any
    ) -> ScopedDatabase:
        """*document* holds extra ``configure`` keys sent with the bootstrap's own."""
        from loom.core.repository.sqlalchemy.rls import (
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
        config = _DocumentedBootstrap(
            schema=schema,
            roles=DatabaseRoles(owner=f"{schema}_owner", migrator=f"{schema}_migrator"),
            database_users={
                f"{schema}_ro": DatabaseUser(login=True, access="read"),
                f"{schema}_rw": DatabaseUser(login=True, access="write"),
                f"{schema}_ops": DatabaseUser(login=True, access="bypass"),
            },
            names=options.pop("names", SchemaNames.derived(schema)),
            extra=dict(document or {}),
            **options,
        )
        created_roles.update(
            {
                f"{schema}_owner",
                f"{schema}_readers",
                f"{schema}_writers",
                f"{schema}_auditor",
                *passwords,
            }
        )
        await apply_bootstrap(database_uri, config, passwords)
        base = ScopedDatabase(
            schema=schema,
            superuser=database_uri,
            migrator="",
            read="",
            write="",
            bypass="",
        )
        return ScopedDatabase(
            schema=schema,
            superuser=database_uri,
            migrator=base.as_user(f"{schema}_migrator", passwords[f"{schema}_migrator"]),
            read=base.as_user(f"{schema}_ro", passwords[f"{schema}_ro"]),
            write=base.as_user(f"{schema}_rw", passwords[f"{schema}_rw"]),
            bypass=base.as_user(f"{schema}_ops", passwords[f"{schema}_ops"]),
        )

    return factory


@pytest.fixture
async def admin_connection(database_uri: str) -> AsyncIterator[Any]:
    engine = create_async_engine(database_uri, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            yield conn
    finally:
        await engine.dispose()


def application_for(
    product: ModuleType,
    database: ScopedDatabase,
    tmp_path: Path,
    *,
    schema: str | None = None,
    store: StoreKey = DEFAULT_STORE,
    audit: Mapping[str, Any] | None = None,
) -> Application:
    """Load a synthetic product as *store*'s ``Application`` from a generated configuration file.

    *audit*, when given, is the store's ``schema.audit`` section.
    """
    name = schema or product.SCHEMA
    config = {
        "app": {
            "name": name,
            "discovery": {"mode": "modules", "modules": {"include": [product.__name__]}},
        },
        "stores": {
            store: {
                "backend": "sqlalchemy",
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
                    "version_tables": {
                        "structure": "alembic_version",
                        "data": "alembic_version_data",
                    },
                    "naming_convention": getattr(product, "NAMING_CONVENTION", None),
                },
            }
        },
    }
    if audit is not None:
        config["stores"][store]["schema"]["audit"] = dict(audit)
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(config))
    return load_application(str(path), store=store)


def store_section(
    database: ScopedDatabase, scopes: Mapping[str, str], *, url: str | None = None
) -> dict[str, Any]:
    """The ``stores.<key>`` section serving *database*'s external schema, as its write user."""
    return {
        "backend": "sqlalchemy",
        "url": url or database.write,
        "schema": {
            "mode": "external",
            "scopes": dict(scopes),
            "guard": f"loom_guard_{database.schema}",
        },
    }


AUDIT = StoreKey("audit")
StoreFactory = Callable[[StoreKey, ModuleType, str], Awaitable[ScopedDatabase]]


@pytest.fixture
def create_store(
    scoped_database: BootstrapFactory, scoped_database_b: BootstrapFactory, tmp_path: Path
) -> StoreFactory:
    """Bootstrap *schema* on the server of store *server* and create *product*'s tables there.

    ``default`` lives on server A and ``audit`` on server B; the tables are
    those of the store *product*'s models belong to.
    """
    servers = {DEFAULT_STORE: scoped_database, AUDIT: scoped_database_b}

    async def create(server: StoreKey, product: ModuleType, schema: str) -> ScopedDatabase:
        database = await servers[server](schema)
        store = store_of(product.MODELS[0])
        application = application_for(product, database, tmp_path, schema=schema, store=store)
        await create_schema(database.migrator, application)
        return database

    return create


U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
MANAGE = Permission("members.manage")
CATALOG = RoleCatalog((MANAGE,), (Role("admin", (MANAGE,)),))
FLAG = "SELECT coalesce(current_setting('loom.scope.editor.any', true), '')"
OWNER_SETTING = "SELECT coalesce(current_setting('loom.scope.owner', true), '')"
DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))


@dataclass
class NotesProduct:
    """The authorization product of the ``notes`` schema: ``editor`` elevates with ``MANAGE``."""

    catalog: RoleCatalog = CATALOG
    delegate: Permission = MANAGE
    elevations: dict[str, Permission] = field(default_factory=lambda: {"editor": MANAGE})


@pytest.fixture
def notes_wiring() -> Iterator[None]:
    """Register the ``editor`` scope source and :class:`NotesProduct` for one test."""
    clear_scope_sources()
    clear_authz_product()
    register_scope_source("editor", lambda: "ana")
    register_authz_product(NotesProduct())
    yield
    clear_scope_sources()
    clear_authz_product()


async def seed_notes(bypass_url: str) -> None:
    """Insert ana's and bob's notes for ``U1`` and zoe's for ``U2``."""
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
