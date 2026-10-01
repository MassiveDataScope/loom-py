from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

URI_ENV_VAR = "LOOM_PG_IT_URI"
COMPOSE_COMMAND = "docker compose -f docker-compose.local.yaml up -d postgres"
APP_ROLE = "loom_it_app"
APP_PASSWORD = "loom_it_app"
PLATFORM_ROLE = "loom_it_platform"
PLATFORM_PASSWORD = "loom_it_platform"
ROWS_TABLE = "loom_it_rows"
TENANT_SETTING = "app.tenant_id"

_SETUP_STATEMENTS = (
    f"DROP TABLE IF EXISTS {ROWS_TABLE}",
    f"DROP ROLE IF EXISTS {APP_ROLE}",
    f"DROP ROLE IF EXISTS {PLATFORM_ROLE}",
    f"CREATE ROLE {APP_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{APP_PASSWORD}'",
    f"CREATE ROLE {PLATFORM_ROLE} LOGIN NOSUPERUSER BYPASSRLS PASSWORD '{PLATFORM_PASSWORD}'",
    f"CREATE TABLE {ROWS_TABLE}"
    " (id serial PRIMARY KEY, tenant_id text NOT NULL, payload text NOT NULL)",
    f"ALTER TABLE {ROWS_TABLE} ENABLE ROW LEVEL SECURITY",
    f"ALTER TABLE {ROWS_TABLE} FORCE ROW LEVEL SECURITY",
    f"CREATE POLICY tenant_rows ON {ROWS_TABLE}"
    f" USING (tenant_id = NULLIF(current_setting('{TENANT_SETTING}', true), ''))"
    f" WITH CHECK (tenant_id = NULLIF(current_setting('{TENANT_SETTING}', true), ''))",
    f"GRANT SELECT, INSERT ON {ROWS_TABLE} TO {APP_ROLE}",
    f"GRANT USAGE ON SEQUENCE {ROWS_TABLE}_id_seq TO {APP_ROLE}",
    f"GRANT SELECT ON {ROWS_TABLE} TO {PLATFORM_ROLE}",
    f"INSERT INTO {ROWS_TABLE} (tenant_id, payload) VALUES ('a', 'a1'), ('a', 'a2'), ('b', 'b1')",
)

_TEARDOWN_STATEMENTS = (
    f"DROP TABLE IF EXISTS {ROWS_TABLE}",
    f"DROP OWNED BY {APP_ROLE}",
    f"DROP OWNED BY {PLATFORM_ROLE}",
    f"DROP ROLE IF EXISTS {APP_ROLE}",
    f"DROP ROLE IF EXISTS {PLATFORM_ROLE}",
)


async def _run(uri: str, statements: tuple[str, ...]) -> None:
    engine = create_async_engine(uri, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            for statement in statements:
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


async def _connection_problem(uri: str) -> str | None:
    try:
        await _run(uri, ("SELECT 1",))
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


def _as_role(admin_uri: str, role: str, password: str) -> str:
    return (
        make_url(admin_uri)
        .set(username=role, password=password)
        .render_as_string(hide_password=False)
    )


@pytest.fixture(scope="session")
def pg_schema(pg_admin_uri: str) -> Iterator[None]:
    asyncio.run(_run(pg_admin_uri, _SETUP_STATEMENTS))
    try:
        yield
    finally:
        asyncio.run(_run(pg_admin_uri, _TEARDOWN_STATEMENTS))


@pytest.fixture(scope="session")
def pg_app_uri(pg_admin_uri: str, pg_schema: None) -> str:
    return _as_role(pg_admin_uri, APP_ROLE, APP_PASSWORD)


@pytest.fixture(scope="session")
def pg_platform_uri(pg_admin_uri: str, pg_schema: None) -> str:
    return _as_role(pg_admin_uri, PLATFORM_ROLE, PLATFORM_PASSWORD)
