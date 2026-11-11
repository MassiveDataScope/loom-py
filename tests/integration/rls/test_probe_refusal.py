"""A connection-only probe asks the database itself for scoped tables (real Postgres).

A worker composed with no model serves raw SQL. Outside ``schema.mode:
external`` nothing binds the scoped settings, so it must refuse to start
against a database whose search path holds a table that row-level security
or loom's guard protects, whoever compiled the models: another process, or an
application composed in this one into a metadata of its own.
"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable

import pytest
from sqlalchemy.engine import make_url

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.profile import WORKER_PROFILE
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from tests.helpers.persistence import build_backend
from tests.integration.rls.conftest import execute, scalar

pytestmark = pytest.mark.integration

_Schema = Callable[..., Awaitable[str]]


@pytest.fixture
def probe_schema(database_uri: str, created_roles: set[str]) -> _Schema:
    """Create a schema, run *statements* in it as the superuser, return a plain user's URL.

    The user is neither superuser nor bypass, and its search path is that schema alone.
    """

    async def create(*statements: str) -> str:
        name = f"probe_{secrets.token_hex(4)}"
        password = secrets.token_urlsafe(12)
        created_roles.add(name)
        await execute(
            database_uri,
            f"CREATE ROLE {name} LOGIN PASSWORD '{password}'",
            f"CREATE SCHEMA {name}",
            f"GRANT USAGE ON SCHEMA {name} TO {name}",
            f"ALTER ROLE {name} SET search_path = {name}",
            *(statement.format(schema=name) for statement in statements),
        )
        url = make_url(database_uri).set(username=name, password=password)
        return url.render_as_string(hide_password=False)

    return create


async def _probe(url: str) -> None:
    ctx = ConfigContext.from_dict({"stores": {"default": {"backend": "sqlalchemy", "url": url}}})
    wiring = build_backend(SQLAlchemyBackend(), ctx, (), WORKER_PROFILE, connection_only=True)
    wiring.prepare_models(())
    await wiring.startup_checks()


async def test_a_database_without_scoped_tables_lets_the_probe_start(
    probe_schema: _Schema,
) -> None:
    url = await probe_schema("CREATE TABLE {schema}.plain (id int)")

    await _probe(url)


async def test_a_table_with_row_level_security_refuses_the_probe(probe_schema: _Schema) -> None:
    url = await probe_schema(
        "CREATE TABLE {schema}.guarded (id int)",
        "ALTER TABLE {schema}.guarded ENABLE ROW LEVEL SECURITY",
    )

    with pytest.raises(ConfigError, match=r"cannot protect scoped tables guarded.*mode: external"):
        await _probe(url)


async def test_a_table_carrying_loom_s_owner_trigger_refuses_the_probe(
    probe_schema: _Schema,
) -> None:
    url = await probe_schema(
        "CREATE TABLE {schema}.owned (id int)",
        "CREATE FUNCTION {schema}.deny() RETURNS trigger LANGUAGE plpgsql "
        "AS 'BEGIN RETURN NULL; END'",
        "CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT ON {schema}.owned "
        "FOR EACH STATEMENT EXECUTE FUNCTION {schema}.deny()",
    )

    with pytest.raises(ConfigError, match=r"cannot protect scoped tables owned"):
        await _probe(url)


class _Plain(BaseModel):
    __tablename__ = "privileged_create_all_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)


async def test_create_all_as_a_superuser_is_refused_before_creating_anything(
    database_uri: str,
) -> None:
    store = {"backend": "sqlalchemy", "url": database_uri, "schema": {"mode": "create_all"}}
    cursors = {"cursors": {"signing": "ephemeral"}}
    ctx = ConfigContext.from_dict({"stores": {"default": store}, "persistence": cursors})
    wiring = build_backend(SQLAlchemyBackend(), ctx, (_Plain,), WORKER_PROFILE)
    wiring.prepare_models((_Plain,))

    with pytest.raises(ConfigError, match=r"stores\.default\.url connects as a superuser"):
        await wiring.startup_checks()

    assert await scalar(database_uri, "SELECT to_regclass('privileged_create_all_rows')") is None
