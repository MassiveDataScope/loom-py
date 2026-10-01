"""Install the static guard of one application schema and configure it from bound data.

No SQL is built from the product's declaration: the guard revisions are static
files pinned by digest, and every name reaches Postgres as a bound parameter
that the guard quotes itself. Passwords never reach the server: only
SCRAM-SHA-256 verifiers computed on the client do.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig
from loom.core.repository.sqlalchemy.rls.guard_manifest import pending_revisions, preflight_sql

MIN_SERVER_VERSION_NUM = 140000
SCRAM_ITERATIONS = 4096

CREATE_GUARD = "SELECT pg_temp.loom_create_guard($1, $2)"
LOCK_GUARD = "SELECT pg_advisory_xact_lock(hashtextextended('loom.guard:' || $1, 0))"
ENTER_GUARD = (
    "SELECT set_config('search_path', quote_ident($1) || ', pg_catalog, pg_temp', true), "
    "set_config($1 || '.installing', 'on', true)"
)
APPLIED_REVISIONS = "SELECT n, sha256 FROM revision ORDER BY n"
RECORD_REVISION = "INSERT INTO revision (n, sha256) VALUES ($1, $2)"
CONFIGURE = "SELECT configure($1::jsonb)"
SET_PASSWORD = "SELECT set_password_verifier($1, $2)"


def scram_sha256_verifier(password: str, *, salt: bytes, iterations: int = SCRAM_ITERATIONS) -> str:
    """Return the ``SCRAM-SHA-256$iter:salt$stored_key:server_key`` verifier Postgres stores."""
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    encode = base64.b64encode
    return (
        f"SCRAM-SHA-256${iterations}:{encode(salt).decode()}$"
        f"{encode(stored_key).decode()}:{encode(server_key).decode()}"
    )


async def apply_bootstrap(
    superuser_url: str,
    config: BootstrapConfig,
    passwords: Mapping[str, str],
    *,
    salt_factory: Callable[[], bytes] = lambda: os.urandom(16),
) -> None:
    """Install or upgrade the guard of ``config.schema`` and configure it, in one transaction.

    Raises:
        ConfigError: When a name is invalid, the server refuses to create the
            event triggers (superuser or ``rds_superuser`` needed), the guard
            schema exists but is not loom's, or the guard holds revisions this
            release does not know.
    """
    try:
        config.validated()
    except ValueError as exc:
        raise ConfigError(f"database.schema: {exc}") from exc
    verifiers = {
        user: scram_sha256_verifier(password, salt=salt_factory())
        for user, password in passwords.items()
    }
    guard = config.names.guard
    engine = create_async_engine(superuser_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            driver = await _driver(connection)
            async with driver.transaction():
                await _create_guard(driver, config)
                await driver.execute(LOCK_GUARD, guard)
                await driver.execute(ENTER_GUARD, guard)
                applied = {row["n"]: row["sha256"] for row in await driver.fetch(APPLIED_REVISIONS)}
                for revision in pending_revisions(applied):
                    await driver.execute(revision.sql())
                    await driver.execute(RECORD_REVISION, revision.number, revision.sha256)
                await driver.execute(CONFIGURE, json.dumps(config.document()))
                for user, verifier in verifiers.items():
                    await driver.execute(SET_PASSWORD, user, verifier)
    finally:
        await engine.dispose()


async def _create_guard(driver: Any, config: BootstrapConfig) -> None:
    await driver.execute(preflight_sql())
    try:
        await driver.execute(CREATE_GUARD, config.names.guard, MIN_SERVER_VERSION_NUM)
    except Exception as exc:
        if _needs_superuser(exc):
            raise ConfigError(
                f"bootstrap of {config.schema} needs superuser or rds_superuser: {exc}"
            ) from exc
        raise


async def _driver(connection: AsyncConnection) -> Any:
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    if driver is None:
        raise ConfigError("the database connection exposes no driver connection")
    return driver


def _needs_superuser(exc: Exception) -> bool:
    return "event trigger" in str(exc).lower() and getattr(exc, "sqlstate", "") == "42501"
