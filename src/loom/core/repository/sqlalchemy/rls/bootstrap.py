"""Install the static guard of one application schema and configure it from bound data.

No SQL is built from the product's declaration: the guard revisions are static
files pinned by digest, and every name reaches Postgres as a bound parameter
that the guard quotes itself. Passwords never reach the server: only
SCRAM-SHA-256 verifiers computed on the client do, and the transaction turns
statement logging off before sending them. The bootstrap takes the schema's
advisory lock before creating anything, the same lock the migration runners
and ``create_schema`` take, and re-verifies the guard before it commits.

Passwords are not SASLprep-normalised: a password must already be in its
SASLprep normal form, which every ASCII password is.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from collections.abc import Callable, Mapping
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import LOCK_TIMEOUT, SCHEMA_LOCK, validate_timeout
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls.config import POSTGRES_SCRAM_ITERATIONS, BootstrapConfig
from loom.core.repository.sqlalchemy.rls.guard_manifest import pending_revisions, preflight_sql
from loom.core.repository.sqlalchemy.rls.integrity import guard_problems

MIN_SERVER_VERSION_NUM: Final = 140000

QUIET_LOGS: Final = (
    "SELECT set_config('log_statement', 'none', true), "
    "set_config('log_min_duration_statement', '-1', true), "
    "set_config('log_parameter_max_length', '0', true)"
)
INSTALLER: Final = "SELECT current_user"
CREATE_GUARD: Final = "SELECT pg_temp.loom_create_guard($1, $2)"
INSTALL_PATH: Final = (
    "SELECT set_config('search_path', quote_ident($1) || ', pg_catalog, pg_temp', true), "
    "set_config($1 || '.installing', 'on', true)"
)
APPLIED_REVISIONS: Final = "SELECT n, sha256 FROM revision ORDER BY n"
RECORD_REVISION: Final = "INSERT INTO revision (n, sha256) VALUES ($1, $2)"
CONFIGURE: Final = "SELECT configure($1::jsonb)"
SET_PASSWORD: Final = "SELECT set_password_verifier($1, $2)"
_QUIET_LOGS: Final = text(QUIET_LOGS)
_INSTALLER: Final = text(INSTALLER)
_LOCK_TIMEOUT: Final = text(LOCK_TIMEOUT)
_SCHEMA_LOCK: Final = text(SCHEMA_LOCK)


def scram_sha256_verifier(
    password: str, *, salt: bytes, iterations: int = POSTGRES_SCRAM_ITERATIONS
) -> str:
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
    lock_timeout: str = "5s",
) -> None:
    """Install or upgrade the guard of ``config.schema`` and configure it, in one transaction.

    Raises:
        ConfigError: When a name or the lock timeout is invalid, the server
            refuses to create the event triggers (superuser or ``rds_superuser``
            needed), the guard holds revisions this release does not know, or
            the guard differs from the released one before commit.
    """
    try:
        config.validated()
        validate_timeout(lock_timeout)
    except ValueError as exc:
        raise ConfigError(f"database.schema: {exc}") from exc
    verifiers = {
        user: scram_sha256_verifier(
            password, salt=salt_factory(), iterations=config.scram_iterations
        )
        for user, password in passwords.items()
    }
    engine = create_async_engine(superuser_url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(_QUIET_LOGS)
            await connection.execute(_LOCK_TIMEOUT, {"lock": lock_timeout})
            await connection.execute(_SCHEMA_LOCK, {"schema": config.schema})
            driver = await _driver(connection)
            await _create_guard(driver, config)
            await _install(driver, config, verifiers)
            await _reverify(connection, config)
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


async def _install(driver: Any, config: BootstrapConfig, verifiers: Mapping[str, str]) -> None:
    await driver.execute(INSTALL_PATH, config.names.guard)
    applied = {row["n"]: row["sha256"] for row in await driver.fetch(APPLIED_REVISIONS)}
    for revision in pending_revisions(applied):
        await driver.execute(revision.sql())
        await driver.execute(RECORD_REVISION, revision.number, revision.sha256)
    await driver.execute(CONFIGURE, json.dumps(config.document()))
    for user, verifier in verifiers.items():
        await driver.execute(SET_PASSWORD, user, verifier)


async def _reverify(connection: AsyncConnection, config: BootstrapConfig) -> None:
    guard = config.names.guard
    installer = str((await connection.execute(_INSTALLER)).scalar())
    problems = await guard_problems(connection, guard, config.roles.owner, installer)
    if problems:
        details = "; ".join(f"{p.check} {p.subject}: {p.actual}" for p in problems)
        raise ConfigError(f"the guard {guard} differs from the released one: {details}")


async def _driver(connection: AsyncConnection) -> Any:
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    if driver is None:
        raise ConfigError("the database connection exposes no driver connection")
    return driver


def _needs_superuser(exc: Exception) -> bool:
    return "event trigger" in str(exc).lower() and getattr(exc, "sqlstate", "") == "42501"
