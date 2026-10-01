"""Render and apply the bootstrap of one application schema.

The SQL comes from a package template; the product's names are substituted
in, nothing else. Passwords never enter the rendered text: ``apply_bootstrap``
sends SCRAM-SHA-256 verifiers computed on the client.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
from collections.abc import Callable, Mapping
from importlib.resources import files
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, DatabaseUser

MIN_SERVER_VERSION_NUM = 140000
SCRAM_ITERATIONS = 4096

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TEMPLATE = files("loom.core.repository.sqlalchemy.rls") / "templates" / "bootstrap.sql"


def render_bootstrap(config: BootstrapConfig) -> str:
    """Return the idempotent SQL that provisions ``config.schema`` and its guard."""
    names = [config.schema, config.roles.owner, config.roles.migrator, *config.database_users]
    for name in names:
        _identifier(name)
    readers, writers = _groups(config.schema)
    bypass = [user for user, spec in config.database_users.items() if spec.access == "bypass"]
    substitutions = {
        "{MIN_SERVER_VERSION_NUM}": str(MIN_SERVER_VERSION_NUM),
        "{S}": config.schema,
        "{OWNER}": config.roles.owner,
        "{MIGRATOR}": config.roles.migrator,
        "{READERS}": readers,
        "{WRITERS}": writers,
        "{BYPASS_LIST}": ", ".join(f"'{user}'" for user in bypass) or "NULL",
        "{ROLE_ROWS}": _role_rows(config, readers, writers),
        "{USER_STATEMENTS}": _user_statements(
            config.schema, config.database_users, readers, writers
        ),
        "{BYPASS_STATEMENTS}": _bypass_statements(config, bypass),
        "{REVOKE_PUBLIC}": _revoke_public(config.revoke_public),
    }
    text = _TEMPLATE.read_text()
    for placeholder, value in substitutions.items():
        text = text.replace(placeholder, value)
    return text


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


def password_statements(
    passwords: Mapping[str, str], *, salt_factory: Callable[[], bytes] = lambda: os.urandom(16)
) -> list[str]:
    """One ``ALTER ROLE`` per user carrying a verifier, never the password itself."""
    return [
        f"ALTER ROLE {_identifier(user)} PASSWORD "
        f"'{scram_sha256_verifier(password, salt=salt_factory())}'"
        for user, password in passwords.items()
    ]


async def apply_bootstrap(
    superuser_url: str, config: BootstrapConfig, passwords: Mapping[str, str]
) -> None:
    """Apply the rendered bootstrap and the password verifiers in one transaction.

    Raises:
        ConfigError: When the server refuses to create the event trigger, which
            needs superuser or ``rds_superuser``.
    """
    script = render_bootstrap(config)
    statements = password_statements(passwords)
    engine = create_async_engine(superuser_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            driver = await _driver(connection)
            async with driver.transaction():
                try:
                    await driver.execute(script)
                except Exception as exc:
                    raise _translate(exc, config.schema) from exc
                for statement in statements:
                    await driver.execute(statement)
    finally:
        await engine.dispose()


async def _driver(connection: AsyncConnection) -> Any:
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    if driver is None:
        raise ConfigError("the database connection exposes no driver connection")
    return driver


def _translate(exc: Exception, schema: str) -> Exception:
    message = str(exc)
    if "event trigger" in message.lower() and getattr(exc, "sqlstate", "") == "42501":
        return ConfigError(f"bootstrap of {schema} needs superuser or rds_superuser: {message}")
    return exc


def _identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"{name!r} is not a plain SQL identifier")
    return name


def _revoke_public(revoke: bool) -> str:
    return "REVOKE ALL ON SCHEMA public FROM PUBLIC;" if revoke else ""


def _groups(schema: str) -> tuple[str, str]:
    return f"{schema}_readers", f"{schema}_writers"


def _role_row(name: str, *, login: bool, bypass: bool, inherit: bool) -> str:
    flags = ", ".join(str(flag).lower() for flag in (login, bypass, inherit))
    return f"('{name}', {flags})"


def _role_rows(config: BootstrapConfig, readers: str, writers: str) -> str:
    rows = [
        _role_row(config.roles.owner, login=False, bypass=False, inherit=True),
        _role_row(config.roles.migrator, login=True, bypass=False, inherit=False),
        _role_row(readers, login=False, bypass=False, inherit=True),
        _role_row(writers, login=False, bypass=False, inherit=True),
    ]
    for name, spec in config.database_users.items():
        bypass = spec.access == "bypass"
        rows.append(_role_row(name, login=spec.login, bypass=bypass, inherit=not bypass))
    return ", ".join(rows)


def _user_statements(
    schema: str, users: Mapping[str, DatabaseUser], readers: str, writers: str
) -> str:
    lines: list[str] = []
    for name, spec in users.items():
        if spec.access == "read":
            lines.append(f"GRANT {readers} TO {name};")
        elif spec.access == "write":
            lines.append(f"GRANT {readers}, {writers} TO {name};")
        if spec.login:
            lines.append(f"ALTER ROLE {name} SET search_path = {schema};")
    return "\n".join(lines)


def _bypass_statements(config: BootstrapConfig, bypass: list[str]) -> str:
    lines: list[str] = []
    for user in bypass:
        lines += [
            f"GRANT USAGE ON SCHEMA {config.schema} TO {user};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {config.roles.owner} IN SCHEMA {config.schema} "
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {user};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {config.roles.owner} IN SCHEMA {config.schema} "
            f"GRANT USAGE ON SEQUENCES TO {user};",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {config.schema} "
            f"TO {user};",
            f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA {config.schema} TO {user};",
            f"DO $$ BEGIN IF to_regclass('{config.schema}.alembic_version') IS NOT NULL THEN "
            f"REVOKE ALL ON TABLE {config.schema}.alembic_version FROM {user}; END IF; END $$;",
        ]
    return "\n".join(lines)
