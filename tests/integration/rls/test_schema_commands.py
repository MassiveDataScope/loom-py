"""``loom schema bootstrap``, ``verify`` and ``audit drop-expired`` against Postgres (T037)."""

from __future__ import annotations

import asyncio
import secrets
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from loom.cli.app import app
from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    FIXTURE_USERS,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)

pytestmark = pytest.mark.integration

SCHEMA = "books_cli"
RETENTION = "SELECT retention_months FROM loom_guard_books_cli.config"
GUARD_EVENT_TRIGGERS = (f"loom_guard_{SCHEMA}_ddl", f"loom_guard_{SCHEMA}_drop")
runner = CliRunner()


def _database(superuser: str, passwords: dict[str, str]) -> ScopedDatabase:
    base = ScopedDatabase(SCHEMA, superuser, "", "", "", "")
    return ScopedDatabase(
        schema=SCHEMA,
        superuser=superuser,
        migrator=base.as_user(f"{SCHEMA}_migrator", passwords[f"{SCHEMA}_migrator"]),
        read=base.as_user(f"{SCHEMA}_ro", passwords[f"{SCHEMA}_ro"]),
        write=base.as_user(f"{SCHEMA}_rw", passwords[f"{SCHEMA}_rw"]),
        bypass=base.as_user(f"{SCHEMA}_ops", passwords[f"{SCHEMA}_ops"]),
    )


def _set_retention(config: Path, months: int) -> None:
    document = yaml.safe_load(config.read_text())
    document["stores"]["default"]["schema"]["audit"] = {"retention_months": months}
    config.write_text(yaml.safe_dump(document))


async def _run(*args: str) -> tuple[int, str]:
    """Run ``loom schema *args`` on its own thread: the command calls ``asyncio.run``."""
    result = await asyncio.to_thread(runner.invoke, app, ["schema", *args])
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise result.exception
    return result.exit_code, result.output


async def test_the_pipeline_commands_bootstrap_verify_and_drop_one_expired_partition(
    database_uri: str,
    created_roles: set[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = [f"{SCHEMA}_migrator", *(f"{SCHEMA}_{suffix}" for suffix in FIXTURE_USERS)]
    passwords = {user: secrets.token_urlsafe(12) for user in users}
    created_roles.update(
        {*users, f"{SCHEMA}_owner", f"{SCHEMA}_readers", f"{SCHEMA}_writers", f"{SCHEMA}_auditor"}
    )
    database = _database(database_uri, passwords)
    application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    config = tmp_path / f"{SCHEMA}.yaml"
    _set_retention(config, 12)
    monkeypatch.setenv("LOOM_SUPERUSER_URL", database_uri)
    monkeypatch.setenv("LOOM_MIGRATOR_URL", database.migrator)
    for user, password in passwords.items():
        monkeypatch.setenv(f"LOOM_DB_PASSWORD_{user.upper()}", password)

    first = await _run("bootstrap", "--config", str(config))
    second = await _run("bootstrap", "--config", str(config))
    assert first[0] == 0, first[1]
    assert second[0] == 0, second[1]
    _set_retention(config, 6)
    assert (await _run("bootstrap", "--config", str(config)))[0] == 0
    assert await scalar(database_uri, RETENTION) == 6

    # the migrator logs in with the password the command set
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    )
    _set_retention(config, 6)  # application_for rewrote the file without the audit block

    clean = await _run("verify", "--config", str(config))
    assert clean[0] == 0, clean[1]
    await execute(
        database.migrator,
        "SELECT create_audit_partition('2024-01-01 00:00:00+00')",
        "SELECT create_audit_partition('2024-02-01 00:00:00+00')",
    )
    dropped = await _run("audit", "drop-expired")
    assert dropped == (0, "dropped audit_log_p202401\n")
    remaining = await scalar(
        database_uri,
        f"SELECT count(*) FROM pg_inherits WHERE inhparent = '{SCHEMA}.audit_log'::regclass "
        "AND inhrelid::regclass::text LIKE '%audit_log_p2024%'",
    )
    assert remaining == 1

    await execute(
        database_uri,
        *(f"ALTER EVENT TRIGGER {name} DISABLE" for name in GUARD_EVENT_TRIGGERS),
        f"DROP TRIGGER loom_audit_record ON {SCHEMA}.memos",
        *(f"ALTER EVENT TRIGGER {name} ENABLE ALWAYS" for name in GUARD_EVENT_TRIGGERS),
    )
    drifted = await _run("verify", "--config", str(config))
    assert drifted[0] == 1
    assert (
        "audit trigger missing, disabled, wrong function or wrong events: books_cli.memos"
        in drifted[1]
    )
