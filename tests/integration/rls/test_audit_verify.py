"""``verify`` reports every drift of the audit log the assertion cannot require (FR-006, FR-018)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy.exc import DBAPIError

from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import create_schema, verify
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
)

pytestmark = pytest.mark.integration


async def _created(
    factory: BootstrapFactory, tmp_path: Path, schema: str
) -> tuple[ScopedDatabase, Application]:
    database = await factory(schema)
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return database, application


async def _drift(database: ScopedDatabase, statement: str) -> None:
    """Apply *statement* behind the guard's back: event triggers off, then on again."""
    ddl, drop = (f"loom_guard_{database.schema}_{kind}" for kind in ("ddl", "drop"))
    await execute(
        database.superuser,
        f"ALTER EVENT TRIGGER {ddl} DISABLE",
        f"ALTER EVENT TRIGGER {drop} DISABLE",
        statement,
    )
    await execute(database.superuser, f"ALTER EVENT TRIGGER {drop} ENABLE ALWAYS")
    await execute(database.superuser, f"ALTER EVENT TRIGGER {ddl} ENABLE ALWAYS")


def _checks(report: object) -> set[str]:
    return {finding.check for finding in report.findings}  # type: ignore[attr-defined]


async def test_an_audited_schema_verifies_clean(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_clean")

    report = await verify(database.superuser, application)

    assert report.findings == ()
    assert report.expired_audit_partitions == ()


async def test_the_guard_refuses_a_grant_on_the_log_even_under_the_hatch(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, _ = await _created(scoped_database, tmp_path, "books_grant")

    with pytest.raises(DBAPIError, match="audit log grant outside"):
        await execute(
            database.superuser,
            "SELECT loom_guard_books_grant.enter_hatch()",
            "GRANT SELECT ON books_grant.audit_log TO books_grant_rw",
        )


async def test_a_grant_made_with_the_event_triggers_off_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_drift")
    await _drift(database, "GRANT INSERT ON books_drift.audit_log TO books_drift_ops")
    try:
        report = await verify(database.superuser, application)
    finally:
        await _drift(database, "REVOKE INSERT ON books_drift.audit_log FROM books_drift_ops")

    assert "audit.privileges" in _checks(report)


async def test_a_model_that_audits_other_tables_or_excludes_other_columns_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_model")
    accounts = next(key for key in application.audited if key[1] == "accounts")
    widened = replace(
        application.audited[accounts], excluded=application.audited[accounts].excluded | {"email"}
    )
    drifted = replace(
        application,
        audited={accounts: widened},
    )

    report = await verify(database.superuser, drifted)

    assert {"audit.registry", "audit.excluded"} <= _checks(report)


async def test_history_alone_is_still_verified(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_history")
    unmarked = replace(application, audited={})
    await create_schema(database.migrator, unmarked)
    await _drift(database, "GRANT INSERT ON books_history.audit_log TO books_history_ops")
    try:
        report = await verify(database.superuser, unmarked)
    finally:
        await _drift(database, "REVOKE INSERT ON books_history.audit_log FROM books_history_ops")

    assert "audit.privileges" in _checks(report)


async def test_an_operator_login_that_is_a_member_of_the_auditor_role_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_member")
    await execute(database.superuser, "GRANT books_member_auditor TO books_member_ops")

    report = await verify(database.superuser, application)

    assert "auditor.membership" in _checks(report)
