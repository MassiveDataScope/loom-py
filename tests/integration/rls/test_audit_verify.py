"""``verify`` reports every drift of the audit log the assertion cannot require (FR-006, FR-018)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy import make_url, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import create_schema, verify
from loom.core.repository.sqlalchemy.rls.audit_verify import audit_reader_findings
from loom.core.repository.sqlalchemy.rls.integrity import released_reader_source
from loom.core.repository.sqlalchemy.rls.verify import GUARD_CONFIG
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
)

TENANT = ("tenant", "character varying")
READER = (
    "read_audit_log(boolean, text[], integer, timestamptz, timestamptz, text, text, text[], "
    "jsonb, text, text, timestamptz, bigint, boolean, integer)"
)

pytestmark = pytest.mark.integration


async def _created(
    factory: BootstrapFactory, tmp_path: Path, schema: str
) -> tuple[ScopedDatabase, Application]:
    database = await factory(schema)
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return database, application


async def _reading(
    factory: BootstrapFactory, tmp_path: Path, schema: str
) -> tuple[ScopedDatabase, Application]:
    """Bootstrap *schema* with reading on, deploy it, and load the application that reads."""
    database = await factory(schema, audit_reading=True, audit_boundary=TENANT)
    application = application_for(
        bookkeeping, database, tmp_path, schema=schema, audit={"reading": {"enabled": True}}
    )
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


@asynccontextmanager
async def drifted(database: ScopedDatabase, change: str, undo: str) -> AsyncIterator[None]:
    """Apply *change* behind the guard's back for the block, then *undo* it the same way."""
    await _drift(database, change)
    try:
        yield
    finally:
        await _drift(database, undo)


def reader_body_drift(schema: str) -> tuple[str, str]:
    """The statements that append a comment to *schema*'s reader body, and remove it."""
    reader = f"to_regprocedure('{schema}.{READER}')"
    return (
        f"UPDATE pg_proc SET prosrc = prosrc || E'\\n-- drift' WHERE oid = {reader}",
        f"UPDATE pg_proc SET prosrc = replace(prosrc, E'\\n-- drift', '') WHERE oid = {reader}",
    )


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
    async with drifted(
        database,
        "GRANT INSERT ON books_drift.audit_log TO books_drift_ops",
        "REVOKE INSERT ON books_drift.audit_log FROM books_drift_ops",
    ):
        report = await verify(database.superuser, application)

    assert "audit.privileges" in _checks(report)


async def test_a_column_grant_on_a_log_partition_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """T12c fix round F3: column grants on the log or a partition are drift of their own."""
    database, application = await _created(scoped_database, tmp_path, "books_columns")
    partition = datetime.now(UTC).strftime("audit_log_p%Y%m")
    async with drifted(
        database,
        f"GRANT SELECT (subject) ON books_columns.{partition} TO books_columns_readers",
        f"REVOKE SELECT (subject) ON books_columns.{partition} FROM books_columns_readers",
    ):
        report = await verify(database.superuser, application)

    found = {finding.check: finding.actual for finding in report.findings}
    assert f"{partition}.subject:books_columns_readers:SELECT" in found["audit.column_privileges"]


async def test_a_model_that_audits_other_tables_or_excludes_other_columns_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_model")
    accounts = next(key for key in application.audited if key[1] == "accounts")
    widened = replace(
        application.audited[accounts], excluded=application.audited[accounts].excluded | {"email"}
    )
    widened_application = replace(
        application,
        audited={accounts: widened},
    )

    report = await verify(database.superuser, widened_application)

    assert {"audit.registry", "audit.excluded"} <= _checks(report)


async def test_history_alone_is_still_verified(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_history")
    unmarked = replace(application, audited={})
    await create_schema(database.migrator, unmarked)
    async with drifted(
        database,
        "GRANT INSERT ON books_history.audit_log TO books_history_ops",
        "REVOKE INSERT ON books_history.audit_log FROM books_history_ops",
    ):
        report = await verify(database.superuser, unmarked)

    assert "audit.privileges" in _checks(report)


async def test_an_operator_login_that_is_a_member_of_the_auditor_role_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _created(scoped_database, tmp_path, "books_member")
    await execute(database.superuser, "GRANT books_member_auditor TO books_member_ops")

    report = await verify(database.superuser, application)

    assert "auditor.membership" in _checks(report)


async def test_a_reading_schema_verifies_clean(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """M8: the flag, ``B``, the reader's pin and its one grantee all match."""
    database, application = await _reading(scoped_database, tmp_path, "rv_clean")

    report = await verify(database.superuser, application)

    assert report.findings == ()
    assert report.warnings == ()


async def test_a_reading_store_without_the_log_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """N13: reading needs the log, even when the store marks no table ``Audited``."""
    database = await scoped_database("rv_nolog", audit_reading=True, audit_boundary=TENANT)
    application = application_for(
        bookkeeping, database, tmp_path, schema="rv_nolog", audit={"reading": {"enabled": True}}
    )

    report = await verify(database.superuser, replace(application, audited={}))

    assert "audit.shape" in _checks(report)


async def test_an_extra_reader_grantee_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """SC-003: an EXECUTE grantee other than the readers group, made behind the guard's back."""
    database, application = await _reading(scoped_database, tmp_path, "rv_grantee")
    reader = f"rv_grantee.{READER}"
    async with drifted(
        database,
        f"GRANT EXECUTE ON FUNCTION {reader} TO rv_grantee_ops",
        f"REVOKE EXECUTE ON FUNCTION {reader} FROM rv_grantee_ops",
    ):
        report = await verify(database.superuser, application)

    assert "audit.reader_grants" in _checks(report)


async def test_a_changed_reader_body_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """SC-003: the reader's source differs from ``audit_reader_source()``."""
    database, application = await _reading(scoped_database, tmp_path, "rv_body")
    async with drifted(database, *reader_body_drift("rv_body")):
        report = await verify(database.superuser, application)

    assert "audit.reader_source" in _checks(report)


async def test_a_volatile_reader_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """``guard/0004.sql`` releases the reader STABLE; a volatile one is not the released shape."""
    database, application = await _reading(scoped_database, tmp_path, "rv_volatile")
    reader = f"rv_volatile.{READER}"
    async with drifted(
        database, f"ALTER FUNCTION {reader} VOLATILE", f"ALTER FUNCTION {reader} STABLE"
    ):
        report = await verify(database.superuser, application)

    assert "audit.reader_shape" in _checks(report)


async def test_a_recorded_reading_flag_that_differs_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """M8: the database reads while the declaration does not, and the grant implies the flag."""
    database, application = await _reading(scoped_database, tmp_path, "rv_flag")
    assert application.bootstrap is not None
    declared_off = replace(
        application,
        bootstrap=replace(application.bootstrap, audit_reading=False, audit_boundary=None),
    )

    report = await verify(database.superuser, declared_off)

    assert {"guard.audit_reading", "audit.reader_grants"} <= _checks(report)


async def test_a_recorded_boundary_that_differs_while_reading_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """M8: reading is on and the recorded ``B`` is not the one the models derive."""
    database, application = await _reading(scoped_database, tmp_path, "rv_boundary")
    assert application.bootstrap is not None
    other = replace(
        application,
        bootstrap=replace(application.bootstrap, audit_boundary=("tenant", "text")),
    )

    report = await verify(database.superuser, other)

    assert _checks(report) == {"guard.audit_reading"}


async def test_a_boundary_kept_while_reading_is_off_is_not_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """M8: turning reading off keeps ``B`` on purpose until the operator clears it."""
    _, reading = await _reading(scoped_database, tmp_path, "rv_kept")
    database = await scoped_database("rv_kept")
    application = application_for(bookkeeping, database, tmp_path, schema="rv_kept")
    await create_schema(database.migrator, application)
    assert application.bootstrap is not None and reading.bootstrap is not None
    assert reading.bootstrap.audit_boundary == TENANT

    report = await verify(database.superuser, application)

    assert report.findings == ()


async def test_the_reader_check_does_not_depend_on_the_search_path(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Review F3: the released source is found in the guard schema, whatever the search path."""
    database, application = await _reading(scoped_database, tmp_path, "rv_path")
    assert application.bootstrap is not None
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SET search_path = pg_catalog"))
            findings = await audit_reader_findings(connection, application.bootstrap)
            released = await released_reader_source(connection, "loom_guard_rv_path")
            called = await connection.scalar(
                text("SELECT loom_guard_rv_path.audit_reader_source()")
            )
    finally:
        await engine.dispose()

    assert findings == []
    assert released == called


async def test_the_recorded_reading_flag_is_read_without_the_json_codec(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Review F5: ``GUARD_CONFIG`` yields a boolean even on a driver that decodes no jsonb."""
    database, _ = await _reading(scoped_database, tmp_path, "rv_codec")
    raw = make_url(database.superuser).set(drivername="postgresql")
    connection = await asyncpg.connect(raw.render_as_string(hide_password=False))
    try:
        await connection.execute("SET search_path = loom_guard_rv_codec")
        row = await connection.fetchrow(GUARD_CONFIG)
    finally:
        await connection.close()

    assert row is not None
    assert row["audit_reading"] is True


# --- Views and definer functions over the audit log (T12b) ---------------------------------


def leak_view(schema: str) -> tuple[str, str]:
    """The statements that create a view over *schema*'s audit log outside it, and drop it."""
    view = f"public.{schema}_leak"
    return (
        f"CREATE VIEW {view} AS SELECT * FROM {schema}.audit_log",
        f"DROP VIEW IF EXISTS {view}",
    )


def _this_month_partition() -> str:
    return datetime.now(UTC).strftime("audit_log_p%Y%m")


@pytest.mark.parametrize(
    ("schema", "as_migrator", "create", "drop"),
    [
        (
            "av_inside",
            True,
            "CREATE VIEW {s}.leak AS SELECT * FROM {s}.audit_log",
            "DROP VIEW IF EXISTS {s}.leak",
        ),
        (
            "av_outside",
            False,
            "CREATE VIEW public.{s}_leak AS SELECT id, subject FROM {s}.audit_log",
            "DROP VIEW IF EXISTS public.{s}_leak",
        ),
        (
            "av_partition",
            False,
            "CREATE MATERIALIZED VIEW public.{s}_leak AS SELECT * FROM {s}.{partition}",
            "DROP MATERIALIZED VIEW IF EXISTS public.{s}_leak",
        ),
    ],
    ids=["owner-in-schema", "outside-the-schema", "materialized-over-a-partition"],
)
async def test_the_guard_refuses_a_view_over_the_audit_log(
    scoped_database: BootstrapFactory,
    tmp_path: Path,
    schema: str,
    as_migrator: bool,
    create: str,
    drop: str,
) -> None:
    """A view runs with its owner's rights: one over the log would read every tenant's rows."""
    database, _ = await _created(scoped_database, tmp_path, schema)
    names = {"s": schema, "partition": _this_month_partition()}
    url = database.migrator if as_migrator else database.superuser
    try:
        with pytest.raises(DBAPIError, match=r"read the audit log with their owner"):
            await execute(url, create.format(**names))
    finally:
        await execute(database.superuser, drop.format(**names))


async def test_the_guard_refuses_a_view_over_a_view_over_the_audit_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The outer view depends on the inner one, not on the log; the guard walks the chain."""
    schema = "av_nested"
    database, _ = await _created(scoped_database, tmp_path, schema)
    outer = f"public.{schema}_outer"
    async with drifted(database, *leak_view(schema)):
        try:
            with pytest.raises(DBAPIError, match=rf"read the audit log.*{schema}_outer"):
                await execute(
                    database.superuser,
                    f"CREATE VIEW {outer} AS SELECT id FROM public.{schema}_leak",
                )
        finally:
            # While the inner view exists the guard refuses every DDL, this drop included.
            await _drift(database, f"DROP VIEW IF EXISTS {outer}")


async def test_a_view_made_with_the_event_triggers_off_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """A view that predates the guard's revision, or was made behind its back, is reported."""
    database, application = await _created(scoped_database, tmp_path, "av_drift")
    async with drifted(database, *leak_view("av_drift")):
        report = await verify(database.superuser, application)

    assert {"audit.view", "assertion"} <= _checks(report)
    assert not report.ok


async def test_an_unknown_definer_function_is_a_warning_and_the_guards_own_are_not(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """A definer function the owner adds may read the log; verify names it, advisory only."""
    schema = "av_definer"
    database, application = await _reading(scoped_database, tmp_path, schema)
    function = f"{schema}.leak()"
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {function} RETURNS bigint LANGUAGE sql SECURITY DEFINER "
            f"AS 'SELECT count(*) FROM {schema}.audit_log'",
            f"REVOKE EXECUTE ON FUNCTION {function} FROM PUBLIC",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP FUNCTION IF EXISTS {function}")

    assert report.findings == ()
    assert report.ok
    [warning] = report.warnings
    assert warning.check == "audit.definer"
    assert "leak()" in warning.actual
    assert "read_audit_log" not in warning.actual
    assert "write_audit_row" not in warning.actual
