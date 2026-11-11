"""Rules, triggers, definer functions and views that expose the audit log (T12b, review round 2).

Every object made behind the guard's back is made through ``drifted``, which
undoes it in ``finally``: the guard's event triggers fire on all later DDL in
the test's database, and a leftover leak would refuse it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import create_schema, guard_manifest, verify
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import AUDIT_LOG_VIEWS, startup_problems
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)
from tests.integration.rls.test_audit_verify import drifted

pytestmark = pytest.mark.integration

HINT = "rerun the bootstrap"


async def _created(
    factory: BootstrapFactory, tmp_path: Path, schema: str
) -> tuple[ScopedDatabase, Application]:
    database = await factory(schema)
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return database, application


def _partition() -> str:
    return datetime.now(UTC).strftime("audit_log_p%Y%m")


async def _startup(database: ScopedDatabase) -> dict[str, str]:
    """Startup's problems as the application user, by check."""
    engine = create_async_engine(database.write, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            problems = await startup_problems(
                connection, f"loom_guard_{database.schema}", {}, audited=True
            )
    finally:
        await engine.dispose()
    return {problem.check: problem.actual for problem in problems}


def _by_check(report: object) -> dict[str, str]:
    findings = report.findings  # type: ignore[attr-defined]
    return {finding.check: finding.actual for finding in findings}


@asynccontextmanager
async def _all_drifted(database: ScopedDatabase, *pairs: tuple[str, str]) -> AsyncIterator[None]:
    """Each ``(change, undo)`` behind the guard's back, undone in reverse order."""
    async with AsyncExitStack() as stack:
        for change, undo in pairs:
            await stack.enter_async_context(drifted(database, change, undo))
        yield


def _chain(schema: str) -> tuple[tuple[str, str], ...]:
    """A materialized view over a partition and a view over a view over the log."""
    mv, inner, outer = (f"public.{schema}_{name}" for name in ("mv", "inner", "outer"))
    return (
        (
            f"CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {schema}.{_partition()}",
            f"DROP MATERIALIZED VIEW IF EXISTS {mv}",
        ),
        (
            f"CREATE VIEW {inner} AS SELECT * FROM {schema}.audit_log",
            f"DROP VIEW IF EXISTS {inner} CASCADE",
        ),
        (f"CREATE VIEW {outer} AS SELECT id FROM {inner}", f"DROP VIEW IF EXISTS {outer}"),
    )


def _copy_table(schema: str) -> tuple[str, str]:
    copy = f"public.{schema}_copy"
    return f"CREATE TABLE {copy} (id bigint)", f"DROP TABLE IF EXISTS {copy} CASCADE"


# --- M1: rules -------------------------------------------------------------------------------


async def test_the_guard_refuses_a_rule_that_reads_the_log_from_an_ordinary_table(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ax_rule_table"
    database, _ = await _created(scoped_database, tmp_path, schema)
    table, copy = f"public.{schema}_t", f"public.{schema}_copy"
    await execute(database.superuser, f"CREATE TABLE {table} (x int)", _copy_table(schema)[0])
    try:
        with pytest.raises(DBAPIError, match=rf"{schema}_t rule leak.*{HINT}"):
            await execute(
                database.superuser,
                f"CREATE RULE leak AS ON INSERT TO {table} "
                f"DO ALSO INSERT INTO {copy} SELECT id FROM {schema}.audit_log",
            )
    finally:
        await execute(database.superuser, f"DROP TABLE IF EXISTS {table}, {copy} CASCADE")


async def test_the_guard_refuses_a_rule_on_the_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ax_rule_log"
    database, _ = await _created(scoped_database, tmp_path, schema)
    create_copy, drop_copy = _copy_table(schema)
    await execute(database.superuser, create_copy)
    try:
        with pytest.raises(DBAPIError, match=rf"{schema}\.audit_log rule leak"):
            await execute(
                database.superuser,
                f"CREATE RULE leak AS ON INSERT TO {schema}.audit_log "
                f"DO ALSO INSERT INTO public.{schema}_copy VALUES (NEW.id)",
            )
    finally:
        await execute(
            database.superuser, f"DROP RULE IF EXISTS leak ON {schema}.audit_log", drop_copy
        )


# --- M2: triggers on the log -----------------------------------------------------------------


def _trigger_function(schema: str) -> tuple[str, str]:
    function = f"public.{schema}_copy_row()"
    return (
        f"CREATE FUNCTION {function} RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
        f"DROP FUNCTION IF EXISTS {function}",
    )


async def test_the_guard_refuses_a_trigger_on_the_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ax_trigger"
    database, _ = await _created(scoped_database, tmp_path, schema)
    create_function, drop_function = _trigger_function(schema)
    await execute(database.superuser, create_function)
    try:
        with pytest.raises(DBAPIError, match=rf"triggers on the audit log.*leak.*{HINT}"):
            await execute(
                database.superuser,
                f"CREATE TRIGGER leak AFTER INSERT ON {schema}.audit_log "
                f"FOR EACH ROW EXECUTE FUNCTION public.{schema}_copy_row()",
            )
    finally:
        await execute(
            database.superuser, f"DROP TRIGGER IF EXISTS leak ON {schema}.audit_log", drop_function
        )


async def test_a_trigger_on_the_log_made_behind_the_guards_back_is_reported(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ax_trigger_drift"
    database, application = await _created(scoped_database, tmp_path, schema)
    trigger = (
        f"CREATE TRIGGER leak AFTER INSERT ON {schema}.audit_log "
        f"FOR EACH ROW EXECUTE FUNCTION public.{schema}_copy_row()",
        f"DROP TRIGGER IF EXISTS leak ON {schema}.audit_log",
    )
    async with _all_drifted(database, _trigger_function(schema), trigger):
        startup = await _startup(database)
        report = await verify(database.superuser, application)

    assert "leak" in startup["audit.log_trigger"]
    assert HINT in startup["audit.log_trigger"]
    assert "leak" in _by_check(report)["audit.log_trigger"]
    assert "assertion" in _by_check(report)


# --- M3: security definer functions ----------------------------------------------------------


async def test_an_exposed_overload_of_the_writer_that_reads_the_log_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The verifier's probe: the name allow-list let this pass with ``ok``."""
    schema = "ax_overload"
    database, application = await _created(scoped_database, tmp_path, schema)
    function = f"{schema}.write_audit_row(integer)"
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {function} RETURNS bigint LANGUAGE sql SECURITY DEFINER "
            f"AS 'SELECT count(*) FROM {schema}.audit_log'",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP FUNCTION IF EXISTS {function}")

    assert not report.ok
    assert f"{schema}.write_audit_row(integer)" in _by_check(report)["audit.definer_reads"]


async def test_a_definer_in_another_schema_is_listed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ax_side"
    database, application = await _created(scoped_database, tmp_path, schema)
    side = f"{schema}_side"
    await execute(database.superuser, f"CREATE SCHEMA {side} AUTHORIZATION {schema}_owner")
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {side}.helper() RETURNS int LANGUAGE sql SECURITY DEFINER "
            "AS 'SELECT 1'",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP SCHEMA IF EXISTS {side} CASCADE")

    [warning] = report.warnings
    assert warning.check == "audit.definer"
    assert f"{side}.helper()" in warning.actual
    assert "loom_guard" not in warning.actual


async def test_an_owner_security_invoker_function_is_neither_finding_nor_warning(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Verifier F3: only definer rights matter; an invoker runs with the caller's."""
    schema = "ax_invoker_fn"
    database, application = await _created(scoped_database, tmp_path, schema)
    function = f"{schema}.helper()"
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {function} RETURNS bigint LANGUAGE sql "
            f"AS 'SELECT count(*) FROM {schema}.audit_log'",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP FUNCTION IF EXISTS {function}")

    assert report.findings == ()
    assert report.warnings == ()


# --- L1: the upgrade names the view ----------------------------------------------------------


async def _refused(bootstrap: Awaitable[object]) -> BaseException:
    """The error *bootstrap* raises; fails the test when it raises none."""
    try:
        await bootstrap
    except Exception as exc:  # noqa: BLE001 - the bootstrap raises the driver's error as is
        return exc
    pytest.fail("the upgrade did not refuse")


def _sqlstate(error: BaseException) -> str | None:
    """The SQLSTATE of a driver error, unwrapped from SQLAlchemy's when wrapped."""
    state = getattr(error, "sqlstate", None) or getattr(
        getattr(error, "orig", None), "sqlstate", None
    )
    return None if state is None else str(state)


@pytest.mark.parametrize("where", ["public", "schema"])
async def test_an_upgrade_over_a_view_fails_naming_it_and_succeeds_once_it_is_dropped(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    """The verifier's upgrade probe: revision 3 with a view over the log, then revision 4."""
    schema = f"ax_up_{where}"
    view = f"public.{schema}_leak" if where == "public" else f"{schema}.leak"
    monkeypatch.setattr(
        guard_manifest, "GUARD_REVISIONS", guard_manifest.GUARD_REVISIONS[:AUDIT_GUARD_REVISION]
    )
    database = await scoped_database(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )
    creator = database.superuser if where == "public" else database.migrator
    await execute(creator, f"CREATE VIEW {view} AS SELECT * FROM {schema}.audit_log")
    monkeypatch.undo()
    applied = f"SELECT string_agg(n::text, ',' ORDER BY n) FROM loom_guard_{schema}.revision"
    try:
        error = await _refused(scoped_database(schema))
        assert _sqlstate(error) == "LG002"
        assert view.rpartition(".")[2] in str(error)
        assert "security_invoker" in str(error)
        assert await scalar(database.superuser, applied) == "1,2,3"
    finally:
        await execute(database.superuser, f"DROP VIEW IF EXISTS {view}")

    await scoped_database(schema)
    assert await scalar(database.superuser, applied) == "1,2,3,4"


# --- L3: security_invoker views --------------------------------------------------------------


async def test_a_security_invoker_view_is_allowed_and_a_view_over_it_is_not(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """An invoker view reads with the caller's rights; one over it runs as its owner (PG 15+)."""
    schema = "ax_invoker"
    database, _ = await _created(scoped_database, tmp_path, schema)
    if int(await scalar(database.superuser, "SHOW server_version_num")) < 150000:
        pytest.skip("security_invoker views exist from Postgres 15")
    invoker, outer = f"public.{schema}_inv", f"public.{schema}_outer"
    try:
        await execute(
            database.superuser,
            f"CREATE VIEW {invoker} WITH (security_invoker=on) AS SELECT * FROM {schema}.audit_log",
        )
        assert "audit.view" not in await _startup(database)
        with pytest.raises(DBAPIError, match=rf"{schema}_outer") as refused:
            await execute(database.superuser, f"CREATE VIEW {outer} AS SELECT id FROM {invoker}")
        assert f"{schema}_inv," not in str(refused.value)
    finally:
        await execute(
            database.superuser, f"DROP VIEW IF EXISTS {outer}", f"DROP VIEW IF EXISTS {invoker}"
        )


# --- Info: a guard without configuration ---------------------------------------------------


async def test_ddl_runs_while_the_guard_has_no_configuration(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """``format('%I', NULL)`` raises; the view check must not run before ``configure``."""
    schema = "ax_unconfigured"
    database, _ = await _created(scoped_database, tmp_path, schema)
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f"DELETE FROM loom_guard_{schema}.config"))
            await connection.execute(text(f"CREATE TABLE public.{schema}_t (x int)"))
            await connection.rollback()
    finally:
        await engine.dispose()


# --- Verifier gaps F2, and one query for guard and Python --------------------------------


async def test_a_chain_made_behind_the_guards_back_stops_startup_and_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Verifier F2: a materialized view over a partition and a two-level chain."""
    schema = "ax_chain"
    database, application = await _created(scoped_database, tmp_path, schema)
    async with _all_drifted(database, *_chain(schema)):
        startup = await _startup(database)
        report = await verify(database.superuser, application)

    found = _by_check(report)
    for actual in (startup["audit.view"], found["audit.view"]):
        for name in ("mv", "inner", "outer"):
            assert f"public.{schema}_{name}" in actual
        assert HINT in actual
    assert "assertion" in found


async def test_the_guard_and_startup_find_the_same_readers(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """One definition in two languages: the guard function and ``AUDIT_LOG_VIEWS`` agree."""
    schema = "ax_parity"
    database, _ = await _created(scoped_database, tmp_path, schema)
    copy = f"public.{schema}_copy"
    rules = (
        _copy_table(schema),
        (f"CREATE TABLE public.{schema}_t (x int)", f"DROP TABLE IF EXISTS public.{schema}_t"),
        (
            f"CREATE RULE leak AS ON INSERT TO public.{schema}_t "
            f"DO ALSO INSERT INTO {copy} SELECT id FROM {schema}.audit_log",
            f"DROP RULE IF EXISTS leak ON public.{schema}_t",
        ),
        (
            f"CREATE RULE dup AS ON INSERT TO {schema}.audit_log "
            f"DO ALSO INSERT INTO {copy} VALUES (NEW.id)",
            f"DROP RULE IF EXISTS dup ON {schema}.audit_log",
        ),
    )
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with _all_drifted(database, *_chain(schema), *rules), engine.connect() as connection:
            guard = await connection.execute(
                text(f"SELECT * FROM loom_guard_{schema}.audit_log_readers()")
            )
            guard_names = {str(row[0]) for row in guard}
            python = await connection.execute(text(AUDIT_LOG_VIEWS), {"schema": schema})
            python_names = {str(row.name) for row in python}
    finally:
        await engine.dispose()

    assert guard_names == python_names
    assert len(python_names) == 5


# --- Verifier round 3: mutants the suite let through ----------------------------------------


async def test_a_begin_atomic_definer_in_another_schema_reading_a_partition_is_drift(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Its ``prosrc`` is empty: only its dependency on the partition shows that it reads."""
    schema = "ax_atomic"
    database, application = await _created(scoped_database, tmp_path, schema)
    side = f"{schema}_side"
    await execute(database.superuser, f"CREATE SCHEMA {side} AUTHORIZATION {schema}_owner")
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {side}.peek() RETURNS bigint LANGUAGE sql SECURITY DEFINER "
            f"BEGIN ATOMIC SELECT count(*) FROM {schema}.{_partition()}; END",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP SCHEMA IF EXISTS {side} CASCADE")

    assert not report.ok
    assert f"{side}.peek()" in _by_check(report)["audit.definer_reads"]


async def test_a_definer_only_the_readers_may_execute_is_drift_when_it_reads_the_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Not PUBLIC: an application role's EXECUTE alone escalates it."""
    schema = "ax_readers_fn"
    database, application = await _created(scoped_database, tmp_path, schema)
    function = f"{schema}.peek()"
    try:
        await execute(
            database.migrator,
            f"CREATE FUNCTION {function} RETURNS bigint LANGUAGE sql SECURITY DEFINER "
            f"AS 'SELECT count(*) FROM {schema}.audit_log'",
            f"REVOKE EXECUTE ON FUNCTION {function} FROM PUBLIC",
            f"GRANT EXECUTE ON FUNCTION {function} TO {schema}_readers",
        )
        report = await verify(database.superuser, application)
    finally:
        await execute(database.superuser, f"DROP FUNCTION IF EXISTS {function}")

    assert not report.ok
    assert f"{schema}.peek()" in _by_check(report)["audit.definer_reads"]


async def test_an_owner_view_over_an_invoker_view_is_reported_and_the_invoker_one_is_not(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The exemption skips the invoker view, never the walk through it (Postgres 15+)."""
    schema = "ax_invoker_drift"
    database, application = await _created(scoped_database, tmp_path, schema)
    if int(await scalar(database.superuser, "SHOW server_version_num")) < 150000:
        pytest.skip("security_invoker views exist from Postgres 15")
    invoker, outer = f"public.{schema}_inv", f"public.{schema}_outer"
    views = (
        (
            f"CREATE VIEW {invoker} WITH (security_invoker=on) AS SELECT * FROM {schema}.audit_log",
            f"DROP VIEW IF EXISTS {invoker} CASCADE",
        ),
        (f"CREATE VIEW {outer} AS SELECT id FROM {invoker}", f"DROP VIEW IF EXISTS {outer}"),
    )
    async with _all_drifted(database, *views):
        startup = await _startup(database)
        report = await verify(database.superuser, application)

    for actual in (startup["audit.view"], _by_check(report)["audit.view"]):
        assert outer in actual
        assert f"{invoker}," not in actual
        assert f"{invoker};" not in actual


async def test_ddl_on_the_guard_is_refused_while_it_has_no_configuration(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The early return for a guard without configuration comes after the guard's own fence."""
    schema = "ax_fence"
    database, _ = await _created(scoped_database, tmp_path, schema)
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f"DELETE FROM loom_guard_{schema}.config"))
            with pytest.raises(DBAPIError, match="outside the loom installer"):
                await connection.execute(text(f"CREATE TABLE loom_guard_{schema}.extra (x int)"))
            await connection.rollback()
    finally:
        await engine.dispose()


def _sub_partitioned(schema: str) -> tuple[tuple[str, str], ...]:
    """A registered, protected log partition for 2099-01 that is itself partitioned."""
    log, guard, owner = f"{schema}.audit_log", f"loom_guard_{schema}", f"{schema}_owner"
    part, leaf = f"{schema}.audit_log_p209901", f"{schema}.audit_log_p209901_a"
    truncate = "loom_audit_log_immutable_truncate"
    return (
        (
            f"CREATE TABLE {part} PARTITION OF {log} FOR VALUES FROM ('2099-01-01') "
            "TO ('2099-02-01') PARTITION BY RANGE (at)",
            f"DROP TABLE IF EXISTS {part} CASCADE",
        ),
        (f"ALTER TABLE {part} OWNER TO {owner}", "SELECT 1"),
        (
            f"CREATE TRIGGER {truncate} BEFORE TRUNCATE ON {part} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {guard}.refuse_audit_log_change()",
            "SELECT 1",
        ),
        (f"ALTER TABLE {part} ENABLE ALWAYS TRIGGER {truncate}", "SELECT 1"),
        (
            f"INSERT INTO {guard}.audit_partition VALUES ('{part}', '2099-01-01', '2099-02-01')",
            f"DELETE FROM {guard}.audit_partition WHERE rel::text = '{part}'",
        ),
        (
            f"CREATE TABLE {leaf} PARTITION OF {part} FOR VALUES FROM ('2099-01-01') "
            "TO ('2099-01-15')",
            f"DROP TABLE IF EXISTS {leaf}",
        ),
        (f"ALTER TABLE {leaf} OWNER TO {owner}", "SELECT 1"),
    )


async def test_the_assertion_finds_a_trigger_on_a_sub_partition_of_the_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The guard walks the whole partition tree, as startup's ``LOG_TRIGGERS`` does.

    A registered log partition that is itself partitioned is refused on its own (T12c fix
    round, F5): the guard's other log checks look one level down.
    """
    schema = "ax_subpart"
    database, application = await _created(scoped_database, tmp_path, schema)
    leaf = f"{schema}.audit_log_p209901_a"
    leak = (
        f"CREATE TRIGGER leak AFTER INSERT ON {leaf} "
        f"FOR EACH ROW EXECUTE FUNCTION public.{schema}_copy_row()",
        f"DROP TRIGGER IF EXISTS leak ON {leaf}",
    )
    parts = _sub_partitioned(schema)
    async with _all_drifted(database, *parts):
        clean = _by_check(await verify(database.superuser, application))
        async with _all_drifted(database, _trigger_function(schema), leak):
            startup = await _startup(database)
            found = _by_check(await verify(database.superuser, application))

    assert "audit log partitions that are not plain tables" in clean["assertion"]
    assert "audit_log_p209901" in clean["assertion"]
    assert f"{leaf}.leak" in startup["audit.log_trigger"]
    assert "triggers on the audit log" in found["assertion"]
    assert "audit_log_p209901_a.leak" in found["assertion"]
