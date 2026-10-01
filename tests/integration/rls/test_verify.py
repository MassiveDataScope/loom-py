"""``verify`` reports what the guard's assertion cannot require (T018, FR-024).

Two synthetic products share the module database: ``notes`` brings serial
sequences and an elevable scope, ``ledger`` brings a global table with
declared privileges and a boundary that is a foreign key to it. Every negative
applies one change as the superuser, expects one check to fail, and reverts.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import create_schema, verify
from tests.integration.agnosticism import ledger, notes
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, application_for

pytestmark = pytest.mark.integration


@asynccontextmanager
async def _admin(url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            yield conn
    finally:
        await engine.dispose()


async def _run(url: str, statements: Sequence[str]) -> None:
    """Run statements through the driver's simple protocol, which accepts multi-command scripts."""
    async with _admin(url) as conn:
        raw = await conn.get_raw_connection()
        for statement in statements:
            await raw.driver_connection.execute(statement)


@pytest.fixture(scope="module")
def tmp_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("verify")


@pytest.fixture
async def products(
    scoped_database: BootstrapFactory, tmp_dir: Path
) -> dict[str, tuple[ScopedDatabase, Application]]:
    built: dict[str, tuple[ScopedDatabase, Application]] = {}
    for product in (notes, ledger):
        database = await scoped_database(product.SCHEMA)
        application = application_for(product, database, tmp_dir)
        await create_schema(database.migrator, application)
        built[product.SCHEMA] = (database, application)
    return built


async def _checks(url: str, application: Application) -> set[str]:
    report = await verify(url, application)
    return {finding.check for finding in report.findings}


async def test_a_freshly_created_schema_verifies_clean(products) -> None:
    for database, application in products.values():
        report = await verify(database.superuser, application)
        assert report.ok, report.findings


HATCH = "SELECT set_config('loom_guard_notes.protecting', 'on', true)"
REGISTERED_QUAL = (
    "SELECT qual FROM loom_guard_notes.scoped_policy "
    "WHERE rel = 'notes.notes'::regclass AND policyname = 'loom_select'"
)


@pytest.mark.parametrize(
    ("label", "mutate", "revert", "check"),
    [
        (
            "missing group grant",
            ["REVOKE SELECT ON notes.notes FROM notes_readers"],
            ["GRANT SELECT ON notes.notes TO notes_readers"],
            "group.privileges",
        ),
        (
            "direct grant to a user within the registered set",
            [
                "ALTER EVENT TRIGGER loom_guard_notes_ddl DISABLE",
                "GRANT SELECT ON notes.notes TO notes_rw",
                "ALTER EVENT TRIGGER loom_guard_notes_ddl ENABLE",
            ],
            ["REVOKE SELECT ON notes.notes FROM notes_rw"],
            "assertion",
        ),
        (
            "bypass grant revoked",
            ["REVOKE SELECT ON notes.notes FROM notes_ops"],
            ["GRANT SELECT ON notes.notes TO notes_ops"],
            "bypass.privileges",
        ),
        (
            "bypass grant on alembic_version",
            [
                "SET ROLE notes_owner",
                "CREATE TABLE notes.alembic_version (version_num varchar(32) PRIMARY KEY)",
                "RESET ROLE",
            ],
            ["DROP TABLE notes.alembic_version"],
            "bypass.alembic_version",
        ),
        (
            "sequence privilege other than USAGE for writers",
            ["GRANT SELECT ON SEQUENCE notes.notes_id_seq TO notes_writers"],
            ["REVOKE SELECT ON SEQUENCE notes.notes_id_seq FROM notes_writers"],
            "sequence.usage",
        ),
        (
            "bypass without USAGE on a sequence",
            ["REVOKE USAGE ON SEQUENCE notes.notes_id_seq FROM notes_ops"],
            ["GRANT USAGE ON SEQUENCE notes.notes_id_seq TO notes_ops"],
            "bypass.sequence_usage",
        ),
        (
            "a read user member of writers",
            ["GRANT notes_writers TO notes_ro"],
            ["REVOKE notes_writers FROM notes_ro"],
            "membership.access",
        ),
        (
            "migrator with inherited membership",
            ["GRANT notes_owner TO notes_migrator WITH INHERIT TRUE"],
            ["GRANT notes_owner TO notes_migrator WITH INHERIT FALSE"],
            "migrator.inherit",
        ),
        (
            "owner member of a bypass role",
            ["GRANT notes_ops TO notes_owner"],
            ["REVOKE notes_ops FROM notes_owner"],
            "bypass.membership",
        ),
    ],
)
async def test_notes_negatives_surface_one_check(products, label, mutate, revert, check) -> None:
    database, application = products["notes"]

    await _run(database.superuser, mutate)
    try:
        checks = await _checks(database.superuser, application)
    finally:
        await _run(database.superuser, revert)

    assert check in checks, (label, checks)
    assert (await verify(database.superuser, application)).ok


@pytest.mark.parametrize(
    ("label", "mutate", "revert", "check"),
    [
        (
            "undeclared grant on a global table",
            ["GRANT INSERT ON ledger.accounts TO ledger_writers"],
            ["REVOKE INSERT ON ledger.accounts FROM ledger_writers"],
            "global.privileges",
        ),
        (
            "missing declared grant on a global table",
            ["REVOKE SELECT ON ledger.accounts FROM ledger_readers"],
            ["GRANT SELECT ON ledger.accounts TO ledger_readers"],
            "global.privileges",
        ),
        (
            "DELETE granted to a group on a global table a scoped table references",
            ["GRANT DELETE ON ledger.accounts TO ledger_writers"],
            ["REVOKE DELETE ON ledger.accounts FROM ledger_writers"],
            "c9.group_write",
        ),
    ],
)
async def test_ledger_negatives_surface_one_check(products, label, mutate, revert, check) -> None:
    database, application = products["ledger"]

    await _run(database.superuser, mutate)
    try:
        checks = await _checks(database.superuser, application)
    finally:
        await _run(database.superuser, revert)

    assert check in checks, (label, checks)
    assert (await verify(database.superuser, application)).ok


async def test_findings_name_the_table_the_check_and_both_sides(products) -> None:
    database, application = products["notes"]

    await _run(database.superuser, ["REVOKE SELECT ON notes.notes FROM notes_readers"])
    try:
        report = await verify(database.superuser, application)
    finally:
        await _run(database.superuser, ["GRANT SELECT ON notes.notes TO notes_readers"])

    finding = next(f for f in report.findings if f.check == "group.privileges")
    assert finding.table == "notes"
    assert "SELECT" in finding.expected
    assert finding.actual is not None


@pytest.mark.parametrize(
    ("label", "alteration"),
    [
        ("altered qual with the same policy count", "USING (true)"),
        ("policy roles other than public", "TO notes_readers"),
    ],
)
async def test_a_policy_drift_surfaces_as_an_assertion_finding(products, label, alteration) -> None:
    database, application = products["notes"]
    async with _admin(database.superuser) as conn:
        registered = (await conn.execute(text(REGISTERED_QUAL))).scalar_one()
    drift = f"BEGIN; {HATCH}; ALTER POLICY loom_select ON notes.notes {alteration}; COMMIT"
    restore = (
        f"BEGIN; {HATCH}; "
        f"ALTER POLICY loom_select ON notes.notes TO PUBLIC USING ({registered}); COMMIT"
    )

    await _run(database.superuser, [drift])
    try:
        checks = await _checks(database.superuser, application)
    finally:
        await _run(database.superuser, [restore])

    assert "assertion" in checks, (label, checks)
    assert (await verify(database.superuser, application)).ok
