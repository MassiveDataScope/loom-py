"""What one DDL costs the guards of a database, counted, not timed (T12c).

Every store's guard has its own event triggers, so every DDL wakes every guard.
Production must never pay a full assertion per store or per statement: a guard
asserts only DDL that can touch its store, once per transaction, at COMMIT.

Each test turns on ``track_functions`` for its database and reads how many
times each guard function ran from ``pg_stat_user_functions``. The DDL runs in
its own session, which then calls ``public.cost_marker()`` and disconnects; the
counts are read once the marker's call is visible, so they hold all of that
session's calls (Postgres 14's collector included).
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
)

pytestmark = pytest.mark.integration

ASSERT = "assert_scoped_schema"
WALK = "audit_log_readers"
EXPENSIVE = (ASSERT, WALK)
MARKER = "cost_marker"
STORES = ("cost_a", "cost_b", "cost_c", "cost_d", "cost_e")
NOOP = (
    "CREATE FUNCTION public.cost_noop() RETURNS trigger LANGUAGE plpgsql "
    "AS 'BEGIN RETURN NULL; END'"
)
_STATS = (
    "SELECT schemaname, funcname, calls FROM pg_stat_user_functions "
    "WHERE schemaname LIKE 'loom\\_guard\\_%' OR funcname = $1"
)


def _dsn(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


async def _counts(superuser: str) -> Counter[tuple[str, str]]:
    connection = await asyncpg.connect(_dsn(superuser))
    try:
        await connection.execute("SELECT pg_stat_clear_snapshot()")
        rows = await connection.fetch(_STATS, MARKER)
    finally:
        await connection.close()
    return Counter({(str(r["schemaname"]), str(r["funcname"])): int(r["calls"]) for r in rows})


async def _settled(superuser: str, marker: int) -> Counter[tuple[str, str]]:
    """The counts once the marker has been called ``marker`` times."""
    for _ in range(200):
        counts = await _counts(superuser)
        if counts[("public", MARKER)] >= marker:
            return counts
        await asyncio.sleep(0.05)
    raise AssertionError(f"the statistics never showed {MARKER} called {marker} times")


async def _flushed(superuser: str) -> Counter[tuple[str, str]]:
    """The counts once every earlier session's statistics have arrived.

    Before Postgres 15 a closed session reports its calls to the statistics
    collector asynchronously, so counts read right after the setup can miss
    work the setup did; that work would then be measured as the DDL's. A
    marker called from a new session, and waited for, comes after it. Every
    earlier marker call was waited for, so the count read first is exact.
    """
    called = (await _counts(superuser))[("public", MARKER)]
    connection = await asyncpg.connect(_dsn(superuser))
    try:
        await connection.execute(f"SELECT public.{MARKER}()")
    finally:
        await connection.close()
    return await _settled(superuser, called + 1)


async def _prepared(superuser: str) -> None:
    """Track every function of this test's database, and create the marker."""
    connection = await asyncpg.connect(_dsn(superuser))
    try:
        database = await connection.fetchval("SELECT current_database()")
        await connection.execute(f"ALTER DATABASE \"{database}\" SET track_functions = 'all'")
        await connection.execute(
            f"CREATE FUNCTION public.{MARKER}() RETURNS void LANGUAGE plpgsql AS 'BEGIN END'"
        )
    finally:
        await connection.close()


async def _measured(
    superuser: str, url: str, statements: Sequence[str]
) -> Counter[tuple[str, str]]:
    """Each guard function's calls while *statements* run in one transaction as *url*'s user."""
    before = await _flushed(superuser)
    connection = await asyncpg.connect(_dsn(url))
    try:
        async with connection.transaction():
            for statement in statements:
                await connection.execute(statement)
        await connection.execute(f"SELECT public.{MARKER}()")
    finally:
        await connection.close()
    after = await _settled(superuser, before[("public", MARKER)] + 1)
    after.subtract(before)
    return +after


def _calls(counts: Counter[tuple[str, str]], function: str) -> dict[str, int]:
    """*function*'s calls by store, every store listed."""
    return {store: counts[(f"loom_guard_{store}", function)] for store in STORES}


def _profile(counts: Counter[tuple[str, str]], store: str) -> dict[str, int]:
    """Every guard function *store* ran, with its calls."""
    return {f: n for (schema, f), n in counts.items() if schema == f"loom_guard_{store}"}


def _only(store: str, calls: int) -> dict[str, int]:
    return {s: calls if s == store else 0 for s in STORES}


async def _stores(
    factory: BootstrapFactory, tmp_path: Path, names: Sequence[str]
) -> list[ScopedDatabase]:
    """Bootstrap and create the audited ``bookkeeping`` product once per name."""
    built = []
    for name in names:
        database = await factory(name)
        application = application_for(bookkeeping, database, tmp_path, schema=name)
        await create_schema(database.migrator, application)
        built.append(database)
    return built


@pytest.fixture
async def five(
    database_uri: str, scoped_database: BootstrapFactory, tmp_path: Path
) -> list[ScopedDatabase]:
    """Five audited stores in the test's database, with every function tracked."""
    await _prepared(database_uri)
    return await _stores(scoped_database, tmp_path, STORES)


async def test_a_grant_in_one_store_asserts_that_store_alone_once(
    five: list[ScopedDatabase],
) -> None:
    """Case 1: a GRANT names no schema, yet only the store whose table it changes asserts."""
    a = five[0]

    counts = await _measured(
        a.superuser, a.migrator, ["GRANT SELECT ON cost_a.accounts TO cost_a_readers"]
    )

    assert _calls(counts, ASSERT) == _only("cost_a", 1)
    assert _calls(counts, WALK) == _only("cost_a", 1)


async def test_a_long_migration_in_one_store_asserts_it_once_at_commit(
    five: list[ScopedDatabase],
) -> None:
    """Case 2: fifty statements, one full assertion, run by the deferred hatch check."""
    a = five[0]
    statements = [f"ALTER TABLE cost_a.drafts ADD COLUMN c{i} integer" for i in range(50)]

    counts = await _measured(a.superuser, a.migrator, statements)

    assert _calls(counts, ASSERT) == _only("cost_a", 1)
    assert _calls(counts, WALK) == _only("cost_a", 1)
    assert _calls(counts, "close_hatch") == _only("cost_a", 1)


async def test_ddl_outside_every_store_asserts_nothing(
    five: list[ScopedDatabase],
) -> None:
    """Case 3: a table in ``public`` is no store's concern."""
    a = five[0]

    counts = await _measured(a.superuser, a.superuser, ["CREATE TABLE public.cost_free (id int)"])

    for function in EXPENSIVE:
        assert _calls(counts, function) == _only("cost_a", 0), function


async def test_a_view_over_a_log_is_still_refused_at_commit(
    five: list[ScopedDatabase],
) -> None:
    """Case 4: the view statement passes; COMMIT runs the walk and refuses it with LG002."""
    a = five[0]
    connection = await asyncpg.connect(_dsn(a.superuser))
    try:
        transaction = connection.transaction()
        await transaction.start()
        await connection.execute("CREATE VIEW public.cost_leak AS SELECT * FROM cost_a.audit_log")
        with pytest.raises(asyncpg.PostgresError) as refused:
            await transaction.commit()
    finally:
        await connection.close()

    assert getattr(refused.value, "sqlstate", None) == "LG002"
    assert "public.cost_leak" in str(refused.value)
    assert "loom_guard[cost_a]" in str(refused.value)


async def test_the_cost_of_a_migration_does_not_grow_with_the_stores(
    database_uri: str, scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Case 5: the same migration, with two stores and then five, runs the same expensive calls."""
    await _prepared(database_uri)
    first, _ = await _stores(scoped_database, tmp_path, STORES[:2])

    await execute(first.superuser, NOOP)

    def migration(round_: int) -> list[str]:
        return [
            "GRANT SELECT ON cost_a.accounts TO cost_a_readers",
            f"ALTER TABLE cost_a.drafts ADD COLUMN r{round_} integer",
            f"CREATE TRIGGER t{round_} AFTER INSERT ON cost_a.drafts "
            "FOR EACH ROW EXECUTE FUNCTION public.cost_noop()",
            f"DROP TRIGGER t{round_} ON cost_a.drafts",
        ]

    with_two = await _measured(first.superuser, first.migrator, migration(2))
    await _stores(scoped_database, tmp_path, STORES[2:])
    with_five = await _measured(first.superuser, first.migrator, migration(5))

    for function in EXPENSIVE:
        assert sum(_calls(with_five, function).values()) == sum(
            _calls(with_two, function).values()
        ), function
        assert _calls(with_five, function) == _only("cost_a", 1), function
    # A store the migration does not touch pays the same calls of every guard function, however
    # many stores (and catalogue rows) the database holds.
    assert _profile(with_five, "cost_b") == _profile(with_two, "cost_b")


async def test_a_trigger_wakes_only_the_store_of_its_table(
    five: list[ScopedDatabase],
) -> None:
    """A trigger reports no schema; the guard finds it through its table."""
    a = five[0]
    await execute(a.superuser, NOOP)

    counts = await _measured(
        a.superuser,
        a.superuser,
        [
            "CREATE TRIGGER noop AFTER INSERT ON cost_b.drafts "
            "FOR EACH ROW EXECUTE FUNCTION public.cost_noop()"
        ],
    )

    assert _calls(counts, ASSERT) == _only("cost_b", 1)


async def test_an_unknown_schemaless_command_wakes_every_store(
    five: list[ScopedDatabase],
) -> None:
    """Fail closed: a command naming no schema and not on the harmless list asserts everywhere."""
    a = five[0]

    counts = await _measured(
        a.superuser, a.superuser, ["COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language'"]
    )

    assert _calls(counts, ASSERT) == dict.fromkeys(STORES, 1)


async def test_an_unknown_schemaless_drop_wakes_every_store(
    five: list[ScopedDatabase],
) -> None:
    """Fail closed: a dropped object naming no schema and not listed harmless asserts everywhere."""
    a = five[0]
    await execute(a.superuser, "CREATE SCHEMA cost_ext", "CREATE EXTENSION pg_trgm SCHEMA cost_ext")

    counts = await _measured(a.superuser, a.superuser, ["DROP EXTENSION pg_trgm"])

    assert _calls(counts, ASSERT) == dict.fromkeys(STORES, 1)


async def test_a_partition_made_in_another_schema_wakes_the_store_of_its_parent(
    five: list[ScopedDatabase],
) -> None:
    """The new partition reports its own schema; the guard finds the store through its parent."""
    a = five[0]
    parent = "CREATE TABLE cost_a.events (kind text NOT NULL) PARTITION BY LIST (kind)"
    await execute(a.migrator, parent)

    counts = await _measured(
        a.superuser,
        a.superuser,
        ["CREATE TABLE public.cost_events_x PARTITION OF cost_a.events FOR VALUES IN ('x')"],
    )

    assert _calls(counts, ASSERT) == _only("cost_a", 1)


async def test_a_temporary_view_of_an_application_user_wakes_no_guard(
    five: list[ScopedDatabase],
) -> None:
    """Any role may make temporary views; one that reads no log must not cost every guard."""
    a = five[0]

    counts = await _measured(a.superuser, a.read, ["CREATE TEMP VIEW cost_tmp AS SELECT 1"])

    assert _calls(counts, ASSERT) == _only("cost_a", 0)


async def test_a_temporary_view_over_a_log_is_still_refused_at_commit(
    five: list[ScopedDatabase],
) -> None:
    """A temporary view reads with its owner's rights too; only views reading no log are skipped."""
    a = five[0]
    connection = await asyncpg.connect(_dsn(a.superuser))
    try:
        transaction = connection.transaction()
        await transaction.start()
        await connection.execute("CREATE TEMP VIEW cost_tmp_leak AS SELECT * FROM cost_a.audit_log")
        with pytest.raises(asyncpg.PostgresError) as refused:
            await transaction.commit()
    finally:
        await connection.close()

    assert getattr(refused.value, "sqlstate", None) == "LG002"
    assert "cost_tmp_leak" in str(refused.value)
