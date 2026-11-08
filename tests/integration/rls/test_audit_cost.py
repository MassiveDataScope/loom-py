"""SC-004: the write cost the audit trigger adds, measured and printed; informative, not a gate.

Protocol: the audited ``accounts`` table with its audit trigger enabled,
then disabled, so only that trigger differs; a thousand single-row updates
of two columns, the median of five series, written as the superuser.

Two figures. Each update committed on its own from the client: what an
application write pays, and what the 30 % target is about. The same updates
in one server-side loop: the trigger's own cost per row, nothing around it.
"""

from __future__ import annotations

import statistics
import time
import warnings
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMA = "ledger_cost"
GUARD_EVENT_TRIGGERS = (f"loom_guard_{SCHEMA}_ddl", f"loom_guard_{SCHEMA}_drop")
ROWS = 1000
SERIES = 5
TARGET = 0.30
FILL = (
    "INSERT INTO {table} (tenant, id, name, email, balance, currency, active, internal_note, "
    "api_token) SELECT 'acme', i, 'n', 'e', 0, 'EUR', true, '', '' "
    "FROM generate_series(1, {rows}) i"
)
LOOP = (
    "DO $$ BEGIN FOR i IN 1..{rows} LOOP "
    "UPDATE {table} SET balance = balance + 1, name = name || 'x' "
    "WHERE tenant = 'acme' AND id = i; "
    "END LOOP; END $$"
)


UPDATE = (
    "UPDATE {table} SET balance = balance + 1, name = name || 'x' "
    "WHERE tenant = 'acme' AND id = :id"
)


async def _looped(session: AsyncConnection, table: str) -> float:
    """One series in a single server-side loop."""
    started = time.perf_counter()
    await session.execute(text(LOOP.format(rows=ROWS, table=table)))
    return time.perf_counter() - started


async def _committed(session: AsyncConnection, table: str) -> float:
    """One series, each update committed on its own from the client."""
    statement = text(UPDATE.format(table=table))
    started = time.perf_counter()
    for row in range(1, ROWS + 1):
        await session.execute(statement, {"id": row})
    return time.perf_counter() - started


async def _medians(session: AsyncConnection, table: str) -> dict[str, float]:
    """Median times with the audit trigger on and off, the series alternating.

    One unmeasured round first, then each round runs both states, so a cold
    cache or a checkpoint lands on both alike.
    """
    timings: dict[str, list[float]] = {key: [] for key in ("committed", "looped")}
    timings |= {f"{key}_off": [] for key in ("committed", "looped")}
    for round_ in range(SERIES + 1):
        for state, suffix in (("ENABLE ALWAYS", ""), ("DISABLE", "_off")):
            await _audit_trigger(session, state)
            committed, looped = await _committed(session, table), await _looped(session, table)
            if round_:
                timings[f"committed{suffix}"].append(committed)
                timings[f"looped{suffix}"].append(looped)
    await _audit_trigger(session, "ENABLE ALWAYS")
    return {key: statistics.median(values) for key, values in timings.items()}


async def _audit_trigger(session: AsyncConnection, state: str) -> None:
    """Switch the audit trigger of ``accounts``; the guard would refuse it, so it is paused."""
    for name in GUARD_EVENT_TRIGGERS:
        await session.execute(text(f"ALTER EVENT TRIGGER {name} DISABLE"))
    await session.execute(text(f"ALTER TABLE {SCHEMA}.accounts {state} TRIGGER loom_audit_record"))
    for name in GUARD_EVENT_TRIGGERS:
        await session.execute(text(f"ALTER EVENT TRIGGER {name} ENABLE ALWAYS"))


async def test_the_audit_trigger_cost_is_measured_against_the_target(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(SCHEMA)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    )
    audited = f"{SCHEMA}.accounts"
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(FILL.format(table=audited, rows=ROWS)))
        async with engine.connect() as connection:
            session = await connection.execution_options(isolation_level="AUTOCOMMIT")
            median = await _medians(session, audited)
    finally:
        await engine.dispose()

    committed_baseline, committed_audit = median["committed_off"], median["committed"]
    baseline, with_audit = median["looped_off"], median["looped"]
    overhead = committed_audit / committed_baseline - 1
    per_row_us = (with_audit - baseline) / ROWS * 1_000_000
    print(  # noqa: T201 - the measurement is this test's output
        f"\nSC-004: {ROWS} updates, median of {SERIES}. Committed one by one: "
        f"{committed_baseline * 1000:.0f} ms unaudited, {committed_audit * 1000:.0f} ms audited, "
        f"overhead {overhead:.0%} (target under {TARGET:.0%}). Server-side loop: "
        f"{baseline * 1000:.1f} ms unaudited, {with_audit * 1000:.1f} ms audited, "
        f"{per_row_us:.0f} us per audited row"
    )
    if overhead >= TARGET:
        warnings.warn(
            f"audit overhead {overhead:.0%} is over the {TARGET:.0%} target", stacklevel=1
        )
    assert committed_baseline > 0
