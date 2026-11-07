"""Keep the store's audit log in step with the models that declare ``Audited``.

One reconciliation, run as the schema's owner inside the deploy transaction
(after ``create_all`` or after the structural migrations): it prepares the
audit log when a table is audited, protects every declared table with its
current exclusions, stops auditing a table whose marker was removed or every
table when ``schema.audit.enabled`` is false, and creates this month's and the
next month's partitions. History is never touched. Every call goes through
the guard; names travel as bound parameters and Postgres quotes them.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.model.audited import AuditedTables
from loom.core.model.partition import range_partitions
from loom.core.schema_names import schema_identifier, sql_identifier

AUDIT_LOG: Final = "audit_log"
PREPARE_AUDIT_LOG: Final = "SELECT prepare_audit_log()"
PROTECT_AUDITED: Final = (
    "SELECT protect_audited_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:excluded AS text[]))"
)
UNPROTECT_AUDITED: Final = "SELECT unprotect_audited_table(CAST(:rel AS regclass))"
REGISTERED_AUDITED: Final = (
    "SELECT n.nspname, c.relname, a.rel::text FROM audited_table a "
    "JOIN pg_class c ON c.oid = a.rel JOIN pg_namespace n ON n.oid = c.relnamespace"
)
AUDIT_SUPPORTED: Final = "SELECT to_regprocedure('unprotect_audited_table(regclass)') IS NOT NULL"
AUDIT_LOG_EXISTS: Final = "SELECT to_regclass(quote_ident(:schema) || '.audit_log') IS NOT NULL"
CREATE_AUDIT_PARTITION: Final = (
    "SELECT create_audit_partition(CAST(:partition AS text), CAST(:lower AS text), "
    "CAST(:upper AS text))"
)
OLDEST_EXPIRED_AUDIT_PARTITION: Final = (
    "SELECT c.relname FROM audit_partition p JOIN pg_class c ON c.oid = p.rel "
    "CROSS JOIN config cfg WHERE cfg.retention_months IS NOT NULL "
    "AND p.upper_bound "
    "<= date_trunc('month', now()) - make_interval(months => cfg.retention_months) "
    "ORDER BY p.upper_bound LIMIT 1"
)
DROP_EXPIRED_AUDIT_PARTITION: Final = (
    "SELECT drop_expired_audit_partition(CAST(:partition AS text))"
)
_PREPARE_AUDIT_LOG: Final = text(PREPARE_AUDIT_LOG)
_PROTECT_AUDITED: Final = text(PROTECT_AUDITED)
_UNPROTECT_AUDITED: Final = text(UNPROTECT_AUDITED)
_REGISTERED_AUDITED: Final = text(REGISTERED_AUDITED)
_AUDIT_SUPPORTED: Final = text(AUDIT_SUPPORTED)
_AUDIT_LOG_EXISTS: Final = text(AUDIT_LOG_EXISTS)
_CREATE_AUDIT_PARTITION: Final = text(CREATE_AUDIT_PARTITION)
_OLDEST_EXPIRED_AUDIT_PARTITION: Final = text(OLDEST_EXPIRED_AUDIT_PARTITION)
_DROP_EXPIRED_AUDIT_PARTITION: Final = text(DROP_EXPIRED_AUDIT_PARTITION)


@dataclass(frozen=True, slots=True)
class AuditReconciliation:
    """What one reconciliation changed, table names unqualified."""

    protected: tuple[str, ...] = ()
    unprotected: tuple[str, ...] = ()
    partitions: tuple[str, ...] = ()


def reconcile_audited(
    connection: Connection,
    *,
    schema: str,
    declared: AuditedTables,
    today: dt.date | None = None,
) -> AuditReconciliation:
    """Bring the guard's audited tables and the log's partitions in line with *declared*.

    The caller's connection acts as the schema's owner with the guard first on
    its path; everything runs in the caller's transaction, so the guard's
    assertion at commit sees the final state. On a guard that predates the
    audit log and with nothing to audit, nothing is done.

    Args:
        connection: The migrator's connection, inside the deploy transaction.
        schema: The application schema.
        declared: The tables that record rows (``Application.active_audited``);
            empty stops auditing every registered table.
        today: The day partitions are created around; today when absent.
    """
    schema = schema_identifier(schema)
    if not declared and not connection.execute(_AUDIT_SUPPORTED).scalar():
        return AuditReconciliation()
    if declared:
        connection.execute(_PREPARE_AUDIT_LOG)
    registered = _registered(connection, schema)
    protected = _protect(connection, schema, declared, registered)
    unprotected = _unprotect(connection, {name for _, name in declared}, registered)
    return AuditReconciliation(protected, unprotected, _partitions(connection, schema, today))


def _partitions(connection: Connection, schema: str, today: dt.date | None) -> tuple[str, ...]:
    """This month's and the next month's partitions, created when the log exists."""
    if not connection.execute(_AUDIT_LOG_EXISTS, {"schema": schema}).scalar():
        return ()
    return ensure_audit_partitions(connection, today or dt.date.today())


def _registered(connection: Connection, schema: str) -> dict[str, str]:
    """The guard's audited tables of *schema*: unqualified name to qualified relation."""
    rows = connection.execute(_REGISTERED_AUDITED)
    return {str(name): str(rel) for owner, name, rel in rows if owner == schema}


def _protect(
    connection: Connection, schema: str, wanted: AuditedTables, registered: Mapping[str, str]
) -> tuple[str, ...]:
    """Protect every *wanted* table, refreshing its exclusions; return the newly audited."""
    for (_, name), table in sorted(wanted.items(), key=lambda item: item[0][1]):
        connection.execute(
            _PROTECT_AUDITED,
            {"schema": schema, "table": sql_identifier(name), "excluded": sorted(table.excluded)},
        )
    return tuple(sorted(name for _, name in wanted if name not in registered))


def _unprotect(
    connection: Connection, names: set[str], registered: Mapping[str, str]
) -> tuple[str, ...]:
    """Stop auditing every registered table not in *names*; the log keeps its history."""
    unprotected = tuple(sorted(name for name in registered if name not in names))
    for name in unprotected:
        connection.execute(_UNPROTECT_AUDITED, {"rel": registered[name]})
    return unprotected


def ensure_audit_partitions(
    connection: Connection, today: dt.date, months: int = 2
) -> tuple[str, ...]:
    """Create the audit log's partitions for *today*'s month and the following ones.

    A partition that exists is left alone. Returns the names created, in order.
    """
    start = today.replace(day=1)
    end = start
    for _ in range(months):
        end = (end.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    created: list[str] = []
    for partition in range_partitions(AUDIT_LOG, start, end):
        parameters = {
            "partition": partition.name,
            "lower": partition.lower,
            "upper": partition.upper,
        }
        if connection.execute(_CREATE_AUDIT_PARTITION, parameters).scalar():
            created.append(partition.name)
    return tuple(created)


async def drop_expired_audit_partition(migrator_url: str) -> str | None:
    """Drop the oldest audit partition past the configured retention; return its name.

    One partition per call, so a wrong retention costs at most one month
    before someone notices (FR-017). ``None`` when nothing is expired or no
    retention is configured. The guard re-checks the retention and refuses a
    partition within it, whatever this query found.
    """
    engine = create_async_engine(migrator_url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            oldest = (await connection.execute(_OLDEST_EXPIRED_AUDIT_PARTITION)).scalar()
            if oldest is None:
                return None
            await connection.execute(_DROP_EXPIRED_AUDIT_PARTITION, {"partition": oldest})
            return str(oldest)
    finally:
        await engine.dispose()
