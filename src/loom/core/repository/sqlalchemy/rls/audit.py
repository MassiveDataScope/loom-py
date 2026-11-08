"""Keep the store's audit log in step with the models that declare ``Audited``.

One reconciliation, run as the schema's owner inside the deploy transaction
(after ``create_all`` or after the structural migrations): it prepares the
audit log when a table is audited, protects every declared table with its
current exclusions, stops auditing a table whose marker was removed or every
table when ``schema.audit.enabled`` is false, and asks the guard to cover the
next :data:`AUDIT_HORIZON_MONTHS` months with partitions. History is never
touched. Every call goes through
the guard; names travel as bound parameters and Postgres quotes them.
"""

from __future__ import annotations

from typing import Final

from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.model.audited import AuditedTables
from loom.core.repository.sqlalchemy.rls.integrity import AUDIT_SUPPORTED
from loom.core.schema_names import schema_identifier, sql_identifier

AUDIT_LOG: Final = "audit_log"
# Months ahead the guard keeps partitioned on every deploy and every run of
# ``loom schema audit ensure-partitions``; a year without either still writes.
AUDIT_HORIZON_MONTHS: Final = 12
PREPARE_AUDIT_LOG: Final = "SELECT prepare_audit_log()"
PROTECT_AUDITED: Final = (
    "SELECT protect_audited_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:excluded AS text[]))"
)
UNPROTECT_AUDITED: Final = "SELECT unprotect_audited_table(CAST(:rel AS regclass))"
REGISTERED_AUDITED: Final = (
    "SELECT c.relname, a.rel::text FROM audited_table a "
    "JOIN pg_class c ON c.oid = a.rel JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :schema"
)
PRESENT_TABLES: Final = (
    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p') "
    "AND c.relname = ANY(CAST(:names AS text[]))"
)
AUDIT_LOG_EXISTS: Final = "SELECT to_regclass(quote_ident(:schema) || '.audit_log') IS NOT NULL"
ENSURE_AUDIT_PARTITIONS: Final = "SELECT ensure_audit_partitions(CAST(:months AS int))"
# A scheduled run must not queue audited writes behind a long lock wait.
SCHEDULED_LOCK_TIMEOUT: Final = "SET LOCAL lock_timeout = '5s'"
# The one definition of an expired partition, on the database's clock in UTC (the
# partitions' bounds): its upper bound is at or before this UTC month's start
# minus the retention. ``verify`` lists
# them; ``drop-expired`` takes the oldest; the guard re-checks the same rule.
EXPIRED_AUDIT_PARTITIONS: Final = (
    "SELECT c.relname FROM audit_partition p JOIN pg_class c ON c.oid = p.rel "
    "CROSS JOIN config cfg WHERE cfg.retention_months IS NOT NULL "
    "AND p.upper_bound "
    "<= (date_trunc('month', now() AT TIME ZONE 'UTC') "
    "- make_interval(months => cfg.retention_months)) AT TIME ZONE 'UTC' "
    "ORDER BY p.upper_bound"
)
# EXPIRED_AUDIT_PARTITIONS with LIMIT 1, spelled out: SQL reaching the database is a literal.
OLDEST_EXPIRED_AUDIT_PARTITION: Final = (
    "SELECT c.relname FROM audit_partition p JOIN pg_class c ON c.oid = p.rel "
    "CROSS JOIN config cfg WHERE cfg.retention_months IS NOT NULL "
    "AND p.upper_bound "
    "<= (date_trunc('month', now() AT TIME ZONE 'UTC') "
    "- make_interval(months => cfg.retention_months)) AT TIME ZONE 'UTC' "
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
_PRESENT_TABLES: Final = text(PRESENT_TABLES)
_AUDIT_LOG_EXISTS: Final = text(AUDIT_LOG_EXISTS)
_ENSURE_AUDIT_PARTITIONS: Final = text(ENSURE_AUDIT_PARTITIONS)
_SCHEDULED_LOCK_TIMEOUT: Final = text(SCHEDULED_LOCK_TIMEOUT)
_OLDEST_EXPIRED_AUDIT_PARTITION: Final = text(OLDEST_EXPIRED_AUDIT_PARTITION)
_DROP_EXPIRED_AUDIT_PARTITION: Final = text(DROP_EXPIRED_AUDIT_PARTITION)


def reconcile_audited(
    connection: Connection,
    *,
    schema: str,
    guard: str,
    declared: AuditedTables,
) -> None:
    """Bring the guard's audited tables and the log's partitions in line with *declared*.

    The caller's connection acts as the schema's owner with the guard first on
    its path; everything runs in the caller's transaction, so the guard's
    assertion at commit sees the final state. On a guard that predates the
    audit log and with nothing to audit, nothing is done.

    Args:
        connection: The migrator's connection, inside the deploy transaction.
        schema: The application schema.
        guard: The guard schema, to tell whether it has the audit log's revision.
        declared: The tables that record rows (``Application.active_audited``);
            empty stops auditing every registered table.
    """
    schema = schema_identifier(schema)
    if not declared and not connection.execute(_AUDIT_SUPPORTED, {"guard": guard}).scalar():
        return
    if declared:
        connection.execute(_PREPARE_AUDIT_LOG)
    registered = _registered(connection, schema)
    for (_, name), table in sorted(declared.items(), key=lambda item: item[0][1]):
        connection.execute(
            _PROTECT_AUDITED,
            {"schema": schema, "table": sql_identifier(name), "excluded": sorted(table.excluded)},
        )
    for name in sorted(set(registered) - {name for _, name in declared}):
        connection.execute(_UNPROTECT_AUDITED, {"rel": registered[name]})
    if connection.execute(_AUDIT_LOG_EXISTS, {"schema": schema}).scalar():
        connection.execute(_ENSURE_AUDIT_PARTITIONS, {"months": AUDIT_HORIZON_MONTHS})


def present_audited(connection: Connection, schema: str, declared: AuditedTables) -> AuditedTables:
    """The *declared* tables that exist in *schema* now.

    After one structural revision, a declared table a later revision creates
    is not there yet; reconciling only the present ones lets every revision
    audit the tables it created before it commits.
    """
    if not declared:
        return {}
    names = sorted({name for _, name in declared})
    rows = connection.execute(_PRESENT_TABLES, {"schema": schema, "names": names})
    present = {str(row.relname) for row in rows}
    return {key: table for key, table in declared.items() if key[1] in present}


def _registered(connection: Connection, schema: str) -> dict[str, str]:
    """The guard's audited tables of *schema*: unqualified name to qualified relation."""
    rows = connection.execute(_REGISTERED_AUDITED, {"schema": schema})
    return {str(name): str(rel) for name, rel in rows}


async def ensure_audit_partitions(connection: AsyncConnection) -> int:
    """Cover the next :data:`AUDIT_HORIZON_MONTHS` months; return how many partitions were made.

    Idempotent and cheap when nothing is missing: the guard takes its own lock
    and touches the log only to create a missing month. Meant for a scheduler
    (EventBridge Scheduler, pg_cron) between deploys.

    Args:
        connection: The migrator's connection with the guard first on its path,
            inside the caller's transaction.
    """
    await connection.execute(_SCHEDULED_LOCK_TIMEOUT)
    created = await connection.execute(_ENSURE_AUDIT_PARTITIONS, {"months": AUDIT_HORIZON_MONTHS})
    return int(created.scalar_one())


async def drop_expired_audit_partition(connection: AsyncConnection) -> str | None:
    """Drop the oldest audit partition past the configured retention; return its name.

    One partition per call, so a wrong retention costs at most one month
    before someone notices (FR-017). ``None`` when nothing is expired or no
    retention is configured. The guard re-checks the retention and refuses a
    partition within it, whatever this query found.

    Args:
        connection: The migrator's connection with the guard first on its path,
            inside the caller's transaction.
    """
    oldest = (await connection.execute(_OLDEST_EXPIRED_AUDIT_PARTITION)).scalar()
    if oldest is None:
        return None
    await connection.execute(_DROP_EXPIRED_AUDIT_PARTITION, {"partition": oldest})
    return str(oldest)
