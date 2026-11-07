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
from dataclasses import dataclass
from typing import Final

from sqlalchemy import Connection, text

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
_PREPARE_AUDIT_LOG: Final = text(PREPARE_AUDIT_LOG)
_PROTECT_AUDITED: Final = text(PROTECT_AUDITED)
_UNPROTECT_AUDITED: Final = text(UNPROTECT_AUDITED)
_REGISTERED_AUDITED: Final = text(REGISTERED_AUDITED)
_AUDIT_SUPPORTED: Final = text(AUDIT_SUPPORTED)
_AUDIT_LOG_EXISTS: Final = text(AUDIT_LOG_EXISTS)
_CREATE_AUDIT_PARTITION: Final = text(CREATE_AUDIT_PARTITION)


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
    enabled: bool,
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
        declared: The audited tables compiled from the models.
        enabled: ``schema.audit.enabled``; false stops auditing every table.
        today: The day partitions are created around; today when absent.
    """
    schema = schema_identifier(schema)
    wanted = {key: table for key, table in declared.items() if enabled}
    if not wanted and not connection.execute(_AUDIT_SUPPORTED).scalar():
        return AuditReconciliation()
    if wanted:
        connection.execute(_PREPARE_AUDIT_LOG)
    registered = {
        str(name): str(rel)
        for owner, name, rel in connection.execute(_REGISTERED_AUDITED)
        if owner == schema
    }
    protected: list[str] = []
    for (_, name), table in sorted(wanted.items(), key=lambda item: item[0][1]):
        connection.execute(
            _PROTECT_AUDITED,
            {"schema": schema, "table": sql_identifier(name), "excluded": sorted(table.excluded)},
        )
        if name not in registered:
            protected.append(name)
    names = {name for _, name in wanted}
    unprotected = sorted(name for name in registered if name not in names)
    for name in unprotected:
        connection.execute(_UNPROTECT_AUDITED, {"rel": registered[name]})
    partitions: tuple[str, ...] = ()
    if connection.execute(_AUDIT_LOG_EXISTS, {"schema": schema}).scalar():
        partitions = ensure_audit_partitions(connection, today or dt.date.today())
    return AuditReconciliation(tuple(protected), tuple(unprotected), partitions)


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
