"""``verify``'s checks of the audit log, read as the database holds it.

The guard audits exactly the model's tables with the model's exclusions;
this month's and the next month's partitions exist on the database's clock;
the log and its partitions grant SELECT to the auditor alone; and its shape
is the pinned one. Partitions past the retention are listed, not drift; they
are the same ones ``loom schema audit drop-expired`` would drop. The audit
reader is the released one and only the readers group may execute it, and
only while the store reads (FR-002, FR-003, SC-003).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.repository.sqlalchemy.rls.audit import EXPIRED_AUDIT_PARTITIONS
from loom.core.repository.sqlalchemy.rls.findings import Acl, Finding, diff
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import (
    AUDIT_HORIZON_WARNING_MONTHS,
    AUDIT_SHAPE,
    AUDIT_SUPPORTED,
    READ_AUDIT_LOG,
    audit_months_covered,
    audit_shape_problems,
    installed_reader,
    reader_shape_problems,
    reader_source_problems,
)

if TYPE_CHECKING:
    from loom.core.model.audited import AuditedTables
    from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig

AUDIT_LOG: Final = "audit_log"
AUDIT_RELATIONS: Final = (
    "SELECT c.relname, c.relkind::text AS relkind FROM pg_class c "
    "WHERE c.relkind IN ('r', 'p', 'S') "
    "AND (c.oid = to_regclass(quote_ident(:schema) || '.audit_log') "
    "OR c.oid IN (SELECT inhrelid FROM pg_inherits "
    "WHERE inhparent = to_regclass(quote_ident(:schema) || '.audit_log')) "
    "OR c.oid IN (SELECT d.objid FROM pg_depend d WHERE d.deptype IN ('a', 'i') "
    "AND d.refobjid = to_regclass(quote_ident(:schema) || '.audit_log')))"
)
AUDIT_REGISTRY: Final = (
    "SELECT c.relname, a.excluded FROM audited_table a JOIN pg_class c ON c.oid = a.rel "
    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :schema"
)
_AUDIT_RELATIONS: Final = text(AUDIT_RELATIONS)
_AUDIT_REGISTRY: Final = text(AUDIT_REGISTRY)
_EXPIRED: Final = text(EXPIRED_AUDIT_PARTITIONS)
_AUDIT_SUPPORTED: Final = text(AUDIT_SUPPORTED)
_AUDIT_SHAPE: Final = text(AUDIT_SHAPE)


@dataclass(frozen=True, slots=True)
class AuditState:
    """The audit log as the database holds it; empty on a guard without one."""

    supported: bool = False
    relations: frozenset[str] = frozenset()
    sequences: frozenset[str] = frozenset()
    registry: Mapping[str, frozenset[str]] = field(default_factory=dict)
    months_covered: int = 0
    expired: tuple[str, ...] = ()


async def audit_state(connection: AsyncConnection, schema: str, guard: str) -> AuditState:
    """Read the audit log's relations, registry, partition coverage and expired partitions."""
    if not (await connection.execute(_AUDIT_SUPPORTED, {"guard": guard})).scalar():
        return AuditState()
    in_schema = {"schema": schema}
    relations = list(await connection.execute(_AUDIT_RELATIONS, in_schema))
    registry = await connection.execute(_AUDIT_REGISTRY, in_schema)
    expired = await connection.execute(_EXPIRED)
    return AuditState(
        supported=True,
        relations=frozenset(str(row.relname) for row in relations),
        sequences=frozenset(str(row.relname) for row in relations if row.relkind == "S"),
        registry={str(row.relname): frozenset(row.excluded or ()) for row in registry},
        months_covered=await audit_months_covered(connection, schema) or 0,
        expired=tuple(str(row.relname) for row in expired),
    )


def audit_findings(state: AuditState, active: AuditedTables) -> list[Finding]:
    """The audited tables and exclusions are the model's, and the log can take its rows."""
    declared = {table.name: table.excluded for table in active.values()}
    if not state.supported:
        return _older_guard(declared)
    findings = _registry_findings(state.registry, declared)
    if state.relations or declared:
        findings += _missing_partitions(state)
    return findings


def _older_guard(declared: Mapping[str, frozenset[str]]) -> list[Finding]:
    """On a guard without the audit log, a finding only when tables must be audited."""
    if not declared:
        return []
    return [
        Finding(None, "audit.guard_revision", f"revision {AUDIT_GUARD_REVISION} or later", "older")
    ]


def _registry_findings(
    registry: Mapping[str, frozenset[str]], declared: Mapping[str, frozenset[str]]
) -> list[Finding]:
    """The guard audits exactly the declared tables, each leaving out the declared columns."""
    findings: list[Finding] = []
    if set(registry) != set(declared):
        findings.append(
            diff(None, "audit.registry", "audited tables", set(declared), set(registry))
        )
    findings += [
        diff(name, "audit.excluded", name, declared[name], registry[name])
        for name in sorted(set(registry) & set(declared))
        if registry[name] != declared[name]
    ]
    return findings


def _missing_partitions(state: AuditState) -> list[Finding]:
    """A finding when this month or the next has no partition, or the horizon is short.

    Partitions are created :data:`AUDIT_HORIZON_MONTHS` months ahead on every
    deploy and every ``loom schema audit ensure-partitions``; fewer than
    :data:`AUDIT_HORIZON_WARNING_MONTHS` covered months means neither has run
    for most of a year, and the pipeline should fail before writes do.
    """
    covered = state.months_covered
    if covered == 0:
        return [Finding(AUDIT_LOG, "audit.partition", "a partition for this month", "none")]
    if covered == 1:
        return [Finding(AUDIT_LOG, "audit.partition", "a partition for next month", "none")]
    if covered < AUDIT_HORIZON_WARNING_MONTHS:
        return [
            Finding(
                AUDIT_LOG,
                "audit.horizon",
                f"at least {AUDIT_HORIZON_WARNING_MONTHS} months of partitions",
                f"{covered} months",
            )
        ]
    return []


async def audit_shape_findings(connection: AsyncConnection, schema: str) -> list[Finding]:
    """The audit log's columns, constraints and indexes are the pinned ones."""
    shape = await connection.execute(_AUDIT_SHAPE, {"schema": schema})
    lines = [str(row.line) for row in shape]
    return [
        Finding(
            AUDIT_LOG,
            problem.check,
            f"{problem.subject}: {problem.expected}",
            f"{problem.subject}: {problem.actual}",
        )
        for problem in audit_shape_problems(lines)
    ]


def audit_privilege_findings(
    acl: Acl, state: AuditState, bootstrap: BootstrapConfig
) -> list[Finding]:
    """The log and its partitions grant SELECT to the auditor alone; its sequence grants nothing."""
    reader = {f"{bootstrap.auditor_role}:SELECT"}
    findings: list[Finding] = []
    for relname in sorted(state.relations):
        wanted = set() if relname in state.sequences else reader
        held = {
            f"{grantee}:{privilege}"
            for grantee, privileges in acl.get(relname, {}).items()
            for privilege in privileges
        }
        if held != wanted:
            findings.append(diff(relname, "audit.privileges", relname, wanted, held))
    return findings


async def audit_reader_findings(
    connection: AsyncConnection, bootstrap: BootstrapConfig
) -> list[Finding]:
    """Read the audit reader and the released source, then compare; see :func:`reader_findings`.

    The released source exists from the reader's guard revision on; below it
    there is none, so any reader found there is drift.
    """
    rows, source = await installed_reader(connection, bootstrap.names.guard, bootstrap.schema)
    return reader_findings(rows, source, bootstrap)


def reader_findings(
    rows: Sequence[Any], source: str | None, bootstrap: BootstrapConfig
) -> list[Finding]:
    """The reader is installed while reading, released as is, and executable by readers alone.

    *rows* are the readers ``AUDIT_READER`` reads and *source* the body
    the guard's ``audit_reader_source()`` releases. EXECUTE goes to the readers group,
    without grant option, while ``bootstrap.audit_reading``; to nobody else.
    """
    if not rows:
        missing = Finding(READ_AUDIT_LOG, "audit.reader", "installed", "missing")
        return [missing] if bootstrap.audit_reading else []
    findings = [
        Finding(READ_AUDIT_LOG, problem.check, problem.expected, problem.actual)
        for problem in reader_shape_problems(rows) + reader_source_problems(rows, source)
    ]
    return findings + _reader_grant_findings(rows, bootstrap)


def _reader_grant_findings(rows: Sequence[Any], bootstrap: BootstrapConfig) -> list[Finding]:
    """EXECUTE on the reader goes to the readers group alone, and only while reading."""
    wanted = {f"{bootstrap.names.readers}:EXECUTE"} if bootstrap.audit_reading else set()
    held = {str(grant) for row in rows for grant in row.grants}
    if held == wanted:
        return []
    return [diff(READ_AUDIT_LOG, "audit.reader_grants", READ_AUDIT_LOG, wanted, held)]
