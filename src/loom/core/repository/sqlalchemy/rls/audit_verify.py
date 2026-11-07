"""``verify``'s checks of the audit log, read as the database holds it.

The guard audits exactly the model's tables with the model's exclusions;
this month's and the next month's partitions exist on the database's clock;
the log and its partitions grant SELECT to the auditor alone; and its shape
is the pinned one. Partitions past the retention are listed, not drift; they
are the same ones ``loom schema audit drop-expired`` would drop.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.repository.sqlalchemy.rls.audit import EXPIRED_AUDIT_PARTITIONS
from loom.core.repository.sqlalchemy.rls.findings import Acl, Finding, diff
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import (
    AUDIT_SHAPE,
    AUDIT_SUPPORTED,
    audit_shape_problems,
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
# Whether a partition covers now, and the start of next month, on the database's clock.
AUDIT_COVERAGE: Final = (
    "WITH m AS (SELECT now() AS this_month, "
    "date_trunc('month', now()) + interval '1 month' AS next_month) "
    "SELECT coalesce(bool_or(m.this_month >= p.lower_bound "
    "AND m.this_month < p.upper_bound), false) AS this_month, "
    "coalesce(bool_or(m.next_month >= p.lower_bound "
    "AND m.next_month < p.upper_bound), false) AS next_month "
    "FROM m LEFT JOIN audit_partition p ON true"
)
_AUDIT_RELATIONS: Final = text(AUDIT_RELATIONS)
_AUDIT_REGISTRY: Final = text(AUDIT_REGISTRY)
_AUDIT_COVERAGE: Final = text(AUDIT_COVERAGE)
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
    covers_this_month: bool = False
    covers_next_month: bool = False
    expired: tuple[str, ...] = ()


async def audit_state(connection: AsyncConnection, schema: str, guard: str) -> AuditState:
    """Read the audit log's relations, registry, partition coverage and expired partitions."""
    if not (await connection.execute(_AUDIT_SUPPORTED, {"guard": guard})).scalar():
        return AuditState()
    in_schema = {"schema": schema}
    relations = list(await connection.execute(_AUDIT_RELATIONS, in_schema))
    registry = await connection.execute(_AUDIT_REGISTRY, in_schema)
    coverage = (await connection.execute(_AUDIT_COVERAGE)).one()
    expired = await connection.execute(_EXPIRED)
    return AuditState(
        supported=True,
        relations=frozenset(str(row.relname) for row in relations),
        sequences=frozenset(str(row.relname) for row in relations if row.relkind == "S"),
        registry={str(row.relname): frozenset(row.excluded or ()) for row in registry},
        covers_this_month=bool(coverage.this_month),
        covers_next_month=bool(coverage.next_month),
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
    """A finding per month, this one or the next, that no partition covers."""
    months = (("this month", state.covers_this_month), ("next month", state.covers_next_month))
    return [
        Finding(AUDIT_LOG, "audit.partition", f"a partition for {label}", "none")
        for label, covered in months
        if not covered
    ]


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
