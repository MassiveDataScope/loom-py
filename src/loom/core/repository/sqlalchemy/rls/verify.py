"""Verify what the guard's assertion cannot require: presence, not only absence.

The assertion forbids excess at every DDL; ``verify`` additionally requires
that every group, bypass user and global table holds exactly what the model
and the declaration say, that memberships match each user's access and no
undeclared role is a member of a group, that every scoped model table is
registered and protected canonically, and that the guard installed in the
database is one this release of loom shipped: same catalogue, owners, grants
and settings, and the configuration the declaration describes.

The audit log, when it exists, must be readable by the auditor role alone,
its audited tables and exclusions must be the model's, this month's and the
next month's partitions must exist, and only declared operators may be
members of the auditor role. Partitions past the retention are listed in
:attr:`Report.expired_audit_partitions`; they are not drift.
"""

from __future__ import annotations

import datetime as dt
import json
import operator
from collections import defaultdict
from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import ASSERT_SCHEMA, GUARD_FIRST
from loom.core.model.introspection import declared_privileges, get_table_name, is_row_scoped
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.scoped import ScopedTable, registered_model_tables
from loom.core.repository.sqlalchemy.rls.config import BYPASS_VERSION_PRIVILEGES, BootstrapConfig
from loom.core.repository.sqlalchemy.rls.integrity import (
    AUDIT_SHAPE,
    Problem,
    audit_shape_problems,
    guard_problems,
    table_problems,
)

if TYPE_CHECKING:
    from loom.core.locator import Application

Acl = dict[str, dict[str, set[str]]]
WRITES: Final = frozenset({Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE})
USAGE: Final = frozenset({"USAGE"})

RELATION_ACL: Final = (
    "SELECT c.relname, c.relkind::text AS relkind, coalesce(r.rolname, 'PUBLIC') AS grantee, "
    "a.privilege_type "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
    "LATERAL aclexplode(coalesce(c.relacl, "
    "acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::\"char\", c.relowner))) a "
    "LEFT JOIN pg_roles r ON r.oid = a.grantee "
    "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p', 'S') AND a.grantee <> c.relowner"
)
SERIALS: Final = (
    "SELECT t.relname AS table_name, s.relname AS sequence_name "
    "FROM pg_depend d JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' "
    "JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace n ON n.oid = t.relnamespace "
    "WHERE n.nspname = :schema AND d.deptype = 'a'"
)
FOREIGN_KEYS: Final = (
    "SELECT c.relname AS child, p.relname AS parent, "
    "con.confdeltype::text AS on_delete, con.confupdtype::text AS on_update "
    "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
    "JOIN pg_class p ON p.oid = con.confrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE con.contype = 'f' AND n.nspname = :schema"
)
GUARD_CONFIG: Final = (
    "SELECT app_schema, owner_role, migrator_role, readers_role, writers_role, version_table, "
    "data_version_table, installer, users::text AS users, "
    "to_jsonb(config) -> 'auditor_role' AS auditor_role, "
    "to_jsonb(config) -> 'retention_months' AS retention_months FROM config"
)
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
AUDIT_PARTITIONS: Final = (
    "SELECT c.relname, p.lower_bound, p.upper_bound FROM audit_partition p "
    "JOIN pg_class c ON c.oid = p.rel ORDER BY p.lower_bound"
)
AUDIT_SUPPORTED: Final = "SELECT to_regclass('audited_table') IS NOT NULL"
REGISTERED: Final = (
    "SELECT c.relname, p.relname AS parent, "
    "t.scopes = pt.scopes AND t.privileges = pt.privileges AS as_parent "
    "FROM scoped_table t JOIN pg_class c ON c.oid = t.rel "
    "LEFT JOIN pg_inherits i ON i.inhrelid = c.oid LEFT JOIN pg_class p ON p.oid = i.inhparent "
    "LEFT JOIN scoped_table pt ON pt.rel = p.oid"
)
MEMBERS: Final = (
    "SELECT m.rolname AS member, r.rolname AS role, r.rolbypassrls, m.rolsuper, "
    # pg_auth_members.inherit_option exists from Postgres 16; read through
    # to_jsonb so the query parses on 14 and 15, where rolinherit decides.
    "COALESCE((to_jsonb(am) ->> 'inherit_option')::boolean, m.rolinherit) AS inherits "
    "FROM pg_auth_members am JOIN pg_roles m ON m.oid = am.member "
    "JOIN pg_roles r ON r.oid = am.roleid"
)
_RELATION_ACL: Final = text(RELATION_ACL)
_SERIALS: Final = text(SERIALS)
_FOREIGN_KEYS: Final = text(FOREIGN_KEYS)
_GUARD_CONFIG: Final = text(GUARD_CONFIG)
_REGISTERED: Final = text(REGISTERED)
_MEMBERS: Final = text(MEMBERS)
_AUDIT_RELATIONS: Final = text(AUDIT_RELATIONS)
_AUDIT_REGISTRY: Final = text(AUDIT_REGISTRY)
_AUDIT_PARTITIONS: Final = text(AUDIT_PARTITIONS)
_AUDIT_SUPPORTED: Final = text(AUDIT_SUPPORTED)
_AUDIT_SHAPE: Final = text(AUDIT_SHAPE)
_GUARD_FIRST: Final = text(GUARD_FIRST)
_ASSERT_SCHEMA: Final = text(ASSERT_SCHEMA)


@dataclass(frozen=True, slots=True)
class Finding:
    """One difference between the declaration and the database."""

    table: str | None
    check: str
    expected: str
    actual: str


@dataclass(frozen=True, slots=True)
class Report:
    """The outcome of ``verify``; ``ok`` when nothing differs.

    ``expired_audit_partitions`` names the audit log's partitions past the
    configured retention, which ``loom schema audit drop-expired`` may drop.
    """

    ok: bool
    findings: tuple[Finding, ...]
    expired_audit_partitions: tuple[str, ...] = ()


async def verify(url: str, application: Application) -> Report:
    """Compare the database with the application's declaration."""
    bootstrap = application.bootstrap
    if bootstrap is None:
        return Report(ok=True, findings=())
    guard = {"guard": bootstrap.names.guard}
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(_GUARD_FIRST, guard)
            findings = await _guard_integrity(connection, bootstrap)
            audit = await _audit_state(connection, bootstrap.schema)
            findings += _audit_findings(audit, application, bootstrap)
            if audit.relations or application.audited:
                shape = await connection.execute(_AUDIT_SHAPE, _in_schema(bootstrap.schema))
                findings += [
                    _finding(problem, "audit_log")
                    for problem in audit_shape_problems([str(row.line) for row in shape])
                ]
            findings += await _registry(connection, application.scoped)
            findings += await _protection(connection, application.scoped, bootstrap)
            findings += await _assertion(connection)
            await connection.execute(_GUARD_FIRST, guard)
            acl, sequences = await _acl(connection, bootstrap.schema)
            findings += _group_privileges(acl, application.scoped, bootstrap)
            findings += await _sequence_usage(connection, acl, application.scoped, bootstrap)
            findings += _audit_privileges(acl, audit, bootstrap)
            findings += _bypass_privileges(acl, sequences, bootstrap, audit.relations)
            findings += _global_privileges(acl, application, bootstrap)
            findings += await _global_parent_findings(
                connection, acl, application.scoped, bootstrap
            )
            findings += await _memberships(connection, bootstrap)
    finally:
        await engine.dispose()
    return Report(ok=not findings, findings=tuple(findings), expired_audit_partitions=audit.expired)


def _finding(problem: Problem, table: str | None = None) -> Finding:
    return Finding(
        table,
        problem.check,
        f"{problem.subject}: {problem.expected}",
        f"{problem.subject}: {problem.actual}",
    )


async def _guard_integrity(
    connection: AsyncConnection, bootstrap: BootstrapConfig
) -> list[Finding]:
    row = (await connection.execute(_GUARD_CONFIG)).one_or_none()
    installer = None if row is None else str(row.installer)
    problems = await guard_problems(
        connection, bootstrap.names.guard, bootstrap.roles.owner, installer
    )
    return [_finding(problem) for problem in problems] + _config_findings(row, bootstrap)


def _config_findings(row: Any, bootstrap: BootstrapConfig) -> list[Finding]:
    names = bootstrap.names
    expected = (
        bootstrap.schema,
        bootstrap.roles.owner,
        bootstrap.roles.migrator,
        names.readers,
        names.writers,
        names.version_table,
        names.data_version_table,
        _users(bootstrap.document()["users"]),
    )
    actual = None if row is None else (*row[:7], _users(json.loads(row.users)))
    findings: list[Finding] = []
    if actual != expected:
        findings.append(Finding(None, "guard.config", str(expected), str(actual)))
    audit = (bootstrap.auditor_role, bootstrap.audit_retention_months)
    held = None if row is None else (row.auditor_role, row.retention_months)
    if row is not None and held != audit:
        findings.append(Finding(None, "guard.audit_config", str(audit), str(held)))
    return findings


def _users(users: Any) -> list[tuple[str, bool, str]]:
    return sorted((str(u["name"]), bool(u["login"]), str(u["access"])) for u in users)


async def _registry(
    connection: AsyncConnection, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> list[Finding]:
    """Every scoped model table is registered, and nothing else but their partitions.

    A partition is not in the model: it counts when its parent is a
    partitioned scoped table of the model, and must carry its parent's scopes
    and privileges.
    """
    rows = list(await connection.execute(_REGISTERED))
    parents = {str(row.relname): None if row.parent is None else str(row.parent) for row in rows}
    registered = registered_model_tables(parents, scoped.values())
    findings = [
        Finding(str(row.relname), "partition.registration", f"scopes of {row.parent}", "different")
        for row in rows
        if str(row.relname) not in registered and not row.as_parent
    ]
    expected = {table.name for table in scoped.values()}
    if registered != expected:
        findings.append(_diff(None, "registry", "registered tables", expected, registered))
    return findings


async def _protection(
    connection: AsyncConnection,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    bootstrap: BootstrapConfig,
) -> list[Finding]:
    problems = await table_problems(connection, bootstrap.names.guard, bootstrap.schema, scoped)
    return [_finding(problem, problem.subject) for problem in problems]


async def _assertion(connection: AsyncConnection) -> list[Finding]:
    try:
        await connection.execute(_ASSERT_SCHEMA)
    except DBAPIError as exc:
        await connection.rollback()
        return [Finding(None, "assertion", "passes", str(exc.orig))]
    return []


async def _acl(connection: AsyncConnection, schema: str) -> tuple[Acl, frozenset[str]]:
    acl: Acl = defaultdict(lambda: defaultdict(set))
    sequences: set[str] = set()
    rows = await connection.execute(_RELATION_ACL, _in_schema(schema))
    for row in rows:
        acl[str(row.relname)][str(row.grantee)].add(str(row.privilege_type))
        if row.relkind == "S":
            sequences.add(str(row.relname))
    return acl, frozenset(sequences)


def _group_privileges(
    acl: Acl, scoped: Mapping[tuple[str | None, str], ScopedTable], bootstrap: BootstrapConfig
) -> list[Finding]:
    names = bootstrap.names
    findings: list[Finding] = []
    for table in scoped.values():
        expected = {
            names.readers: {p.value for p in table.privileges if p is Privilege.SELECT},
            names.writers: {p.value for p in table.privileges if p in WRITES},
        }
        for group, wanted in expected.items():
            actual = acl[table.name].get(group, set())
            if actual != wanted:
                findings.append(_diff(table.name, "group.privileges", group, wanted, actual))
    return findings


async def _sequence_usage(
    connection: AsyncConnection,
    acl: Acl,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    bootstrap: BootstrapConfig,
) -> list[Finding]:
    inserting = {t.name for t in scoped.values() if Privilege.INSERT in t.privileges}
    writers = bootstrap.names.writers
    findings: list[Finding] = []
    for row in await connection.execute(_SERIALS, _in_schema(bootstrap.schema)):
        table_name, sequence = str(row.table_name), str(row.sequence_name)
        if table_name not in inserting:
            continue
        actual = acl[sequence].get(writers, set())
        if actual != USAGE:
            findings.append(_diff(table_name, "sequence.usage", writers, USAGE, actual))
    return findings


def _bypass_privileges(
    acl: Acl,
    sequences: frozenset[str],
    bootstrap: BootstrapConfig,
    audit_relations: frozenset[str] = frozenset(),
) -> list[Finding]:
    findings: list[Finding] = []
    for relname, grants in acl.items():
        if relname in audit_relations:
            continue
        check, wanted, enough = _bypass_expectation(relname, sequences, bootstrap)
        for user in bootstrap.bypass_users:
            actual = grants.get(user, set())
            if not enough(wanted, actual):
                findings.append(_diff(relname, check, user, wanted, actual))
    return findings


def _bypass_expectation(
    relname: str, sequences: frozenset[str], bootstrap: BootstrapConfig
) -> tuple[str, set[str], Callable[[set[str], set[str]], bool]]:
    """The check a bypass user's grants on *relname* answer, what it wants, and how to compare.

    The version table wants exactly its privileges; a sequence and any other
    relation want at least theirs.
    """
    if relname == bootstrap.names.version_table:
        return "bypass.version_table", set(BYPASS_VERSION_PRIVILEGES), operator.eq
    if relname in sequences:
        return "bypass.sequence_usage", set(USAGE), operator.le
    return "bypass.privileges", {p.value for p in READ_WRITE}, operator.le


def _global_privileges(
    acl: Acl, application: Application, bootstrap: BootstrapConfig
) -> list[Finding]:
    groups = {"readers": bootstrap.names.readers, "writers": bootstrap.names.writers}
    findings: list[Finding] = []
    for model in application.models:
        if is_row_scoped(model):
            continue
        table = get_table_name(model)
        declared = declared_privileges(model)
        for group, role in groups.items():
            wanted = {p.value for p in declared.get(group, frozenset())}
            actual = acl[table].get(role, set())
            if actual != wanted:
                findings.append(_diff(table, "global.privileges", role, wanted, actual))
    return findings


async def _global_parent_findings(
    connection: AsyncConnection,
    acl: Acl,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    bootstrap: BootstrapConfig,
) -> list[Finding]:
    scoped_names = {t.name for t in scoped.values()}
    groups = (bootstrap.names.readers, bootstrap.names.writers)
    findings: list[Finding] = []
    for row in await connection.execute(_FOREIGN_KEYS, _in_schema(bootstrap.schema)):
        child, parent = str(row.child), str(row.parent)
        if child in scoped_names and parent not in scoped_names:
            findings.extend(_action_findings(child, (str(row.on_delete), str(row.on_update))))
            findings.extend(_parent_write_findings(parent, acl, groups))
    return findings


def _action_findings(child: str, actions: tuple[str, str]) -> list[Finding]:
    return [
        Finding(child, "c9.action", "RESTRICT or NO ACTION", action)
        for action in actions
        if action not in ("r", "a")
    ]


def _parent_write_findings(parent: str, acl: Acl, groups: tuple[str, str]) -> list[Finding]:
    return [
        _diff(parent, "c9.group_write", group, set(), held)
        for group in groups
        if (held := acl[parent].get(group, set()) & {"UPDATE", "DELETE"})
    ]


async def _memberships(connection: AsyncConnection, bootstrap: BootstrapConfig) -> list[Finding]:
    rows = list(await connection.execute(_MEMBERS))
    memberships: dict[str, set[str]] = defaultdict(set)
    bypass_roles = {str(row.role) for row in rows if row.rolbypassrls}
    inherits: dict[tuple[str, str], bool] = {}
    for row in rows:
        member, role = str(row.member), str(row.role)
        memberships[member].add(role)
        inherits[(member, role)] = bool(row.inherits)
    findings = _access_memberships(memberships, bootstrap)
    findings += _undeclared_members(rows, bootstrap)
    owner, migrator = bootstrap.roles.owner, bootstrap.roles.migrator
    if inherits.get((migrator, owner), False):
        findings.append(
            Finding(
                None, "migrator.inherit", f"{migrator} inherits nothing from {owner}", "inherits"
            )
        )
    for role in (owner, migrator):
        held = memberships[role] & bypass_roles
        if held:
            findings.append(_diff(None, "bypass.membership", role, set(), held))
    auditor = bootstrap.auditor_role
    forbidden = {owner, migrator, *bootstrap.database_users}
    auditors = {member for member, roles in memberships.items() if auditor in roles}
    if auditors & forbidden:
        findings.append(_diff(None, "auditor.membership", auditor, set(), auditors & forbidden))
    return findings


def _access_memberships(
    memberships: Mapping[str, set[str]], bootstrap: BootstrapConfig
) -> list[Finding]:
    readers, writers = bootstrap.names.readers, bootstrap.names.writers
    by_access = {"read": {readers}, "write": {readers, writers}, "bypass": set()}
    findings: list[Finding] = []
    for user, spec in bootstrap.database_users.items():
        wanted = by_access[spec.access]
        actual = memberships[user] & {readers, writers}
        if actual != wanted:
            findings.append(_diff(None, "membership.access", user, wanted, actual))
    return findings


def _undeclared_members(rows: list[Any], bootstrap: BootstrapConfig) -> list[Finding]:
    groups = {bootstrap.names.readers, bootstrap.names.writers}
    declared = {user for user, spec in bootstrap.database_users.items() if spec.access != "bypass"}
    undeclared = {
        f"{row.member} in {row.role}"
        for row in rows
        if str(row.role) in groups and str(row.member) not in declared and not row.rolsuper
    }
    if undeclared:
        return [_diff(None, "membership.undeclared", "group members", set(), undeclared)]
    return []


def _in_schema(schema: str) -> dict[str, str]:
    return {"schema": schema}


def _diff(
    table: str | None, check: str, subject: str, wanted: AbstractSet[str], actual: AbstractSet[str]
) -> Finding:
    return Finding(table, check, f"{subject}: {sorted(wanted)}", f"{subject}: {sorted(actual)}")


@dataclass(frozen=True, slots=True)
class _AuditState:
    """The audit log as the database holds it; empty on a guard without one."""

    supported: bool = False
    relations: frozenset[str] = frozenset()
    sequences: frozenset[str] = frozenset()
    registry: Mapping[str, frozenset[str]] = field(default_factory=dict)
    partitions: tuple[tuple[str, dt.datetime, dt.datetime], ...] = ()
    retention: int | None = None
    expired: tuple[str, ...] = ()


async def _audit_state(connection: AsyncConnection, schema: str) -> _AuditState:
    if not (await connection.execute(_AUDIT_SUPPORTED)).scalar():
        return _AuditState()
    rows = list(await connection.execute(_AUDIT_RELATIONS, _in_schema(schema)))
    relations = frozenset(str(row.relname) for row in rows)
    sequences = frozenset(str(row.relname) for row in rows if row.relkind == "S")
    registry = {
        str(row.relname): frozenset(row.excluded or ())
        for row in await connection.execute(_AUDIT_REGISTRY, _in_schema(schema))
    }
    partitions = tuple(
        (str(row.relname), row.lower_bound, row.upper_bound)
        for row in await connection.execute(_AUDIT_PARTITIONS)
    )
    config = (await connection.execute(_GUARD_CONFIG)).one_or_none()
    retention = (
        None if config is None or config.retention_months is None else int(config.retention_months)
    )
    return _AuditState(
        True, relations, sequences, registry, partitions, retention, _expired(partitions, retention)
    )


def _expired(
    partitions: tuple[tuple[str, dt.datetime, dt.datetime], ...], retention: int | None
) -> tuple[str, ...]:
    """The partitions whose upper bound is older than the retention, oldest first."""
    if retention is None:
        return ()
    month = dt.datetime.now(dt.UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months = month.year * 12 + month.month - 1 - retention
    cutoff = month.replace(year=months // 12, month=months % 12 + 1)
    return tuple(name for name, _, upper in partitions if upper <= cutoff)


def _audit_findings(
    audit: _AuditState, application: Application, bootstrap: BootstrapConfig
) -> list[Finding]:
    """The audited tables and exclusions are the model's, and the log can take this month's rows."""
    enabled = application.database.schema.audit.enabled
    declared = (
        {table.name: table.excluded for table in application.audited.values()} if enabled else {}
    )
    if not audit.supported:
        if declared:
            return [Finding(None, "audit.guard_revision", "revision 3 or later", "older")]
        return []
    findings: list[Finding] = []
    if set(audit.registry) != set(declared):
        findings.append(
            _diff(None, "audit.registry", "audited tables", set(declared), set(audit.registry))
        )
    for name in sorted(set(audit.registry) & set(declared)):
        if audit.registry[name] != declared[name]:
            findings.append(
                _diff(name, "audit.excluded", name, declared[name], audit.registry[name])
            )
    if audit.relations or declared:
        findings += _missing_partitions(audit.partitions)
    return findings


def _missing_partitions(
    partitions: tuple[tuple[str, dt.datetime, dt.datetime], ...],
) -> list[Finding]:
    now = dt.datetime.now(dt.UTC)
    month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    following = (month + dt.timedelta(days=32)).replace(day=1)
    findings: list[Finding] = []
    for moment, label in ((now, "this month"), (following, "next month")):
        if not any(lower <= moment < upper for _, lower, upper in partitions):
            findings.append(
                Finding("audit_log", "audit.partition", f"a partition for {label}", "none")
            )
    return findings


def _audit_privileges(acl: Acl, audit: _AuditState, bootstrap: BootstrapConfig) -> list[Finding]:
    """The log and its partitions grant SELECT to the auditor alone; its sequence grants nothing."""
    findings: list[Finding] = []
    for relname in sorted(audit.relations):
        held = {
            f"{grantee}:{p}"
            for grantee, privileges in acl.get(relname, {}).items()
            for p in privileges
        }
        wanted = set() if relname in audit.sequences else {f"{bootstrap.auditor_role}:SELECT"}
        if held != wanted:
            findings.append(_diff(relname, "audit.privileges", relname, wanted, held))
    return findings
