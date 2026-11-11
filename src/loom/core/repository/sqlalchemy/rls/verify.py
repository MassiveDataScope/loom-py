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
members of the auditor role. No view, rule or foreign trigger may read the
log, nor a security definer function of the owner that an application role may
execute. Partitions past the retention are listed in
:attr:`Report.expired_audit_partitions`, and the owner's other security definer
functions in :attr:`Report.warnings`; neither is drift.
"""

from __future__ import annotations

import json
import operator
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import ASSERT_SCHEMA, GUARD_FIRST
from loom.core.model.introspection import declared_privileges, get_table_name, is_row_scoped
from loom.core.model.materialization import materialized_models
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.scoped import ScopedTable, registered_model_tables
from loom.core.repository.sqlalchemy.rls.audit_verify import (
    audit_column_findings,
    audit_exposure_findings,
    audit_findings,
    audit_privilege_findings,
    audit_reader_findings,
    audit_shape_findings,
    audit_state,
    definer_checks,
)
from loom.core.repository.sqlalchemy.rls.config import BYPASS_VERSION_PRIVILEGES, BootstrapConfig
from loom.core.repository.sqlalchemy.rls.findings import Acl, Finding, problem_finding
from loom.core.repository.sqlalchemy.rls.findings import diff as _diff
from loom.core.repository.sqlalchemy.rls.integrity import guard_problems, table_problems

if TYPE_CHECKING:
    from loom.core.locator import Application

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
    "to_jsonb(config) -> 'retention_months' AS retention_months, "
    "(to_jsonb(config) ->> 'audit_reading')::boolean AS audit_reading, "
    "to_jsonb(config) -> 'boundary_scope' AS boundary_scope, "
    "to_jsonb(config) -> 'boundary_type' AS boundary_type FROM config"
)
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
_GUARD_FIRST: Final = text(GUARD_FIRST)
_ASSERT_SCHEMA: Final = text(ASSERT_SCHEMA)


@dataclass(frozen=True, slots=True)
class Report:
    """The outcome of ``verify``; ``ok`` when nothing differs.

    ``expired_audit_partitions`` names the audit log's partitions past the
    configured retention, which ``loom schema audit drop-expired`` may drop.
    ``warnings`` are advisory findings a person reviews; like expired
    partitions, they are listed and are not drift, so ``ok`` ignores them.
    """

    ok: bool
    findings: tuple[Finding, ...]
    expired_audit_partitions: tuple[str, ...] = ()
    warnings: tuple[Finding, ...] = ()

    @classmethod
    def of(
        cls,
        findings: Sequence[Finding],
        warnings: Sequence[Finding] = (),
        expired: Sequence[str] = (),
    ) -> Report:
        """The report of *findings*, ``ok`` without any; *warnings* never count."""
        return cls(
            ok=not findings,
            findings=tuple(findings),
            expired_audit_partitions=tuple(expired),
            warnings=tuple(warnings),
        )


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
            audit = await audit_state(connection, bootstrap.schema, bootstrap.names.guard)
            findings += audit_findings(audit, application.active_audited)
            if audit.relations or application.audited or bootstrap.audit_reading:
                findings += await audit_shape_findings(connection, bootstrap.schema)
            findings += await _registry(connection, application.scoped)
            findings += await _protection(connection, application.scoped, bootstrap)
            findings += await _assertion(connection)
            await connection.execute(_GUARD_FIRST, guard)
            acl, sequences = await _acl(connection, bootstrap.schema)
            findings += _group_privileges(acl, application.scoped, bootstrap)
            findings += await _sequence_usage(connection, acl, application.scoped, bootstrap)
            findings += audit_privilege_findings(acl, audit, bootstrap)
            findings += await audit_column_findings(connection, bootstrap.schema)
            findings += await audit_reader_findings(connection, bootstrap)
            findings += await audit_exposure_findings(
                connection, bootstrap.names.guard, bootstrap.schema
            )
            escalated, warnings = await definer_checks(connection, bootstrap)
            findings += escalated
            findings += _bypass_privileges(acl, sequences, bootstrap, audit.relations)
            findings += _global_privileges(acl, application, bootstrap)
            findings += await _global_parent_findings(
                connection, acl, application.scoped, bootstrap
            )
            findings += await _memberships(connection, bootstrap)
    finally:
        await engine.dispose()
    return Report.of(findings, warnings, audit.expired)


async def _guard_integrity(
    connection: AsyncConnection, bootstrap: BootstrapConfig
) -> list[Finding]:
    row = (await connection.execute(_GUARD_CONFIG)).one_or_none()
    installer = None if row is None else str(row.installer)
    problems = await guard_problems(
        connection, bootstrap.names.guard, bootstrap.roles.owner, installer
    )
    return [problem_finding(problem) for problem in problems] + _config_findings(row, bootstrap)


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
    return (
        findings + _audit_config_findings(row, bootstrap) + _audit_reading_findings(row, bootstrap)
    )


def _audit_config_findings(row: Any, bootstrap: BootstrapConfig) -> list[Finding]:
    """The guard's auditor role and retention are the declared ones; nothing without a row."""
    if row is None:
        return []
    audit = (bootstrap.auditor_role, bootstrap.audit_retention_months)
    held = (row.auditor_role, row.retention_months)
    if held == audit:
        return []
    return [Finding(None, "guard.audit_config", str(audit), str(held))]


def _audit_reading_findings(row: Any, bootstrap: BootstrapConfig) -> list[Finding]:
    """The recorded reading flag is the declared one, and so is ``B`` while reading.

    A guard before the reader's revision records no flag: it reads as off. A
    ``B`` kept while reading is off is not drift; the operator clears it.
    """
    if row is None:
        return []
    boundary = None if row.boundary_scope is None else (row.boundary_scope, row.boundary_type)
    checks: list[tuple[str, object, object]] = [
        ("audit_reading", bootstrap.audit_reading, row.audit_reading is True)
    ]
    if bootstrap.audit_reading:
        checks.append(("boundary", bootstrap.audit_boundary, boundary))
    return [
        Finding(None, "guard.audit_reading", f"{name} {wanted}", f"{name} {held}")
        for name, wanted, held in checks
        if held != wanted
    ]


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
    return [problem_finding(problem, problem.subject) for problem in problems]


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
    for model in materialized_models(application.models):
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
    inherits: dict[tuple[str, str], bool] = {}
    for row in rows:
        member, role = str(row.member), str(row.role)
        memberships[member].add(role)
        inherits[(member, role)] = bool(row.inherits)
    bypass_roles = {str(row.role) for row in rows if row.rolbypassrls}
    findings = _access_memberships(memberships, bootstrap)
    findings += _undeclared_members(rows, bootstrap)
    findings += _migrator_inherits(inherits, bootstrap)
    findings += _privileged_bypass(memberships, bypass_roles, bootstrap)
    return findings + _auditor_members(memberships, bootstrap)


def _migrator_inherits(
    inherits: Mapping[tuple[str, str], bool], bootstrap: BootstrapConfig
) -> list[Finding]:
    """The migrator acts as the schema's owner only through SET ROLE, never by inheritance."""
    owner, migrator = bootstrap.roles.owner, bootstrap.roles.migrator
    if not inherits.get((migrator, owner), False):
        return []
    return [
        Finding(None, "migrator.inherit", f"{migrator} inherits nothing from {owner}", "inherits")
    ]


def _privileged_bypass(
    memberships: Mapping[str, set[str]], bypass_roles: set[str], bootstrap: BootstrapConfig
) -> list[Finding]:
    """Neither the schema's owner nor the migrator belongs to a role that bypasses RLS."""
    return [
        _diff(None, "bypass.membership", role, set(), held)
        for role in (bootstrap.roles.owner, bootstrap.roles.migrator)
        if (held := memberships[role] & bypass_roles)
    ]


def _auditor_members(
    memberships: Mapping[str, set[str]], bootstrap: BootstrapConfig
) -> list[Finding]:
    """No login the application or the deploy uses can read the audit log."""
    auditor = bootstrap.auditor_role
    forbidden = {bootstrap.roles.owner, bootstrap.roles.migrator, *bootstrap.database_users}
    members = {member for member, roles in memberships.items() if auditor in roles} & forbidden
    if not members:
        return []
    return [_diff(None, "auditor.membership", auditor, set(), members)]


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
