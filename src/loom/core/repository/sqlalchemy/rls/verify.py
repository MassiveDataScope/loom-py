"""Verify what the guard's assertion cannot require: presence, not only absence.

The assertion forbids excess at every DDL; ``verify`` additionally requires
that every group, bypass user and global table holds exactly what the model
and the declaration say, that memberships match each user's access and no
undeclared role is a member of a group, that every scoped model table is
registered and protected canonically, and that the guard installed in the
database is one this release of loom shipped: same catalogue, owners, grants
and settings, and the configuration the declaration describes.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import ASSERT_SCHEMA, GUARD_FIRST
from loom.core.model.introspection import declared_privileges, get_table_name, is_row_scoped
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.config import BYPASS_VERSION_PRIVILEGES, BootstrapConfig
from loom.core.repository.sqlalchemy.rls.integrity import Problem, guard_problems, table_problems

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
    "data_version_table, installer, users::text AS users FROM config"
)
REGISTERED: Final = "SELECT c.relname FROM scoped_table t JOIN pg_class c ON c.oid = t.rel"
MEMBERS: Final = (
    "SELECT m.rolname AS member, r.rolname AS role, r.rolbypassrls, m.rolsuper, "
    "CASE WHEN current_setting('server_version_num')::int >= 160000 "
    "THEN am.inherit_option ELSE m.rolinherit END AS inherits "
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
class Finding:
    """One difference between the declaration and the database."""

    table: str | None
    check: str
    expected: str
    actual: str


@dataclass(frozen=True, slots=True)
class Report:
    """The outcome of ``verify``; ``ok`` when nothing differs."""

    ok: bool
    findings: tuple[Finding, ...]


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
            findings += await _registry(connection, application.scoped)
            findings += await _protection(connection, application.scoped, bootstrap)
            findings += await _assertion(connection)
            await connection.execute(_GUARD_FIRST, guard)
            acl, sequences = await _acl(connection, bootstrap.schema)
            findings += _group_privileges(acl, application.scoped, bootstrap)
            findings += await _sequence_usage(connection, acl, application.scoped, bootstrap)
            findings += _bypass_privileges(acl, sequences, bootstrap)
            findings += _global_privileges(acl, application, bootstrap)
            findings += await _global_parent_findings(
                connection, acl, application.scoped, bootstrap
            )
            findings += await _memberships(connection, bootstrap)
    finally:
        await engine.dispose()
    return Report(ok=not findings, findings=tuple(findings))


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
    if actual != expected:
        return [Finding(None, "guard.config", str(expected), str(actual))]
    return []


def _users(users: Any) -> list[tuple[str, bool, str]]:
    return sorted((str(u["name"]), bool(u["login"]), str(u["access"])) for u in users)


async def _registry(
    connection: AsyncConnection, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> list[Finding]:
    registered = {str(row[0]) for row in await connection.execute(_REGISTERED)}
    expected = {table.name for table in scoped.values()}
    if registered != expected:
        return [_diff(None, "registry", "registered tables", expected, registered)]
    return []


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
    acl: Acl, sequences: frozenset[str], bootstrap: BootstrapConfig
) -> list[Finding]:
    bypass = bootstrap.bypass_users
    version_table = bootstrap.names.version_table
    all_row_privileges = {p.value for p in READ_WRITE}
    findings: list[Finding] = []
    for relname, grants in acl.items():
        for user in bypass:
            actual = grants.get(user, set())
            if relname == version_table and actual != BYPASS_VERSION_PRIVILEGES:
                findings.append(
                    _diff(relname, "bypass.version_table", user, BYPASS_VERSION_PRIVILEGES, actual)
                )
            elif relname in sequences and not USAGE.issubset(actual):
                findings.append(_diff(relname, "bypass.sequence_usage", user, USAGE, actual))
            elif (
                relname != version_table
                and relname not in sequences
                and not all_row_privileges <= actual
            ):
                findings.append(
                    _diff(relname, "bypass.privileges", user, all_row_privileges, actual)
                )
    return findings


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
