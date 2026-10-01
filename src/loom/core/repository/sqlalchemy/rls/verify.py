"""Verify what the guard's assertion cannot require: presence, not only absence.

The assertion forbids excess at every DDL; ``verify`` additionally requires
that every group, bypass user and global table holds exactly what the model
and the declaration say, and that memberships match each user's access.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.model.introspection import declared_privileges, get_table_name, is_row_scoped
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig

if TYPE_CHECKING:
    from loom.core.locator import Application

Acl = dict[str, dict[str, set[str]]]
WRITES = frozenset({Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE})
VERSION_TABLE = "alembic_version"

_RELATION_ACL = text(
    "SELECT c.relname, c.relkind, coalesce(r.rolname, 'PUBLIC') AS grantee, a.privilege_type "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
    "LATERAL aclexplode(coalesce(c.relacl, "
    "acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::\"char\", c.relowner))) a "
    "LEFT JOIN pg_roles r ON r.oid = a.grantee "
    "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p', 'S') AND a.grantee <> c.relowner"
)
_SERIALS = text(
    "SELECT t.relname AS table_name, s.relname AS sequence_name "
    "FROM pg_depend d JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' "
    "JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace n ON n.oid = t.relnamespace "
    "WHERE n.nspname = :schema AND d.deptype = 'a'"
)
_FOREIGN_KEYS = text(
    "SELECT c.relname AS child, p.relname AS parent, "
    "con.confdeltype::text AS on_delete, con.confupdtype::text AS on_update "
    "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
    "JOIN pg_class p ON p.oid = con.confrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE con.contype = 'f' AND n.nspname = :schema"
)
_MEMBERS = text(
    "SELECT m.rolname AS member, r.rolname AS role, r.rolbypassrls, m.rolinherit, "
    "CASE WHEN current_setting('server_version_num')::int >= 160000 "
    "THEN am.inherit_option ELSE m.rolinherit END AS inherits "
    "FROM pg_auth_members am JOIN pg_roles m ON m.oid = am.member "
    "JOIN pg_roles r ON r.oid = am.roleid"
)


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
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            findings = await _assertion(connection, bootstrap.schema)
            acl = await _acl(connection, bootstrap.schema)
            findings += _group_privileges(acl, application.scoped, bootstrap.schema)
            findings += await _sequence_usage(connection, acl, application.scoped, bootstrap)
            findings += _bypass_privileges(acl, bootstrap)
            findings += _global_privileges(acl, application, bootstrap.schema)
            findings += await _c9(connection, acl, application.scoped, bootstrap.schema)
            findings += await _memberships(connection, bootstrap)
    finally:
        await engine.dispose()
    return Report(ok=not findings, findings=tuple(findings))


async def _assertion(connection: AsyncConnection, schema: str) -> list[Finding]:
    try:
        await connection.execute(text(f"SELECT loom_guard_{schema}.assert_scoped_schema()"))
    except DBAPIError as exc:
        await connection.rollback()
        return [Finding(None, "assertion", "passes", str(exc.orig))]
    return []


async def _acl(connection: AsyncConnection, schema: str) -> Acl:
    acl: Acl = defaultdict(lambda: defaultdict(set))
    rows = await connection.execute(_RELATION_ACL, {"schema": schema})
    for row in rows:
        acl[str(row.relname)][str(row.grantee)].add(str(row.privilege_type))
    return acl


def _group_privileges(
    acl: Acl, scoped: Mapping[tuple[str | None, str], ScopedTable], schema: str
) -> list[Finding]:
    findings: list[Finding] = []
    for table in scoped.values():
        expected = {
            f"{schema}_readers": {p.value for p in table.privileges if p is Privilege.SELECT},
            f"{schema}_writers": {p.value for p in table.privileges if p in WRITES},
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
    writers = f"{bootstrap.schema}_writers"
    findings: list[Finding] = []
    for row in await connection.execute(_SERIALS, {"schema": bootstrap.schema}):
        table_name, sequence = str(row.table_name), str(row.sequence_name)
        if table_name not in inserting:
            continue
        actual = acl[sequence].get(writers, set())
        if actual != {"USAGE"}:
            findings.append(_diff(table_name, "sequence.usage", writers, {"USAGE"}, actual))
    return findings


def _bypass_privileges(acl: Acl, bootstrap: BootstrapConfig) -> list[Finding]:
    bypass = [u for u, spec in bootstrap.database_users.items() if spec.access == "bypass"]
    four = {p.value for p in READ_WRITE}
    findings: list[Finding] = []
    for relname, grants in acl.items():
        for user in bypass:
            actual = grants.get(user, set())
            if relname == VERSION_TABLE and actual:
                findings.append(_diff(relname, "bypass.alembic_version", user, set(), actual))
            elif relname != VERSION_TABLE and _is_table(relname, acl) and not four <= actual:
                findings.append(_diff(relname, "bypass.privileges", user, four, actual))
    return findings


def _global_privileges(acl: Acl, application: Application, schema: str) -> list[Finding]:
    findings: list[Finding] = []
    for model in application.models:
        if is_row_scoped(model):
            continue
        table = get_table_name(model)
        declared = declared_privileges(model)
        for group in ("readers", "writers"):
            wanted = {p.value for p in declared.get(group, frozenset())}
            actual = acl[table].get(f"{schema}_{group}", set())
            if actual != wanted:
                findings.append(
                    _diff(table, "global.privileges", f"{schema}_{group}", wanted, actual)
                )
    return findings


async def _c9(
    connection: AsyncConnection,
    acl: Acl,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    schema: str,
) -> list[Finding]:
    scoped_names = {t.name for t in scoped.values()}
    groups = (f"{schema}_readers", f"{schema}_writers")
    findings: list[Finding] = []
    for row in await connection.execute(_FOREIGN_KEYS, {"schema": schema}):
        child, parent = str(row.child), str(row.parent)
        if child not in scoped_names or parent in scoped_names:
            continue
        for action in (str(row.on_delete), str(row.on_update)):
            if action not in ("r", "a"):
                findings.append(Finding(child, "c9.action", "RESTRICT or NO ACTION", action))
        writes = {g: acl[parent].get(g, set()) & {"UPDATE", "DELETE"} for g in groups}
        for group, held in writes.items():
            if held:
                findings.append(_diff(parent, "c9.group_write", group, set(), held))
    return findings


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
    readers, writers = f"{bootstrap.schema}_readers", f"{bootstrap.schema}_writers"
    by_access = {"read": {readers}, "write": {readers, writers}, "bypass": set()}
    findings: list[Finding] = []
    for user, spec in bootstrap.database_users.items():
        wanted = by_access[spec.access]
        actual = memberships[user] & {readers, writers}
        if actual != wanted:
            findings.append(_diff(None, "membership.access", user, wanted, actual))
    return findings


def _is_table(relname: str, acl: Acl) -> bool:
    return not relname.endswith("_seq") and relname in acl


def _diff(
    table: str | None, check: str, subject: str, wanted: set[str], actual: set[str]
) -> Finding:
    return Finding(table, check, f"{subject}: {sorted(wanted)}", f"{subject}: {sorted(actual)}")
