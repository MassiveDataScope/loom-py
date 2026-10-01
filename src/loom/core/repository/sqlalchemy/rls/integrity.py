"""Checks that the guard in the database is one this release of loom shipped, unaltered.

Everything here reads the catalogue, which every role can read, so the
application user runs it at startup, the bootstrap runs it inside its own
transaction and ``verify`` runs it with any connection. The catalogue of the
guard is compared, category by category, with the digests pinned for the
revision its functions identify; owners are compared exactly with the guard
schema's owner, which must also own both event triggers; every grant and every
function setting is compared exactly. Only the configuration row needs a
privileged reader; ``verify`` checks it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import MISSING_EVENT_TRIGGERS
from loom.core.config import ConfigError
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    GUARD_REVISIONS,
    MIN_COMPATIBLE_GUARD_REVISION,
    OWNER_FUNCTIONS,
    OWNER_TABLES,
    PINNED_SETTINGS,
    GuardRevision,
    catalog_digest,
)

FUNCTIONS: Final = "functions"
CATALOG_CHECKS: Final = (FUNCTIONS, "relations", "columns", "constraints", "triggers", "objects")
SCHEMA_KIND: Final = "schema"
OWNER_TRIGGER: Final = "loom_deny_owner_dml"

_CATALOG_SQL: Final = (
    "WITH g AS (SELECT oid FROM pg_namespace WHERE nspname = :guard) "
    "SELECT kind, line FROM ("
    "SELECT 'functions' AS kind, replace(concat_ws(chr(31), p.proname, "
    "pg_get_function_identity_arguments(p.oid), pg_get_function_arguments(p.oid), "
    "pg_get_function_result(p.oid)), quote_ident(:guard) || '.', '') || chr(31) "
    "|| concat_ws(chr(31), l.lanname, p.prosecdef, p.provolatile, p.proisstrict, "
    "p.proleakproof, p.proparallel, p.prokind, p.prosrc) AS line "
    "FROM pg_proc p JOIN g ON g.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang "
    "UNION ALL SELECT 'relations', c.relname || ':' || c.relkind::text "
    "FROM pg_class c JOIN g ON g.oid = c.relnamespace "
    "UNION ALL SELECT 'columns', c.relname || '.' || a.attname || ' ' "
    "|| format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN ' NOT NULL' "
    "ELSE '' END || coalesce(' DEFAULT ' || pg_get_expr(d.adbin, d.adrelid), '') "
    "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN g ON g.oid = c.relnamespace "
    "LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
    "WHERE a.attnum > 0 AND NOT a.attisdropped "
    "UNION ALL SELECT 'constraints', c.relname || '.' || con.conname || ' ' "
    "|| pg_get_constraintdef(con.oid) "
    "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
    "JOIN g ON g.oid = c.relnamespace "
    "UNION ALL SELECT 'triggers', c.relname || '.' || t.tgname || ' ' || t.tgenabled::text || ' ' "
    "|| t.tgtype || ' ' || f.proname || ' ' || (f.pronamespace = g.oid) "
    "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN g ON g.oid = c.relnamespace "
    "JOIN pg_proc f ON f.oid = t.tgfoid "
    "UNION ALL SELECT 'triggers', c.relname || ' rule ' || r.rulename "
    "FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class JOIN g ON g.oid = c.relnamespace "
    "UNION ALL SELECT 'objects', 'operator ' || o.oprname "
    "FROM pg_operator o JOIN g ON g.oid = o.oprnamespace "
    "UNION ALL SELECT 'objects', 'type ' || t.typname FROM pg_type t "
    "JOIN g ON g.oid = t.typnamespace LEFT JOIN pg_type e ON e.oid = t.typelem "
    "WHERE t.typrelid = 0 AND coalesce(e.typrelid, 0) = 0 "
    "UNION ALL SELECT 'objects', 'operator class ' || opcname "
    "FROM pg_opclass JOIN g ON g.oid = opcnamespace "
    "UNION ALL SELECT 'objects', 'operator family ' || opfname "
    "FROM pg_opfamily JOIN g ON g.oid = opfnamespace "
    "UNION ALL SELECT 'objects', 'collation ' || collname "
    "FROM pg_collation JOIN g ON g.oid = collnamespace "
    "UNION ALL SELECT 'objects', 'conversion ' || conname "
    "FROM pg_conversion JOIN g ON g.oid = connamespace "
    "UNION ALL SELECT 'objects', 'text search configuration ' || cfgname "
    "FROM pg_ts_config JOIN g ON g.oid = cfgnamespace "
    "UNION ALL SELECT 'objects', 'text search dictionary ' || dictname "
    "FROM pg_ts_dict JOIN g ON g.oid = dictnamespace "
    "UNION ALL SELECT 'objects', 'text search parser ' || prsname "
    "FROM pg_ts_parser JOIN g ON g.oid = prsnamespace "
    "UNION ALL SELECT 'objects', 'text search template ' || tmplname "
    "FROM pg_ts_template JOIN g ON g.oid = tmplnamespace "
    "UNION ALL SELECT 'objects', 'statistics ' || stxname "
    "FROM pg_statistic_ext JOIN g ON g.oid = stxnamespace"
    ") AS catalog WHERE kind = ANY (CAST(:kinds AS text[]))"
)
_ACCESS_SQL: Final = (
    "WITH g AS (SELECT oid FROM pg_namespace WHERE nspname = :guard) "
    "SELECT o.kind, o.name, o.subject, r.rolname AS owner, o.config, "
    "ARRAY(SELECT coalesce(m.rolname, 'PUBLIC') || ':' || a.privilege_type "
    "FROM aclexplode(o.acl) a LEFT JOIN pg_roles m ON m.oid = a.grantee "
    "WHERE a.grantee <> o.owner ORDER BY 1) AS grants FROM ("
    "SELECT 'schema' AS kind, n.nspname AS name, n.nspname AS subject, n.nspowner AS owner, "
    "coalesce(n.nspacl, acldefault('n', n.nspowner)) AS acl, ARRAY[]::text[] AS config "
    "FROM pg_namespace n JOIN g ON g.oid = n.oid "
    "UNION ALL SELECT 'function', p.proname, "
    "p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', p.proowner, "
    "coalesce(p.proacl, acldefault('f', p.proowner)), coalesce(p.proconfig, ARRAY[]::text[]) "
    "FROM pg_proc p JOIN g ON g.oid = p.pronamespace "
    "UNION ALL SELECT 'relation', c.relname, c.relname, c.relowner, "
    "coalesce(c.relacl, acldefault('r', c.relowner)), ARRAY[]::text[] "
    "FROM pg_class c JOIN g ON g.oid = c.relnamespace "
    "UNION ALL SELECT 'type', t.typname, t.typname, t.typowner, "
    "NULL::aclitem[], ARRAY[]::text[] "
    "FROM pg_type t JOIN g ON g.oid = t.typnamespace"
    ") AS o JOIN pg_roles r ON r.oid = o.owner"
)
_LANDING_SQL: Final = (
    "SELECT n.nspname AS schema, r.rolname AS owner FROM pg_namespace n "
    "JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = current_schema()"
)
_SCOPED_TABLES_SQL: Final = (
    "SELECT c.relname AS name, c.relrowsecurity AND c.relforcerowsecurity AS forced, "
    "ARRAY(SELECT p.polname::text FROM pg_policy p WHERE p.polrelid = c.oid ORDER BY 1) "
    "AS policies, NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid "
    "AND NOT p.polpermissive) AS permissive, EXISTS (SELECT 1 FROM pg_trigger t "
    "JOIN pg_proc f ON f.oid = t.tgfoid JOIN pg_namespace fn ON fn.oid = f.pronamespace "
    "WHERE t.tgrelid = c.oid AND t.tgname = :trigger AND t.tgenabled = 'A' "
    "AND f.proname = 'deny_owner_dml' AND fn.nspname = :guard) AS owner_trigger "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :schema AND c.relname = ANY (CAST(:names AS text[]))"
)
_CATALOG: Final = text(_CATALOG_SQL)
_ACCESS: Final = text(_ACCESS_SQL)
_LANDING: Final = text(_LANDING_SQL)
_SCOPED_TABLES: Final = text(_SCOPED_TABLES_SQL)
_MISSING_EVENT_TRIGGERS: Final = text(MISSING_EVENT_TRIGGERS)
_EXPECTED_PRIVILEGE: Final = {
    SCHEMA_KIND: ("USAGE", None),
    "function": ("EXECUTE", OWNER_FUNCTIONS),
    "relation": ("SELECT", OWNER_TABLES),
    "type": ("USAGE", frozenset[str]()),
}


@dataclass(frozen=True, slots=True)
class Problem:
    """One way the installed guard, or a scoped table, differs from what loom expects."""

    check: str
    subject: str
    expected: str
    actual: str


async def guard_problems(
    connection: AsyncConnection, guard: str, owner: str, installer: str | None = None
) -> list[Problem]:
    """Return how the guard ``guard`` differs from a released one; empty when it matches.

    ``owner`` is the application schema's owner role, the only grantee the
    guard knows. With ``installer`` the guard's owner must be that role.
    """
    rows = await connection.execute(_CATALOG, _catalog_parameters(guard, CATALOG_CHECKS))
    lines = _lines(rows)
    if not lines[FUNCTIONS]:
        return [Problem("guard.functions", guard, "installed", "missing")]
    problems = catalog_problems(lines, guard)
    access = list(await connection.execute(_ACCESS, _guard_parameters(guard)))
    problems += owner_problems(access, installer)
    problems += access_problems(access, guard, owner)
    triggers = await connection.execute(_MISSING_EVENT_TRIGGERS, _guard_parameters(guard))
    problems += [
        Problem("guard.event_triggers", str(row[0]), "enabled always", "missing or changed")
        for row in triggers
    ]
    return problems


async def startup_problems(
    connection: AsyncConnection, guard: str, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> list[Problem]:
    """Everything the application user can check at startup, guard and scoped tables."""
    landing = (await connection.execute(_LANDING)).one_or_none()
    if landing is None:
        return [Problem("schema.landing", "current_schema()", "an existing schema", "none")]
    problems = await guard_problems(connection, guard, str(landing.owner))
    problems += await table_problems(connection, guard, str(landing.schema), scoped)
    return problems


async def table_problems(
    connection: AsyncConnection,
    guard: str,
    schema: str,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
) -> list[Problem]:
    """Compare every scoped model table with its canonical protection in the catalogue."""
    parameters = {
        **_guard_parameters(guard),
        "schema": schema,
        "trigger": OWNER_TRIGGER,
        "names": [table.name for table in scoped.values()],
    }
    found = {str(row.name): row for row in await connection.execute(_SCOPED_TABLES, parameters)}
    problems: list[Problem] = []
    for table in scoped.values():
        row = found.get(table.name)
        if row is None:
            problems.append(Problem("table.missing", table.name, "present", "missing"))
        else:
            problems += _protection_problems(row, table)
    return problems


def canonical_policies(privileges: Iterable[Privilege]) -> set[str]:
    """The policy names the guard creates for a scoped table with ``privileges``."""
    writes = {f"loom_{p.value.lower()}" for p in privileges if p is not Privilege.SELECT}
    return {"loom_select", *writes}


def _protection_problems(row: Any, table: ScopedTable) -> list[Problem]:
    expected = canonical_policies(table.privileges)
    actual = set(row.policies)
    checks = (
        ("table.rls", bool(row.forced), "forced row-level security", "not forced"),
        ("table.policies", actual == expected, str(sorted(expected)), str(sorted(actual))),
        ("table.permissive", bool(row.permissive), "permissive policies", "restrictive"),
        ("table.owner_trigger", bool(row.owner_trigger), "enabled always, guard function", "no"),
    )
    return [
        Problem(check, table.name, wanted, got)
        for check, passed, wanted, got in checks
        if not passed
    ]


def revision_of(functions_digest: str) -> int | None:
    """The released revision whose functions digest is ``functions_digest``, if any."""
    known = {revision.catalog[FUNCTIONS]: revision.number for revision in GUARD_REVISIONS}
    return known.get(functions_digest)


def require_revision(number: int | None, guard: str, *, minimum: int) -> None:
    """Refuse a guard this release does not know or one below ``minimum``.

    Raises:
        ConfigError: Naming the guard and what to run.
    """
    if number is None:
        raise ConfigError(
            f"the guard {guard} is not one this release of loom ships: run apply_bootstrap"
        )
    if number < minimum:
        raise ConfigError(
            f"guard revision pending: {guard} is at revision {number} and this release of loom "
            f"needs {minimum}; upgrade the application, then run apply_bootstrap"
        )


async def require_guard_revision(connection: AsyncConnection, guard: str) -> None:
    """Refuse to call a guard below the minimum compatible revision.

    Raises:
        ConfigError: When the guard is unknown or its revision is pending.
    """
    rows = await connection.execute(_CATALOG, _catalog_parameters(guard, (FUNCTIONS,)))
    number = revision_of(catalog_digest(str(row.line) for row in rows))
    require_revision(number, guard, minimum=MIN_COMPATIBLE_GUARD_REVISION)


def require_guard_revision_sync(connection: Connection, guard: str) -> None:
    """The synchronous :func:`require_guard_revision`, for the migration runners.

    Raises:
        ConfigError: When the guard is unknown or its revision is pending.
    """
    rows = connection.execute(_CATALOG, _catalog_parameters(guard, (FUNCTIONS,)))
    number = revision_of(catalog_digest(str(row.line) for row in rows))
    require_revision(number, guard, minimum=MIN_COMPATIBLE_GUARD_REVISION)


def catalog_problems(lines: Mapping[str, Sequence[str]], guard: str) -> list[Problem]:
    """Compare each catalogue category with the revision its functions identify."""
    number = revision_of(catalog_digest(lines.get(FUNCTIONS, ())))
    revision = _revision(number)
    problems = [
        Problem(f"guard.{check}", guard, revision.catalog[check], actual)
        for check in CATALOG_CHECKS
        if (actual := catalog_digest(lines.get(check, ()))) != revision.catalog[check]
    ]
    if number is not None and number < MIN_COMPATIBLE_GUARD_REVISION:
        problems.append(
            Problem("guard.revision", guard, f">= {MIN_COMPATIBLE_GUARD_REVISION}", str(number))
        )
    return problems


def owner_problems(rows: Sequence[Any], installer: str | None) -> list[Problem]:
    """Every guard object must belong to the guard schema's owner, and that to ``installer``."""
    schema_owner = next((str(row.owner) for row in rows if row.kind == SCHEMA_KIND), None)
    expected = installer or schema_owner
    return [
        Problem("guard.owner", str(row.subject), str(expected), str(row.owner))
        for row in rows
        if str(row.owner) != expected
    ]


def access_problems(rows: Sequence[Any], guard: str, owner: str) -> list[Problem]:
    """Compare every grant and every function setting of the guard exactly."""
    problems: list[Problem] = []
    search_path = f"search_path=pg_catalog, {guard}, pg_temp"
    for row in rows:
        privilege, granted_to_owner = _EXPECTED_PRIVILEGE[str(row.kind)]
        allowed = granted_to_owner is None or row.name in granted_to_owner
        expected = {f"{owner}:{privilege}"} if allowed else set()
        if set(row.grants) != expected:
            problems.append(
                Problem(
                    f"guard.{row.kind}_grants",
                    str(row.subject),
                    str(sorted(expected)),
                    str(sorted(row.grants)),
                )
            )
        settings = [*PINNED_SETTINGS.get(str(row.name), ()), search_path]
        if row.kind == "function" and list(row.config) != settings:
            problems.append(
                Problem("guard.function_config", str(row.subject), str(settings), str(row.config))
            )
    return problems


def _guard_parameters(guard: str) -> dict[str, Any]:
    return {"guard": guard}


def _catalog_parameters(guard: str, kinds: Iterable[str]) -> dict[str, Any]:
    return {**_guard_parameters(guard), "kinds": list(kinds)}


def _lines(rows: Iterable[Any]) -> dict[str, list[str]]:
    lines: dict[str, list[str]] = {check: [] for check in CATALOG_CHECKS}
    for row in rows:
        lines[str(row.kind)].append(str(row.line))
    return lines


def _revision(number: int | None) -> GuardRevision:
    known = {revision.number: revision for revision in GUARD_REVISIONS}
    return known.get(number or GUARD_REVISIONS[-1].number, GUARD_REVISIONS[-1])
