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
from typing import Any, Final, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import MISSING_EVENT_TRIGGERS
from loom.core.config import ConfigError
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    AUDIT_GUARD_REVISION,
    AUDIT_LOG_SHAPE,
    GUARD_REVISIONS,
    MIN_COMPATIBLE_GUARD_REVISION,
    OWNER_FUNCTIONS,
    OWNER_TABLES,
    PINNED_SETTINGS,
    READ_AUDIT_LOG_ARGUMENTS,
    READING_GUARD_REVISION,
    GuardRevision,
    catalog_digest,
)

FUNCTIONS: Final = "functions"
AUDIT_SHAPE: Final = (
    "WITH log AS (SELECT to_regclass(quote_ident(:schema) || '.audit_log') AS rel) "
    "SELECT line FROM ("
    "SELECT 1 AS kind, a.attnum AS position, a.attname || ' ' "
    "|| format_type(a.atttypid, a.atttypmod) "
    "|| CASE WHEN a.attnotnull THEN ' not null' ELSE '' END AS line "
    "FROM pg_attribute a, log WHERE a.attrelid = log.rel AND a.attnum > 0 AND NOT a.attisdropped "
    "UNION ALL SELECT 2, 0, 'constraint ' || pg_get_constraintdef(c.oid) "
    "FROM pg_constraint c, log WHERE c.conrelid = log.rel "
    "UNION ALL SELECT 3, 0, 'index ' || replace(pg_get_indexdef(i.indexrelid), "
    "quote_ident(:schema) || '.', '') FROM pg_index i, log WHERE i.indrelid = log.rel"
    ") shape ORDER BY kind, position, line"
)
# Whether the guard has the audit log's revision: its registry of audited tables.
# Read from the catalogue, not through to_regclass: the application role has
# no USAGE on the guard schema, and startup asks this as that role.
AUDIT_SUPPORTED: Final = (
    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c"
    " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
    " WHERE n.nspname = :guard AND c.relname = 'audited_table')"
)
READ_AUDIT_LOG: Final = "read_audit_log"
# Every ``<schema>.read_audit_log`` as the catalogue holds it, readable by any
# role: its identity arguments, whether the schema's owner owns it, whether it
# runs as its definer, its settings, the one setting the guard gives it as
# Postgres renders it (identifiers quoted), every grant but the owner's as
# ``grantee:privilege[ (grantable)]``, whether the connecting role may execute
# it, its source, volatility, language and result type.
AUDIT_READER: Final = (
    "SELECT pg_get_function_identity_arguments(p.oid) AS arguments,"
    " p.proowner = n.nspowner AS owned, p.prosecdef AS definer,"
    " coalesce(p.proconfig, ARRAY[]::text[]) AS config,"
    " format('search_path=pg_catalog, %I, pg_temp', n.nspname) AS search_path,"
    " ARRAY(SELECT coalesce(g.rolname, 'PUBLIC') || ':' || a.privilege_type"
    " || CASE WHEN a.is_grantable THEN ' (grantable)' ELSE '' END"
    " FROM pg_catalog.aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a"
    " LEFT JOIN pg_catalog.pg_roles g ON g.oid = a.grantee"
    " WHERE a.grantee <> p.proowner ORDER BY 1) AS grants,"
    " has_function_privilege(p.oid, 'EXECUTE') AS executable, p.prosrc AS source,"
    " p.provolatile::text AS volatility, l.lanname::text AS language,"
    " pg_get_function_result(p.oid) AS result"
    " FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace"
    " JOIN pg_catalog.pg_language l ON l.oid = p.prolang"
    " WHERE n.nspname = :schema AND p.proname = 'read_audit_log' ORDER BY p.oid"
)
# What ``guard/0004.sql`` releases the reader as: STABLE, in PL/pgSQL, returning
# this table as ``pg_get_function_result`` renders it.
READ_AUDIT_LOG_VOLATILITY: Final = "s"
READ_AUDIT_LOG_LANGUAGE: Final = "plpgsql"
READ_AUDIT_LOG_RESULT: Final = (
    "TABLE(id bigint, at timestamp with time zone, op text, schema_name text, table_name text, "
    "row_key text, changed jsonb, subject text, mechanism text, on_behalf_of text, "
    "trace_id text, elevated text[], origin text)"
)
# The body ``<guard>.audit_reader_source()`` returns, rendered from its catalogued
# source, which the guard's functions digest pins: ``format`` over the template
# between its ``$src$`` quotes, with the guard's name. Any role can read it, and
# no search path resolves it; the application role may not call the function.
# It ships in the reader's revision, so a guard without it predates that revision.
RELEASED_READER_SOURCE: Final = (
    "SELECT format(split_part(p.prosrc, '$src$', 2), n.nspname)"
    " FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace"
    " WHERE n.nspname = :guard AND p.proname = 'audit_reader_source'"
)
AUDIT_READABLE: Final = (
    "SELECT has_table_privilege(to_regclass(quote_ident(:schema) || '.audit_log'), 'SELECT')"
)
CATALOG_CHECKS: Final = (FUNCTIONS, "relations", "columns", "constraints", "triggers", "objects")
SCHEMA_KIND: Final = "schema"
_SCHEMA_PARAM: Final = SCHEMA_KIND
_AUDIT_LOG: Final = "audit_log"
_AUDIT_SHAPE_CHECK: Final = "audit.shape"
_MISSING: Final = "missing"
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
_AUDIT_SHAPE: Final = text(AUDIT_SHAPE)
_AUDIT_SUPPORTED: Final = text(AUDIT_SUPPORTED)
_AUDIT_READABLE: Final = text(AUDIT_READABLE)
_AUDIT_READER: Final = text(AUDIT_READER)
_RELEASED_READER_SOURCE: Final = text(RELEASED_READER_SOURCE)
_READER_GRANT: Final = "audit.reader_grant"
_READER_SHAPE: Final = "audit.reader_shape"
_READER_SOURCE: Final = "audit.reader_source"
_PUBLIC: Final = "PUBLIC"
AUDIT_TRIGGER: Final = "loom_audit_record"
# The catalogue, not the guard: the application role reads pg_trigger.
AUDIT_TRIGGERS: Final = (
    "SELECT c.relname FROM pg_catalog.pg_trigger g"
    " JOIN pg_catalog.pg_class c ON c.oid = g.tgrelid"
    " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
    " WHERE n.nspname = :schema AND g.tgname = 'loom_audit_record' AND g.tgenabled = 'A'"
)
_AUDIT_TRIGGERS: Final = text(AUDIT_TRIGGERS)
# Consecutive UTC months, from this one, that a partition of the log covers; 0
# when this month is missing, NULL without a log. Read by partition name from the
# catalogue, so the application role can ask: the guard names every partition
# ``audit_log_pYYYYMM`` after the UTC month its bounds span.
AUDIT_MONTHS_COVERED: Final = (
    "WITH log AS (SELECT to_regclass("
    "quote_ident(coalesce(CAST(:schema AS text), current_schema())) || '.audit_log') AS rel),"
    " parts AS (SELECT c.relname FROM pg_catalog.pg_inherits i"
    " JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid"
    " WHERE i.inhparent = (SELECT rel FROM log)),"
    " months AS (SELECT i, 'audit_log_p' || to_char("
    "date_trunc('month', now() AT TIME ZONE 'UTC') + make_interval(months => i), 'YYYYMM')"
    " AS relname FROM generate_series(0, 36) AS i)"
    " SELECT CASE WHEN (SELECT rel FROM log) IS NULL THEN NULL"
    " ELSE coalesce(min(m.i) FILTER (WHERE m.relname NOT IN (SELECT relname FROM parts)), 37)"
    " END AS covered FROM months m"
)
# Fewer covered months than this is a warning at startup and a finding in verify.
AUDIT_HORIZON_WARNING_MONTHS: Final = 3
_AUDIT_MONTHS_COVERED: Final = text(AUDIT_MONTHS_COVERED)
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
        return [Problem("guard.functions", guard, "installed", _MISSING)]
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
    connection: AsyncConnection,
    guard: str,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    *,
    audited: bool = False,
    recording: Sequence[str] = (),
    reading: bool = False,
) -> list[Problem]:
    """Everything the application user can check at startup: guard, scoped tables, audit log.

    The audit log is checked whenever it exists, marked tables or not (FR-018);
    an application with audited tables needs revision 3 and the log, and a
    reading store needs the log (N13).
    *recording* names the tables that must carry their audit trigger, so a
    model marked but never migrated does not write unrecorded rows. *reading*
    is the store's ``schema.audit.reading.enabled``; see :func:`reading_problems`.
    """
    landing = (await connection.execute(_LANDING)).one_or_none()
    if landing is None:
        return [Problem("schema.landing", "current_schema()", "an existing schema", "none")]
    schema = str(landing.schema)
    problems = await guard_problems(connection, guard, str(landing.owner))
    problems += await table_problems(connection, guard, schema, scoped)
    problems += await audit_problems(connection, guard, schema, audited=audited, reading=reading)
    problems += await reading_problems(connection, guard, schema, reading=reading)
    if not problems:
        problems += await _trigger_problems(connection, str(landing.schema), recording)
    return problems


async def audit_problems(
    connection: AsyncConnection, guard: str, schema: str, *, audited: bool, reading: bool
) -> list[Problem]:
    """The audit log's revision, shape and read access, as any role sees them.

    The log must exist when tables are *audited* or the store is *reading*;
    otherwise only an existing log is checked.
    """
    if audited and (older := await _older_than_audit(connection, guard)):
        return [older]
    parameters = {_SCHEMA_PARAM: schema}
    shape = [str(row.line) for row in await connection.execute(_AUDIT_SHAPE, parameters)]
    if not (shape or audited or reading):
        return []
    return audit_shape_problems(shape) + await _read_problems(connection, shape, parameters)


async def reading_problems(
    connection: AsyncConnection, guard: str, schema: str, *, reading: bool
) -> list[Problem]:
    """The audit reader as the application user sees it; see :func:`reader_problems`.

    The released source is read only while *reading*. A guard that releases
    none is below the reader's revision, and that is then the only problem:
    it names the function that revision installs (US1 #3).
    """
    if not reading:
        return _unread_reader_problems(await _audit_readers(connection, schema))
    source = await released_reader_source(connection, guard)
    if source is None:
        return [_older_than_reading(guard)]
    return reader_problems(await _audit_readers(connection, schema), source, reading=True)


async def installed_reader(
    connection: AsyncConnection, guard: str, schema: str
) -> tuple[list[Any], str | None]:
    """Every reader in *schema* and, only when there is one, the body *guard* releases."""
    rows = await _audit_readers(connection, schema)
    return rows, (await released_reader_source(connection, guard) if rows else None)


async def _audit_readers(connection: AsyncConnection, schema: str) -> list[Any]:
    """Every ``<schema>.read_audit_log`` as ``AUDIT_READER`` reads it."""
    return list(await connection.execute(_AUDIT_READER, {_SCHEMA_PARAM: schema}))


async def released_reader_source(connection: AsyncConnection, guard: str) -> str | None:
    """The reader body the guard *guard* releases; ``None`` below the reader's revision."""
    source = (await connection.execute(_RELEASED_READER_SOURCE, _guard_parameters(guard))).scalar()
    return None if source is None else str(source)


def reader_problems(rows: Sequence[Any], source: str | None, *, reading: bool) -> list[Problem]:
    """Compare the readers in *rows* with what the store's *reading* flag needs.

    While reading, the reader must exist, be the released shape with the
    released *source*, and be executable by the connecting role through one
    grantee. While not, nobody but its owner may hold EXECUTE: a grant implies
    the flag.
    """
    if not reading:
        return _unread_reader_problems(rows)
    if not rows:
        return [Problem("audit.reader", READ_AUDIT_LOG, "installed", _MISSING)]
    problems = reader_shape_problems(rows) + reader_source_problems(rows, source)
    return problems + _reader_grant_problems(rows[0])


def reader_source_problems(rows: Sequence[Any], source: str | None) -> list[Problem]:
    """A problem per reader whose body is not the released *source*; ``None`` matches none."""
    return [
        Problem(_READER_SOURCE, READ_AUDIT_LOG, "the released source", "changed")
        for row in rows
        if row.source != source
    ]


def _unread_reader_problems(rows: Sequence[Any]) -> list[Problem]:
    """A problem when the connecting role may execute the reader or its ACL names a grantee.

    The ACL catches a member that does not inherit the grantee's privileges.
    """
    executable = any(bool(row.executable) for row in rows)
    held = [str(grant) for row in rows for grant in row.grants]
    if not (executable or held):
        return []
    actual = "EXECUTE" if executable else ", ".join(held)
    return [Problem(_READER_GRANT, READ_AUDIT_LOG, "no EXECUTE while not reading", actual)]


def _reader_grant_problems(row: Any) -> list[Problem]:
    """EXECUTE goes to one role besides PUBLIC, without grant option, and reaches this user.

    Startup does not know the readers group's name; ``verify`` compares it exactly.
    PUBLIC is a shape problem.
    """
    held = [grant for grant in _grants(row) if grant.role != _PUBLIC]
    if len(held) != 1 or (held[0].privilege, held[0].grantable) != ("EXECUTE", False):
        wanted = "EXECUTE to the readers group alone, without grant option"
        actual = ", ".join(map(str, held)) or "none"
        return [Problem(_READER_GRANT, READ_AUDIT_LOG, wanted, actual)]
    if not row.executable:
        return [Problem(_READER_GRANT, READ_AUDIT_LOG, "EXECUTE", "none")]
    return []


def reader_shape_problems(rows: Sequence[Any]) -> list[Problem]:
    """One reader, in the released shape, with no PUBLIC grant.

    The shape is its arguments, result type, language, volatility, owner,
    definer rights and settings, as ``guard/0004.sql`` releases them.
    """
    if len(rows) > 1:
        return [Problem(_READER_SHAPE, READ_AUDIT_LOG, "one function", f"{len(rows)} functions")]
    row = rows[0]
    public = [str(grant) for grant in _grants(row) if grant.role == _PUBLIC]
    checks = (
        (row.arguments == READ_AUDIT_LOG_ARGUMENTS, READ_AUDIT_LOG_ARGUMENTS, str(row.arguments)),
        (row.result == READ_AUDIT_LOG_RESULT, READ_AUDIT_LOG_RESULT, str(row.result)),
        (row.language == READ_AUDIT_LOG_LANGUAGE, READ_AUDIT_LOG_LANGUAGE, str(row.language)),
        (row.volatility == READ_AUDIT_LOG_VOLATILITY, "stable", f"provolatile {row.volatility}"),
        (bool(row.owned), "owned by the schema's owner", "another owner"),
        (bool(row.definer), "security definer", "security invoker"),
        (list(row.config) == [row.search_path], str([row.search_path]), str(list(row.config))),
        (not public, "no PUBLIC grant", ", ".join(public)),
    )
    return [
        Problem(_READER_SHAPE, READ_AUDIT_LOG, wanted, got)
        for passed, wanted, got in checks
        if not passed
    ]


@dataclass(frozen=True, slots=True)
class _Grant:
    """One ``ROLE:PRIVILEGE[ (grantable)]`` entry of ``AUDIT_READER``'s grants."""

    role: str
    privilege: str
    grantable: bool

    def __str__(self) -> str:
        return f"{self.role}:{self.privilege}{' (grantable)' if self.grantable else ''}"


def _grants(row: Any) -> list[_Grant]:
    """Parse the grants of a reader row; the role is everything before the last colon."""
    return [_grant(str(grant)) for grant in row.grants]


def _grant(entry: str) -> _Grant:
    role, _, rest = entry.rpartition(":")
    privilege, _, option = rest.partition(" ")
    return _Grant(role, privilege, option == "(grantable)")


async def _read_problems(
    connection: AsyncConnection, shape: Sequence[str], parameters: dict[str, str]
) -> list[Problem]:
    """A problem when the connecting role can read an existing audit log."""
    if shape and (await connection.execute(_AUDIT_READABLE, parameters)).scalar():
        return [Problem("audit.read", _AUDIT_LOG, "only the auditor reads", "readable")]
    return []


async def _trigger_problems(
    connection: AsyncConnection, schema: str, recording: Sequence[str]
) -> list[Problem]:
    """A problem per table in *recording* without its enabled audit trigger."""
    if not recording:
        return []
    rows = await connection.execute(_AUDIT_TRIGGERS, {_SCHEMA_PARAM: schema})
    present = {str(row.relname) for row in rows}
    return [
        Problem("audit.trigger", name, f"{AUDIT_TRIGGER} enabled", _MISSING)
        for name in recording
        if name not in present
    ]


async def audit_months_covered(
    connection: AsyncConnection, schema: str | None = None
) -> int | None:
    """Consecutive UTC months from this one the log has partitions for; ``None`` without a log.

    *schema* defaults to the connection's current schema, the one startup lands in.
    """
    covered = (await connection.execute(_AUDIT_MONTHS_COVERED, {_SCHEMA_PARAM: schema})).scalar()
    return None if covered is None else int(covered)


async def _older_than_audit(connection: AsyncConnection, guard: str) -> Problem | None:
    """A problem when the installed guard predates the audit log's revision."""
    if (await connection.execute(_AUDIT_SUPPORTED, _guard_parameters(guard))).scalar():
        return None
    return Problem("audit.guard_revision", guard, f"revision {AUDIT_GUARD_REVISION}", "older")


def _older_than_reading(guard: str) -> Problem:
    """The problem naming the reader for a guard that predates its revision (review N3)."""
    return Problem(
        "audit.reading_revision",
        guard,
        f"revision {READING_GUARD_REVISION}",
        f"older; guard revision {READING_GUARD_REVISION} installs {READ_AUDIT_LOG}",
    )


def audit_shape_problems(lines: Sequence[str]) -> list[Problem]:
    """Compare the audit log's columns, constraints and indexes with the pinned shape.

    Empty when they match; the problem lists only the lines that differ.
    """
    if not lines:
        return [Problem(_AUDIT_SHAPE_CHECK, _AUDIT_LOG, "the pinned shape", _MISSING)]
    if tuple(lines) == AUDIT_LOG_SHAPE:
        return []
    missing = _absent(AUDIT_LOG_SHAPE, lines)
    extra = _absent(lines, AUDIT_LOG_SHAPE)
    if not (missing or extra):
        return [Problem(_AUDIT_SHAPE_CHECK, _AUDIT_LOG, "the pinned order", "reordered")]
    return [Problem(_AUDIT_SHAPE_CHECK, _AUDIT_LOG, missing or "nothing else", extra or "none")]


def _absent(wanted: Sequence[str], held: Sequence[str]) -> str:
    """The lines of *wanted* that *held* lacks, joined; empty when none."""
    return "; ".join(line for line in wanted if line not in held)


async def table_problems(
    connection: AsyncConnection,
    guard: str,
    schema: str,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
) -> list[Problem]:
    """Compare every scoped model table with its canonical protection in the catalogue."""
    parameters = {
        **_guard_parameters(guard),
        _SCHEMA_PARAM: schema,
        "trigger": OWNER_TRIGGER,
        "names": [table.name for table in scoped.values()],
    }
    found = {str(row.name): row for row in await connection.execute(_SCOPED_TABLES, parameters)}
    problems: list[Problem] = []
    for table in scoped.values():
        row = found.get(table.name)
        if row is None:
            problems.append(Problem("table.missing", table.name, "present", _MISSING))
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


async def require_guard_revision(
    connection: AsyncConnection, guard: str, *, minimum: int | None = None
) -> None:
    """Refuse to call a guard below ``minimum``, by default the minimum compatible revision.

    Raises:
        ConfigError: When the guard is unknown or its revision is pending.
    """
    rows = await connection.execute(_CATALOG, _catalog_parameters(guard, (FUNCTIONS,)))
    number = revision_of(catalog_digest(str(row.line) for row in rows))
    require_revision(number, guard, minimum=_minimum(minimum))


class SyncExecutor(Protocol):
    """A synchronous connection, or the bind Alembic hands an operation."""

    def execute(self, statement: Any, parameters: Any = ..., /) -> Any: ...


def require_guard_revision_sync(
    connection: SyncExecutor, guard: str, *, minimum: int | None = None
) -> None:
    """The synchronous :func:`require_guard_revision`, for the migration runners.

    Raises:
        ConfigError: When the guard is unknown or its revision is pending.
    """
    rows = connection.execute(_CATALOG, _catalog_parameters(guard, (FUNCTIONS,)))
    number = revision_of(catalog_digest(str(row.line) for row in rows))
    require_revision(number, guard, minimum=_minimum(minimum))


def _minimum(minimum: int | None) -> int:
    return (
        MIN_COMPATIBLE_GUARD_REVISION
        if minimum is None
        else max(minimum, MIN_COMPATIBLE_GUARD_REVISION)
    )


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
