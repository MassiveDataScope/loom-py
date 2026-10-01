"""Calls to the guard made next to a table's creation, so it is protected in the same transaction.

Every statement is a constant; the schema, table, scopes and privileges travel
as bound parameters and Postgres quotes them. loom names nothing. The
listeners fire only on Postgres, so another dialect creates plain tables.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any, get_args

from sqlalchemy import Connection, Table, TextClause, event, text
from sqlalchemy.dialects.postgresql.base import RESERVED_WORDS

from loom.core.config import ConfigError
from loom.core.model.field import Reach
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import ScopedTable

SCHEMA_KEY = "loom.schema"
POSTGRES_DIALECT = "postgresql"
_REGISTERED = "loom.scoped_ddl"
MAX_IDENTIFIER_LENGTH = 63
MAX_SCHEMA_LENGTH = 47
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_SPECIAL_ROLES = frozenset({"public", "none", "current_role", "current_user", "session_user"})
_ORDER = (Privilege.SELECT, Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE)

OPEN_HATCH = "SELECT open_hatch()"
PROTECT = (
    "SELECT protect_scoped_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:scopes AS jsonb), CAST(:privileges AS text[]))"
)
UNPROTECT = (
    "SELECT unprotect_scoped_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)))"
)
GRANT_TABLE = (
    "SELECT grant_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:readers AS text[]), CAST(:writers AS text[]))"
)
ASSERT_SCHEMA = "SELECT assert_scoped_schema()"
ENTER_GUARD = "SELECT set_config('search_path', quote_ident(:guard) || ', pg_catalog', true)"
MISSING_EVENT_TRIGGERS = (
    "SELECT t.name FROM (VALUES (:guard || '_ddl', 'ddl_command_end', 'on_ddl_end'), "
    "(:guard || '_drop', 'sql_drop', 'on_sql_drop')) AS t(name, event, handler) "
    "WHERE NOT EXISTS (SELECT 1 FROM pg_event_trigger e JOIN pg_roles o ON o.oid = e.evtowner "
    "JOIN pg_proc p ON p.oid = e.evtfoid JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE e.evtname = t.name AND e.evtevent = t.event AND e.evtenabled = 'A' "
    "AND e.evttags IS NULL AND o.rolsuper AND n.nspname = :guard AND p.proname = t.handler "
    "AND p.pronargs = 0)"
)

Parameters = dict[str, Any]
_OPEN_HATCH = text(OPEN_HATCH)
_PROTECT = text(PROTECT)
_GRANT_TABLE = text(GRANT_TABLE)


def scope_documents(scoped: ScopedTable) -> list[dict[str, Any]]:
    """The scopes of ``scoped`` as the guard reads them, after validating each column and reach."""
    for scope in scoped.scopes:
        sql_identifier(scope.column)
        if scope.on not in get_args(Reach):
            raise ValueError(
                f"scope {scope.scope!r} has reach {scope.on!r}; expected one of {get_args(Reach)}"
            )
    return [
        {"col": s.column, "scope": s.scope, "on": s.on, "elevable": s.elevable}
        for s in scoped.scopes
    ]


def privilege_names(privileges: frozenset[Privilege]) -> list[str]:
    """The privileges in the guard's canonical order."""
    return [p.value for p in _ORDER if p in privileges]


def protect_parameters(schema: str, table: str, scoped: ScopedTable) -> Parameters:
    """Bound parameters of :data:`PROTECT` for ``schema.table``."""
    return {
        **table_parameters(schema, table),
        "scopes": json.dumps(scope_documents(scoped)),
        "privileges": privilege_names(scoped.privileges),
    }


def grant_parameters(
    schema: str, table: str, privileges: Mapping[str, frozenset[Privilege]]
) -> Parameters:
    """Bound parameters of :data:`GRANT_TABLE` for an unscoped table."""
    return {
        **table_parameters(schema, table),
        "readers": privilege_names(privileges.get("readers", frozenset())),
        "writers": privilege_names(privileges.get("writers", frozenset())),
    }


def table_parameters(schema: str, table: str) -> Parameters:
    """The validated schema and table a guard call resolves inside Postgres."""
    return {"schema": schema_identifier(schema), "table": sql_identifier(table)}


def check_dialect(
    dialect: str, scoped: Mapping[tuple[str | None, str], ScopedTable], *, allow_unprotected: bool
) -> tuple[str, ...]:
    """Return the scoped tables a non-Postgres dialect leaves unprotected, or raise.

    Raises:
        ConfigError: When scoped tables exist on a non-Postgres dialect and the
            product did not opt in with ``allow_unprotected_dialect``.
    """
    if dialect == POSTGRES_DIALECT or not scoped:
        return ()
    names = tuple(sorted(table.name for table in scoped.values()))
    if not allow_unprotected:
        raise ConfigError(
            f"dialect {dialect!r} cannot protect scoped tables {', '.join(names)}; "
            "set database.schema.allow_unprotected_dialect: true only for tests"
        )
    return names


def register_listeners(
    table: Table,
    *,
    schema: str,
    scoped: ScopedTable | None,
    privileges: Mapping[str, frozenset[Privilege]],
) -> None:
    """Attach the Postgres-only listeners once per table."""
    if table.info.get(_REGISTERED):
        return
    table.info[_REGISTERED] = True
    if scoped is not None:
        _listen(table, "before_create", _OPEN_HATCH, {})
        _listen(table, "after_create", _PROTECT, protect_parameters(schema, table.name, scoped))
        return
    if any(privileges.values()):
        parameters = grant_parameters(schema, table.name, privileges)
        _listen(table, "after_create", _GRANT_TABLE, parameters)


def _listen(table: Table, when: str, clause: TextClause, parameters: Parameters) -> None:
    event.listen(table, when, _postgres_only(clause, parameters))


def _postgres_only(clause: TextClause, parameters: Parameters) -> Callable[..., None]:
    def listener(_target: Table, connection: Connection, **_: Any) -> None:
        if connection.dialect.name == POSTGRES_DIALECT:
            connection.execute(clause, parameters)

    return listener


def sql_identifier(name: str, *, max_length: int = MAX_IDENTIFIER_LENGTH) -> str:
    """Return ``name`` when Postgres stores it exactly as written and resolves it to itself.

    Lowercase letters, digits and ``_``; at most ``max_length`` characters so
    Postgres never truncates it; not a reserved word, a special role name or a
    ``pg_`` name.

    Raises:
        ValueError: Naming the offending identifier.
    """
    if (
        not _IDENTIFIER.fullmatch(name)
        or len(name) > max_length
        or name in RESERVED_WORDS
        or name in _SPECIAL_ROLES
        or name.startswith("pg_")
    ):
        raise ValueError(
            f"{name!r} is not a usable SQL identifier: lowercase letters, digits and '_', "
            f"at most {max_length} characters, not a reserved word, special role or pg_ name"
        )
    return name


def schema_identifier(name: str) -> str:
    """Validate a scoped schema name; short enough that every guard object name fits."""
    return sql_identifier(name, max_length=MAX_SCHEMA_LENGTH)
