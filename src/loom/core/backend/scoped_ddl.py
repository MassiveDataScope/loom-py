"""Calls to the guard made next to a table's creation, so it is protected in the same transaction.

Every statement is a constant; the schema, table, scopes and privileges travel
as bound parameters and Postgres quotes them. loom names nothing. The
listeners fire only on Postgres, so another dialect creates plain tables.
Inside ``create_schema`` each guard call resolves the guard first and then
restores the application-first path the table DDL needs.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any, Final, get_args

from sqlalchemy import Connection, Table, event, text

from loom.core.config import ConfigError
from loom.core.model.field import Reach
from loom.core.model.partition import PartitionRange
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import ScopedTable
from loom.core.schema_names import schema_identifier, sql_identifier

SCHEMA_KEY: Final = "loom.schema"
GUARD_PATH: Final = "loom.guard_path"
POSTGRES_DIALECT: Final = "postgresql"
_REGISTERED: Final = "loom.scoped_ddl"
_TIMEOUT: Final = re.compile(r"^\d+(ms|s|min)?$")
_ORDER: Final = (Privilege.SELECT, Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE)

OPEN_HATCH: Final = "SELECT open_hatch()"
PROTECT: Final = (
    "SELECT protect_scoped_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:scopes AS jsonb), CAST(:privileges AS text[]))"
)
UNPROTECT: Final = (
    "SELECT unprotect_scoped_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)))"
)
GRANT_TABLE: Final = (
    "SELECT grant_table(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:readers AS text[]), CAST(:writers AS text[]))"
)
ASSERT_SCHEMA: Final = "SELECT assert_scoped_schema()"
CREATE_RANGE_PARTITION: Final = (
    "SELECT create_range_partition("
    "to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:partition AS text), CAST(:lower AS text), CAST(:upper AS text))"
)
DETACH_PARTITION: Final = (
    "SELECT detach_partition(to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)), "
    "CAST(:partition AS text), CAST(:drop AS boolean))"
)
PARTITIONS: Final = (
    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
    "WHERE i.inhparent = to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)) "
    "ORDER BY c.relname"
)
LANDING: Final = "SELECT current_user, current_schema()"
GUARD_FIRST: Final = (
    "SELECT set_config('search_path', quote_ident(:guard) || ', pg_catalog, pg_temp', true)"
)
APP_FIRST: Final = (
    "SELECT set_config('search_path', quote_ident(:schema) || ', ' || quote_ident(:guard), true)"
)
SCHEMA_LOCK: Final = "SELECT pg_advisory_xact_lock(hashtextextended('loom.schema:' || :schema, 0))"
LOCK_TIMEOUT: Final = "SELECT set_config('lock_timeout', :lock, true)"
MISSING_EVENT_TRIGGERS: Final = (
    "SELECT t.name FROM (VALUES (:guard || '_ddl', 'ddl_command_end', 'on_ddl_end'), "
    "(:guard || '_drop', 'sql_drop', 'on_sql_drop')) AS t(name, event, handler) "
    "WHERE NOT EXISTS (SELECT 1 FROM pg_event_trigger e "
    "JOIN pg_proc p ON p.oid = e.evtfoid JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE e.evtname = t.name AND e.evtevent = t.event AND e.evtenabled = 'A' "
    "AND e.evttags IS NULL AND e.evtowner = n.nspowner AND n.nspname = :guard "
    "AND p.proname = t.handler AND p.pronargs = 0)"
)

Parameters = dict[str, Any]
GuardCall = Callable[[Connection, Parameters], None]
_OPEN_HATCH: Final = text(OPEN_HATCH)
_PROTECT: Final = text(PROTECT)
_GRANT_TABLE: Final = text(GRANT_TABLE)
_GUARD_FIRST: Final = text(GUARD_FIRST)
_APP_FIRST: Final = text(APP_FIRST)


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


def create_partition_parameters(schema: str, table: str, partition: PartitionRange) -> Parameters:
    """Bound parameters of :data:`CREATE_RANGE_PARTITION` for one partition of ``schema.table``."""
    return {
        **table_parameters(schema, table),
        "partition": sql_identifier(partition.name),
        "lower": partition.lower,
        "upper": partition.upper,
    }


def detach_partition_parameters(
    schema: str, table: str, partition: str, *, drop: bool
) -> Parameters:
    """Bound parameters of :data:`DETACH_PARTITION` for one partition of ``schema.table``."""
    return {**table_parameters(schema, table), "partition": sql_identifier(partition), "drop": drop}


def table_parameters(schema: str, table: str) -> Parameters:
    """The validated schema and table a guard call resolves inside Postgres."""
    return {"schema": schema_identifier(schema), "table": sql_identifier(table)}


def validate_timeout(value: str) -> str:
    """Accept a Postgres duration literal such as ``5s``, ``250ms`` or ``2min``."""
    if not _TIMEOUT.fullmatch(value):
        raise ValueError(f"timeout must be a Postgres duration like '5s', got {value!r}")
    return value


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
        event.listen(table, "before_create", _postgres_only(_open_hatch, {}))
        parameters = protect_parameters(schema, table.name, scoped)
        event.listen(table, "after_create", _postgres_only(_protect, parameters))
        return
    if any(privileges.values()):
        parameters = grant_parameters(schema, table.name, privileges)
        event.listen(table, "after_create", _postgres_only(_grant_table, parameters))


def _open_hatch(connection: Connection, parameters: Parameters) -> None:
    connection.execute(_OPEN_HATCH, parameters)


def _protect(connection: Connection, parameters: Parameters) -> None:
    connection.execute(_PROTECT, parameters)


def _grant_table(connection: Connection, parameters: Parameters) -> None:
    connection.execute(_GRANT_TABLE, parameters)


def _postgres_only(call: GuardCall, parameters: Parameters) -> Callable[..., None]:
    def listener(_target: Table, connection: Connection, **_: Any) -> None:
        if connection.dialect.name != POSTGRES_DIALECT:
            return
        path = connection.info.get(GUARD_PATH)
        if path is None:
            call(connection, parameters)
            return
        connection.execute(_GUARD_FIRST, path)
        call(connection, parameters)
        connection.execute(_APP_FIRST, path)

    return listener
