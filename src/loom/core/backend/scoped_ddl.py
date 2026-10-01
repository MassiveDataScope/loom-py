"""DDL emitted next to a table's creation so the guard protects it in the same transaction.

Every statement is rendered from the compiled model and the application schema
the product configured; loom names nothing. The listeners fire only on
Postgres, so another dialect creates plain tables.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence

from sqlalchemy import DDL, Table, event

from loom.core.config import ConfigError
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import ScopedTable

SCHEMA_KEY = "loom.schema"
_REGISTERED = "loom.scoped_ddl"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ORDER = (Privilege.SELECT, Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE)


def hatch_statement(schema: str) -> str:
    """Open the guard's transaction-local hatch so the table can be created before protection."""
    guard = _guard(schema)
    return f"SELECT set_config('{guard}.protecting', 'on', true)"


def protect_statement(schema: str, table: str, scoped: ScopedTable) -> str:
    """Call the guard's protection with the qualified table, its scopes and its privileges."""
    guard = _guard(schema)
    _identifier(table)
    scopes = json.dumps(
        [
            {"col": s.column, "scope": s.scope, "on": s.on, "elevable": s.elevable}
            for s in scoped.scopes
        ]
    )
    return (
        f"SELECT {guard}.protect_scoped_table('{schema}.{table}', '{scopes}', "
        f"ARRAY[{_privilege_list(scoped.privileges)}])"
    )


def grant_statements(
    schema: str,
    table: str,
    privileges: Mapping[str, frozenset[Privilege]],
    *,
    serial_columns: Sequence[str] = (),
) -> list[str]:
    """Plain grants for an unscoped table, one per group, plus sequence usage for inserters."""
    _identifier(schema)
    _identifier(table)
    statements: list[str] = []
    for group in ("readers", "writers"):
        granted = privileges.get(group, frozenset())
        if not granted:
            continue
        names = ", ".join(p.value for p in _ORDER if p in granted)
        statements.append(f"GRANT {names} ON {schema}.{table} TO {schema}_{group}")
        if Privilege.INSERT in granted:
            statements += [
                f"GRANT USAGE ON SEQUENCE {schema}.{table}_{column}_seq TO {schema}_{group}"
                for column in serial_columns
            ]
    return statements


def check_dialect(
    dialect: str, scoped: Mapping[tuple[str | None, str], ScopedTable], *, allow_unprotected: bool
) -> tuple[str, ...]:
    """Return the scoped tables a non-Postgres dialect leaves unprotected, or raise.

    Raises:
        ConfigError: When scoped tables exist on a non-Postgres dialect and the
            product did not opt in with ``allow_unprotected_dialect``.
    """
    if dialect == "postgresql" or not scoped:
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
    serial_columns: Sequence[str],
) -> None:
    """Attach the Postgres-only DDL listeners once per table."""
    if table.info.get(_REGISTERED):
        return
    table.info[_REGISTERED] = True
    if scoped is not None:
        _listen(table, "before_create", hatch_statement(schema))
        _listen(table, "after_create", protect_statement(schema, table.name, scoped))
        return
    for statement in grant_statements(
        schema, table.name, privileges, serial_columns=serial_columns
    ):
        _listen(table, "after_create", statement)


def _listen(table: Table, when: str, statement: str) -> None:
    ddl = DDL(statement)  # type: ignore[no-untyped-call]
    event.listen(table, when, ddl.execute_if(dialect="postgresql"))


def _guard(schema: str) -> str:
    return f"loom_guard_{_identifier(schema)}"


def _identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"{name!r} is not a plain SQL identifier")
    return name


def _privilege_list(privileges: frozenset[Privilege]) -> str:
    return ",".join(f"'{p.value}'" for p in _ORDER if p in privileges)
