"""Alembic operations for the guard, so generated revisions carry calls and never SQL.

A revision reads ``op.protect_scoped_table("notes", scopes, privileges)``; the
implementation executes a constant statement with bound parameters. The
schema is the one the runner configured as the version table schema.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from alembic.operations import MigrateOperation, Operations
from sqlalchemy import text

from loom.core.backend.scoped_ddl import (
    GRANT_TABLE,
    OPEN_HATCH,
    PROTECT,
    UNPROTECT,
    table_parameters,
)
from loom.core.config import ConfigError

_IMPORT = "import loom.core.repository.sqlalchemy.migrations.operations"
_OPEN_HATCH = text(OPEN_HATCH)
_PROTECT = text(PROTECT)
_UNPROTECT = text(UNPROTECT)
_GRANT_TABLE = text(GRANT_TABLE)


@Operations.register_operation("open_hatch")
class OpenHatchOp(MigrateOperation):
    """Open the guard's hatch for the rest of the revision's transaction."""

    @classmethod
    def open_hatch(cls, operations: Operations) -> None:
        operations.invoke(cls())

    def reverse(self) -> OpenHatchOp:
        return OpenHatchOp()

    def to_diff_tuple(self) -> tuple[str]:
        return ("open_hatch",)


@Operations.register_operation("protect_scoped_table")
class ProtectScopedTableOp(MigrateOperation):
    """Protect a scoped table of the application schema."""

    def __init__(self, table: str, scopes: Sequence[dict[str, Any]], privileges: Sequence[str]):
        self.table = table
        self.scopes = list(scopes)
        self.privileges = list(privileges)

    @classmethod
    def protect_scoped_table(
        cls,
        operations: Operations,
        table: str,
        scopes: Sequence[dict[str, Any]],
        privileges: Sequence[str],
    ) -> None:
        operations.invoke(cls(table, scopes, privileges))

    def reverse(self) -> UnprotectScopedTableOp:
        return UnprotectScopedTableOp(self.table, self.scopes, self.privileges)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("protect_scoped_table", self.table)


@Operations.register_operation("unprotect_scoped_table")
class UnprotectScopedTableOp(MigrateOperation):
    """Unprotect a scoped table of the application schema; needs the open hatch."""

    def __init__(
        self,
        table: str,
        scopes: Sequence[dict[str, Any]] = (),
        privileges: Sequence[str] = (),
    ):
        self.table = table
        self.scopes = list(scopes)
        self.privileges = list(privileges)

    @classmethod
    def unprotect_scoped_table(cls, operations: Operations, table: str) -> None:
        operations.invoke(cls(table))

    def reverse(self) -> ProtectScopedTableOp:
        return ProtectScopedTableOp(self.table, self.scopes, self.privileges)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("unprotect_scoped_table", self.table)


@Operations.register_operation("grant_table")
class GrantTableOp(MigrateOperation):
    """Grant the declared privileges of an unscoped table to the schema's groups."""

    def __init__(self, table: str, readers: Sequence[str], writers: Sequence[str]):
        self.table = table
        self.readers = list(readers)
        self.writers = list(writers)

    @classmethod
    def grant_table(
        cls, operations: Operations, table: str, readers: Sequence[str], writers: Sequence[str]
    ) -> None:
        operations.invoke(cls(table, readers, writers))

    def reverse(self) -> GrantTableOp:
        return GrantTableOp(self.table, (), ())

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("grant_table", self.table)


@Operations.implementation_for(OpenHatchOp)
def _open_hatch(operations: Operations, _op: OpenHatchOp) -> None:
    operations.get_bind().execute(_OPEN_HATCH)


@Operations.implementation_for(ProtectScopedTableOp)
def _protect(operations: Operations, op: ProtectScopedTableOp) -> None:
    parameters = {
        **_table(operations, op.table),
        "scopes": json.dumps(op.scopes),
        "privileges": op.privileges,
    }
    operations.get_bind().execute(_PROTECT, parameters)


@Operations.implementation_for(UnprotectScopedTableOp)
def _unprotect(operations: Operations, op: UnprotectScopedTableOp) -> None:
    operations.get_bind().execute(_UNPROTECT, _table(operations, op.table))


@Operations.implementation_for(GrantTableOp)
def _grant(operations: Operations, op: GrantTableOp) -> None:
    parameters = {**_table(operations, op.table), "readers": op.readers, "writers": op.writers}
    operations.get_bind().execute(_GRANT_TABLE, parameters)


def _table(operations: Operations, table: str) -> dict[str, Any]:
    schema = operations.get_context().opts.get("version_table_schema")
    if not schema:
        raise ConfigError("guard operations run only through loom's run_migrations")
    return table_parameters(str(schema), table)


@renderers.dispatch_for(OpenHatchOp)
def _render_open_hatch(context: AutogenContext, _op: OpenHatchOp) -> str:
    context.imports.add(_IMPORT)
    return "op.open_hatch()"


@renderers.dispatch_for(ProtectScopedTableOp)
def _render_protect(context: AutogenContext, op: ProtectScopedTableOp) -> str:
    context.imports.add(_IMPORT)
    return f"op.protect_scoped_table({op.table!r}, {op.scopes!r}, {op.privileges!r})"


@renderers.dispatch_for(UnprotectScopedTableOp)
def _render_unprotect(context: AutogenContext, op: UnprotectScopedTableOp) -> str:
    context.imports.add(_IMPORT)
    return f"op.unprotect_scoped_table({op.table!r})"


@renderers.dispatch_for(GrantTableOp)
def _render_grant(context: AutogenContext, op: GrantTableOp) -> str:
    context.imports.add(_IMPORT)
    return f"op.grant_table({op.table!r}, {op.readers!r}, {op.writers!r})"
