"""Alembic operations for the guard, so generated revisions carry calls and never SQL.

A revision reads ``op.protect_scoped_table("notes", scopes, privileges)``; the
implementation executes a constant statement with bound parameters. The
schema is the one the runner configured as the version table schema.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Sequence
from typing import Any, Final, Protocol

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from alembic.operations import MigrateOperation, Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.core.backend.scoped_ddl import (
    APP_FIRST,
    CREATE_RANGE_PARTITION,
    DETACH_PARTITION,
    GRANT_TABLE,
    GUARD_FIRST,
    OPEN_HATCH,
    PARTITIONS,
    PROTECT,
    UNPROTECT,
    create_partition_parameters,
    detach_partition_parameters,
    table_parameters,
)
from loom.core.config import ConfigError
from loom.core.model.partition import Interval, as_date, range_partitions, validate_interval
from loom.core.repository.sqlalchemy.rls.guard_manifest import PARTITION_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import require_guard_revision_sync

_IMPORT = "import loom.core.repository.sqlalchemy.migrations.operations"
_OPEN_HATCH: Final = text(OPEN_HATCH)
_PROTECT: Final = text(PROTECT)
_UNPROTECT: Final = text(UNPROTECT)
_GRANT_TABLE: Final = text(GRANT_TABLE)
_GUARD_FIRST: Final = text(GUARD_FIRST)
_APP_FIRST: Final = text(APP_FIRST)
_CREATE_RANGE_PARTITION: Final = text(CREATE_RANGE_PARTITION)
_DETACH_PARTITION: Final = text(DETACH_PARTITION)
_PARTITIONS: Final = text(PARTITIONS)
GUARD_OPTION: Final = "loom_guard"


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


@Operations.register_operation("ensure_range_partitions")
class EnsureRangePartitionsOp(MigrateOperation):
    """Create the missing partitions of a range-partitioned scoped table covering ``[start, end)``.

    The guard creates, registers and protects each partition in the
    revision's transaction; one that already exists is left alone.
    """

    def __init__(
        self,
        table: str,
        start: dt.date | str,
        end: dt.date | str,
        interval: Interval = "month",
    ):
        self.table = table
        self.start = as_date(start)
        self.end = as_date(end)
        self.interval: Interval = validate_interval(interval)

    @classmethod
    def ensure_range_partitions(
        cls,
        operations: Operations,
        table: str,
        start: dt.date | str,
        end: dt.date | str,
        *,
        interval: Interval = "month",
    ) -> None:
        operations.invoke(cls(table, start, end, interval))

    def reverse(self) -> DetachRangePartitionsOp:
        return DetachRangePartitionsOp(self.table, self.start, self.end, self.interval, drop=True)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("ensure_range_partitions", self.table)


@Operations.register_operation("detach_range_partitions")
class DetachRangePartitionsOp(MigrateOperation):
    """Unprotect and detach, and with ``drop`` drop, the partitions covering ``[start, end)``."""

    def __init__(
        self,
        table: str,
        start: dt.date | str,
        end: dt.date | str,
        interval: Interval = "month",
        *,
        drop: bool = False,
    ):
        self.table = table
        self.start = as_date(start)
        self.end = as_date(end)
        self.interval: Interval = validate_interval(interval)
        self.drop = drop

    @classmethod
    def detach_range_partitions(
        cls,
        operations: Operations,
        table: str,
        start: dt.date | str,
        end: dt.date | str,
        *,
        interval: Interval = "month",
        drop: bool = False,
    ) -> None:
        operations.invoke(cls(table, start, end, interval, drop=drop))

    def reverse(self) -> EnsureRangePartitionsOp:
        return EnsureRangePartitionsOp(self.table, self.start, self.end, self.interval)

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("detach_range_partitions", self.table)


@Operations.register_operation("drop_partitions")
class DropPartitionsOp(MigrateOperation):
    """Unprotect, detach and drop every partition of a table, before the table is dropped."""

    def __init__(self, table: str):
        self.table = table

    @classmethod
    def drop_partitions(cls, operations: Operations, table: str) -> None:
        operations.invoke(cls(table))

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("drop_partitions", self.table)


class HandWrittenProtectOp(MigrateOperation):
    """Stop a downgrade where only the developer knows the scopes of the target revision."""

    def __init__(self, table: str):
        self.table = table

    def message(self) -> str:
        return (
            f"the downgrade of {self.table!r} must re-protect it with the scopes of the target "
            f"revision: replace this line with op.protect_scoped_table({self.table!r}, "
            "<scopes>, <privileges>) written by hand"
        )

    def to_diff_tuple(self) -> tuple[str, str]:
        return ("hand_written_protect", self.table)


class _Bind(Protocol):
    def execute(self, clause: Any, parameters: Any = ..., /) -> Any: ...


class _Context(Protocol):
    opts: dict[str, Any]


class GuardOperations(Protocol):
    """The part of Alembic's ``Operations`` a guard operation needs."""

    def get_bind(self) -> _Bind: ...

    def get_context(self) -> _Context: ...


def run_guard_operation(operations: GuardOperations, op: MigrateOperation) -> None:
    """Run one guard operation with the guard resolved before the application schema.

    The guard name comes from ``run_migrations``; outside it the operation is
    refused, so a revision can never reach a routine the application schema
    shadows.
    """
    opts = operations.get_context().opts
    schema, guard = opts.get("version_table_schema"), opts.get(GUARD_OPTION)
    if not schema or not guard:
        raise ConfigError("guard operations run only through loom's run_migrations")
    path = {"schema": str(schema), "guard": str(guard)}
    bind = operations.get_bind()
    if isinstance(op, _PARTITION_OPS):
        require_guard_revision_sync(bind, str(guard), minimum=PARTITION_GUARD_REVISION)
    bind.execute(_GUARD_FIRST, path)
    aborted = False
    try:
        _call_guard(bind, str(schema), op)
    except DBAPIError:
        aborted = True  # the rollback of the aborted transaction undoes the local path
        raise
    finally:
        if not aborted:
            bind.execute(_APP_FIRST, path)


def _call_guard(bind: _Bind, schema: str, op: MigrateOperation) -> None:
    if isinstance(op, _PARTITION_OPS):
        _call_partitions(bind, schema, op)
    elif isinstance(op, ProtectScopedTableOp):
        scopes = {"scopes": json.dumps(op.scopes), "privileges": op.privileges}
        bind.execute(_PROTECT, {**table_parameters(schema, op.table), **scopes})
    elif isinstance(op, UnprotectScopedTableOp):
        bind.execute(_UNPROTECT, table_parameters(schema, op.table))
    elif isinstance(op, GrantTableOp):
        groups = {"readers": op.readers, "writers": op.writers}
        bind.execute(_GRANT_TABLE, {**table_parameters(schema, op.table), **groups})
    else:
        bind.execute(_OPEN_HATCH)


def _call_partitions(bind: _Bind, schema: str, op: MigrateOperation) -> None:
    if isinstance(op, EnsureRangePartitionsOp):
        for partition in range_partitions(op.table, op.start, op.end, op.interval):
            parameters = create_partition_parameters(schema, op.table, partition)
            bind.execute(_CREATE_RANGE_PARTITION, parameters)
        return
    if isinstance(op, DetachRangePartitionsOp):
        partitions = range_partitions(op.table, op.start, op.end, op.interval)
        _detach(bind, schema, op.table, [p.name for p in partitions], drop=op.drop)
        return
    if isinstance(op, DropPartitionsOp):
        rows = bind.execute(_PARTITIONS, table_parameters(schema, op.table))
        _detach(bind, schema, op.table, [str(row[0]) for row in rows], drop=True)


def _detach(bind: _Bind, schema: str, table: str, names: Sequence[str], *, drop: bool) -> None:
    for name in names:
        bind.execute(_DETACH_PARTITION, detach_partition_parameters(schema, table, name, drop=drop))


_PARTITION_OPS: Final = (EnsureRangePartitionsOp, DetachRangePartitionsOp, DropPartitionsOp)


@Operations.implementation_for(OpenHatchOp)
@Operations.implementation_for(EnsureRangePartitionsOp)
@Operations.implementation_for(DetachRangePartitionsOp)
@Operations.implementation_for(DropPartitionsOp)
@Operations.implementation_for(ProtectScopedTableOp)
@Operations.implementation_for(UnprotectScopedTableOp)
@Operations.implementation_for(GrantTableOp)
def _guard_operation(operations: Operations, op: MigrateOperation) -> None:
    run_guard_operation(operations, op)


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


@renderers.dispatch_for(EnsureRangePartitionsOp)
def _render_ensure_partitions(context: AutogenContext, op: EnsureRangePartitionsOp) -> str:
    context.imports.add(_IMPORT)
    return (
        f"op.ensure_range_partitions({op.table!r}, {op.start.isoformat()!r}, "
        f"{op.end.isoformat()!r}, interval={op.interval!r})"
    )


@renderers.dispatch_for(DetachRangePartitionsOp)
def _render_detach_partitions(context: AutogenContext, op: DetachRangePartitionsOp) -> str:
    context.imports.add(_IMPORT)
    return (
        f"op.detach_range_partitions({op.table!r}, {op.start.isoformat()!r}, "
        f"{op.end.isoformat()!r}, interval={op.interval!r}, drop={op.drop!r})"
    )


@renderers.dispatch_for(DropPartitionsOp)
def _render_drop_partitions(context: AutogenContext, op: DropPartitionsOp) -> str:
    context.imports.add(_IMPORT)
    return f"op.drop_partitions({op.table!r})"


@renderers.dispatch_for(HandWrittenProtectOp)
def _render_hand_written_protect(_context: AutogenContext, op: HandWrittenProtectOp) -> str:
    return f"raise {NotImplementedError.__name__}({op.message()!r})"
