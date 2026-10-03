from __future__ import annotations

import datetime as dt

import pytest
from alembic.autogenerate import render_python_code
from alembic.operations import ops
from sqlalchemy import MetaData

from loom.core.backend.scoped_ddl import SCHEMA_KEY
from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.locator import Application, DatabaseConfig, SchemaConfig
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.types import DateTime, String, Text
from loom.core.repository.sqlalchemy.migrations.hook import scope_protection_hook
from loom.core.repository.sqlalchemy.migrations.operations import (
    DetachRangePartitionsOp,
    DropPartitionsOp,
    EnsureRangePartitionsOp,
    OpenHatchOp,
    ProtectScopedTableOp,
    UnprotectScopedTableOp,
)
from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    SchemaNames,
)

SCHEMA = "s1"


class Event(BaseModel, RowScoped):
    __tablename__ = "events"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    __partition_by__ = ("RANGE", "at")
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    kind: str = ColumnField(Text)


def _application() -> Application:
    metadata = MetaData()
    metadata.info[SCHEMA_KEY] = SCHEMA
    compile_all(Event, metadata=metadata)
    return Application(
        models=(Event,),
        metadata=metadata,
        database=DatabaseConfig(
            url="postgresql+asyncpg://u:p@localhost/db",
            schema=SchemaConfig(mode="external", name=SCHEMA),
        ),
        bootstrap=BootstrapConfig(
            schema=SCHEMA,
            roles=DatabaseRoles(owner="s1_owner", migrator="s1_migrator"),
            database_users={"s1_rw": DatabaseUser(login=True, access="write")},
            names=SchemaNames.derived(SCHEMA),
        ),
        scoped=scoped_tables(metadata),
        scope_sources={"owner": "identity.subject"},
    )


def _rewritten(
    upgrade: list[ops.MigrateOperation], downgrade: list[ops.MigrateOperation]
) -> ops.MigrationScript:
    script = ops.MigrationScript(
        "abc123", ops.UpgradeOps(upgrade), ops.DowngradeOps(downgrade), message="t"
    )
    scope_protection_hook(_application())(None, ("head",), [script])
    return script


def _kinds(operations: list[ops.MigrateOperation]) -> list[type]:
    return [type(op) for op in operations]


def test_creating_a_partitioned_table_renders_its_partitioning_under_the_hatch() -> None:
    table = _application().metadata.tables["events"]

    script = _rewritten([ops.CreateTableOp.from_table(table)], [ops.DropTableOp.from_table(table)])

    assert _kinds(script.upgrade_ops.ops) == [OpenHatchOp, ops.CreateTableOp, ProtectScopedTableOp]
    assert "postgresql_partition_by='RANGE (at)'" in render_python_code(script.upgrade_ops)


def test_dropping_a_partitioned_table_drops_its_partitions_through_the_guard_first() -> None:
    table = _application().metadata.tables["events"]

    script = _rewritten([ops.CreateTableOp.from_table(table)], [ops.DropTableOp.from_table(table)])

    assert _kinds(script.downgrade_ops.ops) == [
        OpenHatchOp,
        DropPartitionsOp,
        UnprotectScopedTableOp,
        ops.DropTableOp,
    ]
    code = render_python_code(script.downgrade_ops)
    assert "op.drop_partitions('events')" in code


def test_partition_operations_render_as_loom_operations() -> None:
    upgrade = ops.UpgradeOps(
        [
            EnsureRangePartitionsOp("events", dt.date(2026, 1, 1), "2027-01-01"),
            DetachRangePartitionsOp("events", "2025-01-01", "2025-02-01", "month", drop=True),
        ]
    )

    code = render_python_code(upgrade)

    assert (
        "op.ensure_range_partitions('events', '2026-01-01', '2027-01-01', interval='month')" in code
    )
    assert (
        "op.detach_range_partitions('events', '2025-01-01', '2025-02-01', interval='month', "
        "drop=True)" in code
    )
    assert "op.execute" not in code


def test_ensuring_partitions_has_no_reverse_because_it_cannot_tell_which_it_created() -> None:
    ensure = EnsureRangePartitionsOp("events", "2026-01-01", "2026-03-01", "day")

    with pytest.raises(NotImplementedError, match=r"op\.detach_range_partitions\('events'"):
        ensure.reverse()


def test_an_unknown_interval_is_refused_when_the_operation_is_built() -> None:
    with pytest.raises(ValueError, match="interval 'week'"):
        EnsureRangePartitionsOp("events", "2026-01-01", "2026-03-01", "week")  # type: ignore[arg-type]
