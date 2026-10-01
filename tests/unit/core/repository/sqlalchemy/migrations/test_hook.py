from __future__ import annotations

from alembic.operations import ops
from sqlalchemy import Column, Integer, MetaData, Table, Text

from loom.core.backend.scoped_ddl import (
    SCHEMA_KEY,
    grant_statements,
    hatch_statement,
    protect_statement,
    unprotect_statement,
)
from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.locator import Application, DatabaseConfig, SchemaConfig
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.types import Integer as LoomInteger
from loom.core.model.types import String
from loom.core.model.types import Text as LoomText
from loom.core.repository.sqlalchemy.migrations.hook import (
    scope_protection_hook,
)
from loom.core.repository.sqlalchemy.rls import BootstrapConfig, DatabaseRoles, DatabaseUser

SCHEMA = "s1"


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    key: str = ScopedField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(LoomInteger, primary_key=True, autoincrement=True)
    editor: str = ScopedField(LoomText, scope="editor", on="write", elevable=True)
    body: str = ColumnField(LoomText)


class Kind(BaseModel):
    __tablename__ = "kinds"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(LoomInteger, primary_key=True, autoincrement=True)
    name: str = ColumnField(LoomText)


def _application() -> Application:
    metadata = MetaData()
    metadata.info[SCHEMA_KEY] = SCHEMA
    compile_all(Note, Kind, metadata=metadata)
    return Application(
        models=(Note, Kind),
        metadata=metadata,
        database=DatabaseConfig(
            url="postgresql+asyncpg://u:p@localhost/db",
            schema=SchemaConfig(mode="external", name=SCHEMA),
        ),
        bootstrap=BootstrapConfig(
            schema=SCHEMA,
            roles=DatabaseRoles(owner="s1_owner", migrator="s1_migrator"),
            database_users={"s1_rw": DatabaseUser(login=True, access="write")},
        ),
        scoped=scoped_tables(metadata),
        scope_sources={"holder": "identity.subject", "editor": "request.editor"},
    )


def _script(
    upgrade: list[ops.MigrateOperation], downgrade: list[ops.MigrateOperation] | None = None
) -> ops.MigrationScript:
    return ops.MigrationScript(
        "abc123", ops.UpgradeOps(upgrade), ops.DowngradeOps(downgrade or []), message="t"
    )


def _rewrite(application: Application, script: ops.MigrationScript) -> ops.MigrationScript:
    hook = scope_protection_hook(application)
    hook(None, ("head",), [script])
    return script


def _sql(operations: list[ops.MigrateOperation]) -> list[str]:
    rendered: list[str] = []
    for op in operations:
        if isinstance(op, ops.ExecuteSQLOp):
            rendered.append(str(op.sqltext))
        else:
            rendered.append(type(op).__name__)
    return rendered


def test_creating_a_scoped_table_runs_under_the_hatch_and_protects_it() -> None:
    application = _application()
    table = application.metadata.tables["notes"]
    scoped = application.scoped[(None, "notes")]
    script = _script([ops.CreateTableOp.from_table(table)], [ops.DropTableOp.from_table(table)])

    _rewrite(application, script)

    assert _sql(script.upgrade_ops.ops) == [
        hatch_statement(SCHEMA),
        "CreateTableOp",
        protect_statement(SCHEMA, "notes", scoped),
    ]
    assert _sql(script.downgrade_ops.ops) == [
        hatch_statement(SCHEMA),
        unprotect_statement(SCHEMA, "notes"),
        "DropTableOp",
    ]


def test_creating_a_global_table_with_privileges_emits_its_grants() -> None:
    application = _application()
    table = application.metadata.tables["kinds"]
    script = _script([ops.CreateTableOp.from_table(table)])

    _rewrite(application, script)

    assert _sql(script.upgrade_ops.ops) == [
        "CreateTableOp",
        *grant_statements(
            SCHEMA, "kinds", {"readers": frozenset({Privilege.SELECT})}, serial_columns=("id",)
        ),
    ]


def test_changing_a_scope_column_reprotects_the_table_in_both_directions() -> None:
    application = _application()
    column = Column("editor", Text(), nullable=False)
    upgrade = ops.ModifyTableOps("notes", [ops.AddColumnOp("notes", column)])
    downgrade = ops.ModifyTableOps("notes", [ops.DropColumnOp("notes", "editor")])
    script = _script([upgrade], [downgrade])

    _rewrite(application, script)

    protect = protect_statement(SCHEMA, "notes", application.scoped[(None, "notes")])
    expected = [
        hatch_statement(SCHEMA),
        unprotect_statement(SCHEMA, "notes"),
        "ModifyTableOps",
        protect,
    ]
    assert _sql(script.upgrade_ops.ops) == expected
    assert _sql(script.downgrade_ops.ops) == expected


def test_changing_an_ordinary_column_of_a_scoped_table_is_left_alone() -> None:
    application = _application()
    modify = ops.ModifyTableOps("notes", [ops.AddColumnOp("notes", Column("extra", Integer()))])
    script = _script([modify])

    _rewrite(application, script)

    assert _sql(script.upgrade_ops.ops) == ["ModifyTableOps"]


def test_dropping_a_scoped_table_unprotects_it_first() -> None:
    application = _application()
    script = _script([ops.DropTableOp.from_table(application.metadata.tables["notes"])])

    _rewrite(application, script)

    assert _sql(script.upgrade_ops.ops) == [
        hatch_statement(SCHEMA),
        unprotect_statement(SCHEMA, "notes"),
        "DropTableOp",
    ]


def test_an_unscoped_table_without_privileges_is_untouched() -> None:
    application = _application()
    plain = Table("plain", MetaData(), Column("id", Integer(), primary_key=True))
    script = _script([ops.CreateTableOp.from_table(plain)])

    _rewrite(application, script)

    assert _sql(script.upgrade_ops.ops) == ["CreateTableOp"]


def test_the_hook_never_opens_a_database_connection() -> None:
    application = _application()
    script = _script([ops.CreateTableOp.from_table(application.metadata.tables["notes"])])

    class Context:
        @property
        def connection(self):
            raise AssertionError("the hook must not reach the database")

    scope_protection_hook(application)(Context(), ("head",), [script])
