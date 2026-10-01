from __future__ import annotations

import pytest
from loom.core.backend.scoped_ddl import (
    SCHEMA_KEY,
    check_dialect,
    grant_statements,
    hatch_statement,
    protect_statement,
)
from sqlalchemy import MetaData, create_engine
from sqlalchemy.dialects import postgresql

from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped
from loom.core.model.types import Integer, String, Text


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    key: str = ColumnField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ColumnField(Text, scope="editor", on="write", elevable=True)


class Kind(BaseModel):
    __tablename__ = "kinds"
    __privileges__ = {
        "readers": frozenset({Privilege.SELECT}),
        "writers": frozenset({Privilege.INSERT}),
    }
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    name: str = ColumnField(Text)


def _metadata(schema: str = "s1") -> MetaData:
    metadata = MetaData()
    metadata.info[SCHEMA_KEY] = schema
    return metadata


def _ddl(table, event: str) -> list[str]:
    statements = []
    for listener in table.dispatch[event]:
        target = getattr(listener, "__self__", listener)
        statement = getattr(target, "statement", None)
        if statement is not None:
            statements.append(str(statement))
    return statements


def test_the_hatch_names_the_guard_of_the_application_schema() -> None:
    assert hatch_statement("s1") == "SELECT set_config('loom_guard_s1.protecting', 'on', true)"


def test_protect_passes_the_qualified_table_the_scopes_json_and_the_privileges() -> None:
    scoped = scoped_tables(_metadata_of(Note))[(None, "notes")]

    statement = protect_statement("s1", "notes", scoped)

    assert statement.startswith("SELECT loom_guard_s1.protect_scoped_table('s1.notes', '[")
    assert '{"col": "key", "scope": "holder", "on": "both", "elevable": false}' in statement
    assert '{"col": "editor", "scope": "editor", "on": "write", "elevable": true}' in statement
    assert statement.endswith("ARRAY['SELECT','INSERT','UPDATE','DELETE'])")


def _metadata_of(model: type) -> MetaData:
    metadata = _metadata()
    compile_all(model, metadata=metadata)
    return metadata


def test_global_table_grants_go_to_the_groups_and_insert_brings_sequence_usage() -> None:
    statements = grant_statements(
        "s1",
        "kinds",
        {"readers": frozenset({Privilege.SELECT}), "writers": frozenset({Privilege.INSERT})},
        serial_columns=("id",),
    )

    assert statements == [
        "GRANT SELECT ON s1.kinds TO s1_readers",
        "GRANT INSERT ON s1.kinds TO s1_writers",
        "GRANT USAGE ON SEQUENCE s1.kinds_id_seq TO s1_writers",
    ]


@pytest.mark.parametrize("bad", ["my-schema", "1st", "a.b", "drop table"])
def test_bad_schema_names_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        hatch_statement(bad)


def test_compile_registers_hatch_and_protect_listeners_on_a_scoped_table() -> None:
    metadata = _metadata()

    compile_all(Note, metadata=metadata)

    table = metadata.tables["notes"]
    assert _ddl(table, "before_create") == [hatch_statement("s1")]
    assert _ddl(table, "after_create") == [
        protect_statement("s1", "notes", scoped_tables(metadata)[(None, "notes")])
    ]


def test_compile_registers_grant_listeners_on_a_global_table_with_privileges() -> None:
    metadata = _metadata()

    compile_all(Kind, metadata=metadata)

    assert _ddl(metadata.tables["kinds"], "after_create") == grant_statements(
        "s1",
        "kinds",
        {"readers": frozenset({Privilege.SELECT}), "writers": frozenset({Privilege.INSERT})},
        serial_columns=("id",),
    )


def test_repeated_compile_registers_the_listeners_once_per_metadata() -> None:
    metadata = _metadata()

    compile_all(Note, metadata=metadata)
    compile_all(Note, metadata=metadata)

    assert len(_ddl(metadata.tables["notes"], "after_create")) == 1


def test_without_a_schema_in_the_metadata_nothing_is_registered() -> None:
    metadata = MetaData()

    compile_all(Note, Kind, metadata=metadata)

    assert _ddl(metadata.tables["notes"], "after_create") == []
    assert _ddl(metadata.tables["kinds"], "after_create") == []


def test_listeners_emit_only_on_postgres() -> None:
    metadata = _metadata()
    compile_all(Note, metadata=metadata)
    engine = create_engine("sqlite://")

    metadata.create_all(engine)

    assert "notes" in metadata.tables
    listener = list(metadata.tables["notes"].dispatch.after_create)[0]
    compiled = str(listener.compile(dialect=postgresql.dialect()))
    assert compiled.startswith("SELECT loom_guard_s1")


def test_scoped_models_on_another_dialect_raise_unless_explicitly_allowed() -> None:
    metadata = _metadata()
    compile_all(Note, metadata=metadata)
    scoped = scoped_tables(metadata)

    with pytest.raises(ConfigError, match=r"sqlite.*notes"):
        check_dialect("sqlite", scoped, allow_unprotected=False)

    assert check_dialect("sqlite", scoped, allow_unprotected=True) == ("notes",)
    assert check_dialect("postgresql", scoped, allow_unprotected=False) == ()
