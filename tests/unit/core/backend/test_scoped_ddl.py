from __future__ import annotations

import inspect
import json
from typing import Any

import pytest
from sqlalchemy import MetaData, Table, create_engine

from loom.core.backend.scoped_ddl import (
    ASSERT_SCHEMA,
    ENTER_GUARD,
    GRANT_TABLE,
    MISSING_EVENT_TRIGGERS,
    OPEN_HATCH,
    PROTECT,
    SCHEMA_KEY,
    UNPROTECT,
    check_dialect,
    grant_parameters,
    privilege_names,
    protect_parameters,
    scope_documents,
    table_parameters,
)
from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.scoped import ScopeColumn, ScopedTable
from loom.core.model.types import Integer, String, Text


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    key: str = ScopedField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ScopedField(Text, scope="editor", on="write", elevable=True)


class Kind(BaseModel):
    __tablename__ = "kinds"
    __privileges__ = {
        "readers": frozenset({Privilege.SELECT}),
        "writers": frozenset({Privilege.INSERT}),
    }
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    name: str = ColumnField(Text)


KIND_PRIVILEGES = {
    "readers": frozenset({Privilege.SELECT}),
    "writers": frozenset({Privilege.INSERT}),
}


def _metadata(schema: str = "s1") -> MetaData:
    metadata = MetaData()
    metadata.info[SCHEMA_KEY] = schema
    return metadata


def _metadata_of(model: type) -> MetaData:
    metadata = _metadata()
    compile_all(model, metadata=metadata)
    return metadata


def _ddl(table: Table, event: str) -> list[tuple[str, dict[str, Any]]]:
    calls = []
    for listener in getattr(table.dispatch, event):
        captured = inspect.getclosurevars(listener).nonlocals
        if "clause" in captured:
            calls.append((str(captured["clause"]), captured["parameters"]))
    return calls


def test_every_guard_call_is_a_constant_with_bound_parameters() -> None:
    assert OPEN_HATCH == "SELECT open_hatch()"
    assert ASSERT_SCHEMA == "SELECT assert_scoped_schema()"
    assert ":schema" in PROTECT and ":table" in PROTECT
    assert ":scopes" in PROTECT and ":privileges" in PROTECT
    assert ":schema" in UNPROTECT and ":table" in UNPROTECT
    assert ":readers" in GRANT_TABLE and ":writers" in GRANT_TABLE
    assert ":guard" in ENTER_GUARD
    assert ":guard" in MISSING_EVENT_TRIGGERS


def test_protect_passes_the_table_the_scopes_json_and_the_privileges() -> None:
    scoped = scoped_tables(_metadata_of(Note))[(None, "notes")]

    parameters = protect_parameters("s1", "notes", scoped)

    assert parameters["schema"] == "s1"
    assert parameters["table"] == "notes"
    assert json.loads(parameters["scopes"]) == [
        {"col": "key", "scope": "holder", "on": "both", "elevable": False},
        {"col": "editor", "scope": "editor", "on": "write", "elevable": True},
    ]
    assert parameters["privileges"] == ["SELECT", "INSERT", "UPDATE", "DELETE"]


def test_privileges_follow_the_guard_canonical_order() -> None:
    names = privilege_names(frozenset({Privilege.DELETE, Privilege.SELECT}))

    assert names == ["SELECT", "DELETE"]


def test_global_table_grants_go_to_the_groups_as_privilege_lists() -> None:
    parameters = grant_parameters("s1", "kinds", KIND_PRIVILEGES)

    assert parameters == {
        "schema": "s1",
        "table": "kinds",
        "readers": ["SELECT"],
        "writers": ["INSERT"],
    }


def test_a_group_without_privileges_gets_an_empty_list() -> None:
    parameters = grant_parameters("s1", "kinds", {"readers": frozenset({Privilege.SELECT})})

    assert parameters["writers"] == []


@pytest.mark.parametrize("bad", ["my-schema", "1st", "a.b", "drop table", "pg_x", "public"])
def test_bad_schema_names_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        table_parameters(bad, "notes")


def test_a_schema_longer_than_the_guard_allows_is_rejected() -> None:
    with pytest.raises(ValueError, match="at most 47"):
        table_parameters("s" * 48, "notes")


@pytest.mark.parametrize("bad", ["Notes", "a-b", "select"])
def test_bad_table_names_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        table_parameters("s1", bad)


def test_a_scope_column_that_is_not_an_identifier_is_rejected() -> None:
    scoped = ScopedTable(
        schema=None,
        name="notes",
        scopes=(ScopeColumn(column="Bad-Col", scope="holder", on="both", elevable=False),),
        privileges=frozenset({Privilege.SELECT}),
    )

    with pytest.raises(ValueError, match="identifier"):
        scope_documents(scoped)


def test_a_scope_with_an_unknown_reach_is_rejected() -> None:
    scoped = ScopedTable(
        schema=None,
        name="notes",
        scopes=(ScopeColumn(column="key", scope="holder", on="never", elevable=False),),
        privileges=frozenset({Privilege.SELECT}),
    )

    with pytest.raises(ValueError, match="reach"):
        scope_documents(scoped)


def test_compile_registers_hatch_and_protect_listeners_on_a_scoped_table() -> None:
    metadata = _metadata()

    compile_all(Note, metadata=metadata)

    table = metadata.tables["notes"]
    assert _ddl(table, "before_create") == [(OPEN_HATCH, {})]
    assert _ddl(table, "after_create") == [
        (PROTECT, protect_parameters("s1", "notes", scoped_tables(metadata)[(None, "notes")]))
    ]


def test_compile_registers_grant_listeners_on_a_global_table_with_privileges() -> None:
    metadata = _metadata()

    compile_all(Kind, metadata=metadata)

    assert _ddl(metadata.tables["kinds"], "after_create") == [
        (GRANT_TABLE, grant_parameters("s1", "kinds", KIND_PRIVILEGES))
    ]


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


class Ledger(BaseModel, RowScoped):
    __tablename__ = "ledger"
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


def test_listeners_emit_only_on_postgres() -> None:
    metadata = _metadata()
    compile_all(Ledger, metadata=metadata)
    engine = create_engine("sqlite://")

    metadata.create_all(engine)

    with engine.connect() as connection:
        assert "ledger" in engine.dialect.get_table_names(connection)
    assert _ddl(metadata.tables["ledger"], "after_create")[0][0] == PROTECT


def test_scoped_models_on_another_dialect_raise_unless_explicitly_allowed() -> None:
    metadata = _metadata()
    compile_all(Note, metadata=metadata)
    scoped = scoped_tables(metadata)

    with pytest.raises(ConfigError, match=r"sqlite.*notes"):
        check_dialect("sqlite", scoped, allow_unprotected=False)

    assert check_dialect("sqlite", scoped, allow_unprotected=True) == ("notes",)
    assert check_dialect("postgresql", scoped, allow_unprotected=False) == ()
