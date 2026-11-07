"""The ``Audited`` marker and the tables it compiles into."""

from __future__ import annotations

import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import audited_tables, compile_all
from loom.core.config import ConfigError
from loom.core.model import Audited, BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.audited import AuditedTable
from loom.core.model.types import Integer, Text


class Invoice(BaseModel, RowScoped, Audited):
    __tablename__ = "invoices"
    __audit_exclude__ = frozenset({"notes_blob"})
    tenant: int = ScopedField(Integer, primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True)
    amount: int = ColumnField(Integer)
    notes_blob: str = ColumnField(Text)
    api_token: str = ColumnField(Text)


class Note(BaseModel, Audited):
    __tablename__ = "notes"
    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text)


class Plain(BaseModel):
    __tablename__ = "plain"
    id: int = ColumnField(Integer, primary_key=True)


def _compiled(*models: type) -> MetaData:
    metadata = MetaData()
    compile_all(*models, metadata=metadata)
    return metadata


def test_audited_model_compiles_its_key_and_exclusions() -> None:
    audited = audited_tables(_compiled(Invoice))

    assert audited == {
        (None, "invoices"): AuditedTable(
            schema=None,
            name="invoices",
            key_columns=("tenant", "id"),
            excluded=frozenset({"notes_blob", "api_token"}),
        )
    }


def test_secret_columns_are_excluded_without_being_declared() -> None:
    audited = audited_tables(_compiled(Invoice))

    assert "api_token" in audited[(None, "invoices")].excluded


def test_audited_table_without_scopes_is_audited() -> None:
    audited = audited_tables(_compiled(Note))

    assert audited[(None, "notes")].excluded == frozenset()


def test_unmarked_model_is_not_audited() -> None:
    assert audited_tables(_compiled(Plain)) == {}


def test_excluding_a_column_that_does_not_exist_is_refused() -> None:
    class Typo(BaseModel, Audited):
        __tablename__ = "typo"
        __audit_exclude__ = frozenset({"bodyy"})
        id: int = ColumnField(Integer, primary_key=True)
        body: str = ColumnField(Text)

    with pytest.raises(ConfigError, match=r"typo.*bodyy"):
        _compiled(Typo)


def test_excluding_a_primary_key_column_is_refused() -> None:
    class KeyExcluded(BaseModel, Audited):
        __tablename__ = "key_excluded"
        __audit_exclude__ = frozenset({"id"})
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ConfigError, match=r"key_excluded.*id.*primary key"):
        _compiled(KeyExcluded)


def test_a_secret_primary_key_is_refused() -> None:
    class Keys(BaseModel, Audited):
        __tablename__ = "keys"
        api_key: str = ColumnField(Text, primary_key=True)

    with pytest.raises(ConfigError, match=r"keys.*api_key.*primary key"):
        _compiled(Keys)
