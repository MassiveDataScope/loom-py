"""A ``UUID`` annotation without a declared type compiles to a UUID column."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import CreateTable

from loom.core.backend.sqlalchemy import compile_all
from loom.core.model import BaseModel, ColumnField, Integer


class Ticket(BaseModel):
    __tablename__ = "uuid_tickets"
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    ref: UUID
    alt: UUID | None = None


def _ddl(dialect: Dialect) -> str:
    metadata = MetaData()
    compile_all(Ticket, metadata=metadata)
    return str(CreateTable(metadata.tables["uuid_tickets"]).compile(dialect=dialect))


def test_an_undeclared_uuid_compiles_to_the_postgres_uuid_type() -> None:
    ddl = _ddl(postgresql.dialect())

    assert "ref UUID NOT NULL" in ddl
    assert "alt UUID" in ddl


def test_an_undeclared_uuid_compiles_to_char_32_on_sqlite() -> None:
    ddl = _ddl(sqlite.dialect())

    assert "ref CHAR(32) NOT NULL" in ddl
    assert "alt CHAR(32)" in ddl
