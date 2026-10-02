from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import msgspec
import pytest
from sqlalchemy import LargeBinary, MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.schema import CreateTable

from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.model import BaseModel, Bytes, ColumnField, Postgres, get_column_fields
from loom.core.model.types import Integer
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy


class Device(BaseModel):
    __tablename__ = "devices"
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    fingerprint: bytes = ColumnField(Bytes)
    address: str = ColumnField(Postgres.INET)
    inferred: bytes | None = None


class DeviceInput(msgspec.Struct):
    fingerprint: bytes
    address: str


def test_bytes_compiles_to_large_binary_and_inet_to_the_postgres_inet_type() -> None:
    metadata = MetaData()

    compile_all(Device, metadata=metadata)

    columns = metadata.tables["devices"].c
    assert isinstance(columns.fingerprint.type, LargeBinary)
    assert isinstance(columns.address.type, INET)
    ddl = str(CreateTable(metadata.tables["devices"]).compile(dialect=postgresql.dialect()))
    assert "fingerprint BYTEA NOT NULL" in ddl
    assert "address INET NOT NULL" in ddl


def test_a_bytes_annotation_is_inferred_as_bytes() -> None:
    assert get_column_fields(Device)["inferred"].column_type == Bytes


@pytest.fixture
def shared_registry() -> Iterator[None]:
    """Compile into the registry repositories read, and leave it empty for the next test."""
    reset_registry()
    yield
    reset_registry()


@pytest.mark.usefixtures("shared_registry")
def test_bytes_reach_the_column_as_bytes_not_base64() -> None:
    compile_all(Device)
    repository: RepositorySQLAlchemy[Device, int] = RepositorySQLAlchemy(
        session_manager=cast(Any, None), model=Device
    )

    row = repository.create_object(DeviceInput(fingerprint=b"\x00\xff", address="10.0.0.1"))

    assert row.fingerprint == b"\x00\xff"
    assert row.address == "10.0.0.1"
