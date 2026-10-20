"""``to_column_row`` builds the row a struct writes, as the repository's write path builds it."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import msgspec
import pytest
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import types as sa_types

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.errors.codes import ErrorCode
from loom.core.model import (
    JSON,
    BaseModel,
    BoundaryValidationError,
    ColumnField,
    LoomStruct,
    String,
    Text,
)
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.values import to_column_row


class _Mode(Enum):
    AUTO = "auto"
    MANUAL = "manual"


class _Point(LoomStruct):
    x: int
    taken_at: datetime


class _Station(BaseModel):
    __tablename__ = "column_row_stations"

    id: int = ColumnField(primary_key=True)
    mode: Annotated[_Mode, String(16)]
    origin: _Point = ColumnField(JSON)
    opens_at: time
    note: Annotated[str | msgspec.UnsetType, Text] = msgspec.UNSET


_TAKEN_AT = datetime(2026, 1, 2, 8, 30, tzinfo=UTC)


def test_the_repository_write_path_and_to_column_row_give_the_same_column_values() -> None:
    compile_all(_Station)
    compiled = cast(Any, get_compiled(_Station))
    repository: RepositorySQLAlchemy[_Station, int] = RepositorySQLAlchemy(
        session_manager=MagicMock(),
        model=_Station,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    station = _Station(
        id=1, mode=_Mode.MANUAL, origin=_Point(x=3, taken_at=_TAKEN_AT), opens_at=time(8, 30)
    )

    from_repository = repository._serialize_input(station)
    from_columns = to_column_row(station, compiled.__table__.columns)

    expected = {
        "id": 1,
        "mode": "manual",
        "origin": {"x": 3, "taken_at": "2026-01-02T08:30:00Z"},
        "opens_at": time(8, 30),
    }
    assert from_repository == expected
    assert from_columns == expected


class _Reading(msgspec.Struct):
    reading_id: int
    mode: _Mode
    origin: _Point
    opens_at: time
    amount: Decimal
    visit_id: UUID
    blob: bytes
    note: str | msgspec.UnsetType = msgspec.UNSET
    ignored: str = "not a column"


def _reading_table() -> Table:
    return Table(
        "readings",
        MetaData(),
        Column("reading_id", sa_types.Integer),
        Column("mode", sa_types.Integer),
        Column("origin", sa_types.JSON),
        Column("opens_at", sa_types.Time),
        Column("amount", sa_types.Integer),
        Column("visit_id", sa_types.Integer),
        Column("blob", sa_types.Integer),
        Column("note", sa_types.Integer),
    )


def _reading(**overrides: Any) -> _Reading:
    values: dict[str, Any] = {
        "reading_id": 1,
        "mode": _Mode.AUTO,
        "origin": _Point(x=3, taken_at=_TAKEN_AT),
        "opens_at": time(8, 30),
        "amount": Decimal("1.50"),
        "visit_id": UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60"),
        "blob": b"\x00\x01",
    }
    return _Reading(**{**values, **overrides})


def test_each_column_takes_the_attribute_of_its_name_in_its_column_form() -> None:
    row = to_column_row(_reading(note="n"), _reading_table().columns)

    assert row == {
        "reading_id": 1,
        "mode": "auto",
        "origin": {"x": 3, "taken_at": "2026-01-02T08:30:00Z"},
        "opens_at": time(8, 30),
        "amount": Decimal("1.50"),
        "visit_id": UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60"),
        "blob": b"\x00\x01",
        "note": "n",
    }


def test_an_unset_value_leaves_its_column_out_of_the_row() -> None:
    row = to_column_row(_reading(), _reading_table().columns)

    assert "note" not in row
    assert set(row) == {column.name for column in _reading_table().columns} - {"note"}


def test_a_time_whose_offset_its_column_cannot_take_is_refused() -> None:
    reading = _reading(opens_at=time(8, 30, tzinfo=timezone(timedelta(hours=2))))
    columns = _reading_table().columns

    with pytest.raises(BoundaryValidationError) as refused:
        to_column_row(reading, columns)

    assert refused.value.code == ErrorCode.BOUNDARY_VALIDATION
    assert refused.value.violations == (("opens_at", "time must not carry a UTC offset"),)


def test_a_time_with_an_offset_is_kept_in_a_json_column() -> None:
    aware = time(8, 30, tzinfo=timezone(timedelta(hours=2)))
    table = Table("t", MetaData(), Column("opens_at", sa_types.JSON))

    assert to_column_row(_reading(opens_at=aware), table.columns) == {"opens_at": "08:30:00+02:00"}
