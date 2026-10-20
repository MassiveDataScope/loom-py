"""IntoTable's SQLAlchemy columns and values follow core's model introspection and write path."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Annotated, Any, Literal

import msgspec
import pytest
from sqlalchemy import Insert, MetaData
from sqlalchemy.dialects import postgresql

from loom.core.async_bridge import AsyncBridge
from loom.core.backend.sqlalchemy import compile_model
from loom.core.errors.codes import ErrorCode
from loom.core.model import (
    BaseModel,
    BoundaryValidationError,
    ColumnField,
    LoomStruct,
    Numeric,
    String,
    Text,
)
from loom.core.model.enums import Cardinality
from loom.core.model.relation import RelationField
from loom.streaming.nodes._table import Backend, IntoTable, SqlAlchemySinkConfig


class _Shade(StrEnum):
    LIGHT = "light"
    DARK = "dark"


class _Grade(IntEnum):
    LOW = 1
    HIGH = 2


class _Mode(Enum):
    AUTO = "auto"
    MANUAL = "manual"


class _Reading(BaseModel):
    __tablename__ = "readings"

    label: str
    count: int
    ratio: float
    active: bool
    amount: Annotated[Decimal, Numeric(precision=12, scale=2)]
    taken_at: datetime
    taken_on: date
    opens_at: time
    blob: bytes
    shade: _Shade
    grade: _Grade
    note: Annotated[str, Text]
    code: str = ColumnField(String(12))
    id: int = ColumnField(primary_key=True)


class _PlainReading(msgspec.Struct):
    label: str
    count: int
    ratio: float
    active: bool
    amount: Annotated[Decimal, Numeric(precision=12, scale=2)]
    taken_at: datetime
    taken_on: date
    opens_at: time
    blob: bytes
    shade: _Shade
    grade: _Grade
    note: Annotated[str, Text]
    code: Annotated[str, String(12)]


class _Point(LoomStruct):
    x: int
    y: int


class _Setting(LoomStruct):
    mode: Annotated[_Mode, String(16)]
    origin: _Point


class _UndeclaredMode(LoomStruct):
    mode: _Mode


class _UndeclaredAmount(LoomStruct):
    amount: Decimal


class _UndeclaredKind(LoomStruct):
    kind: Literal["first", "second"]


class _Opening(LoomStruct):
    opening_id: int
    opens_at: time


class _Owner(LoomStruct):
    id: int


class _Asset(BaseModel):
    __tablename__ = "assets"

    owner_id: int
    owner: _Owner | None = RelationField(
        foreign_key="owners.id", cardinality=Cardinality.MANY_TO_ONE
    )
    id: int = ColumnField(primary_key=True)


class _RecordingSession:
    def __init__(self, calls: list[tuple[Insert, list[dict[str, Any]]]]) -> None:
        self._calls = calls

    async def execute(self, statement: Insert, rows: list[dict[str, Any]]) -> None:
        self._calls.append((statement, rows))

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class _RecordingSessionManager:
    def __init__(self) -> None:
        self.calls: list[tuple[Insert, list[dict[str, Any]]]] = []

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_RecordingSession]:
        yield _RecordingSession(self.calls)


@pytest.fixture
def bridge() -> Iterator[AsyncBridge]:
    bridge = AsyncBridge()
    yield bridge
    bridge.shutdown()


def _sink(payload: type[Any]) -> IntoTable[Any]:
    return IntoTable(payload=payload, table="t", backend=Backend.SQLALCHEMY)


def _sink_config() -> SqlAlchemySinkConfig:
    return SqlAlchemySinkConfig.from_config({"table": "t"}, default_table="t")


def _write(bridge: AsyncBridge, item: Any) -> tuple[Insert, dict[str, Any]]:
    """Write ``item`` through an IntoTable partition; return the INSERT and its row."""
    session_manager = _RecordingSessionManager()
    partition = _sink(type(item)).build_partition(
        _sink_config(), 0, 1, bridge=bridge, session_manager=session_manager
    )
    partition.write_batch([item])
    partition.close()
    ((statement, (row,)),) = session_manager.calls
    return statement, row


def _ddl_types(columns: Any) -> dict[str, str]:
    dialect = postgresql.dialect()
    return {column.name: column.type.compile(dialect=dialect) for column in columns}


_READING_VALUES: dict[str, Any] = {
    "label": "a",
    "count": 1,
    "ratio": 0.5,
    "active": True,
    "amount": Decimal("1.50"),
    "taken_at": datetime(2026, 1, 2, 8, 30),
    "taken_on": date(2026, 1, 2),
    "opens_at": time(8, 30),
    "blob": b"\x00\x01",
    "shade": _Shade.DARK,
    "grade": _Grade.HIGH,
    "note": "n",
    "code": "c-1",
}


@pytest.mark.parametrize(
    "item",
    [_Reading(id=1, **_READING_VALUES), _PlainReading(**_READING_VALUES)],
    ids=["loom_model", "plain_struct"],
)
def test_into_table_columns_match_core_columns_for_the_same_fields(
    bridge: AsyncBridge, item: Any
) -> None:
    statement, _ = _write(bridge, item)

    core_types = _ddl_types(compile_model(_Reading, metadata=MetaData()).__table__.columns)
    sink_types = _ddl_types(statement.table.columns)
    assert sink_types == {name: core_types[name] for name in sink_types}
    assert set(core_types) - set(sink_types) <= {"id"}


def test_a_plain_enum_and_a_struct_are_written_as_core_writes_them(bridge: AsyncBridge) -> None:
    _, row = _write(bridge, _Setting(mode=_Mode.MANUAL, origin=_Point(x=1, y=2)))

    assert row == {"mode": "manual", "origin": {"x": 1, "y": 2}}


def test_a_relation_field_is_neither_a_column_nor_a_value(bridge: AsyncBridge) -> None:
    statement, row = _write(bridge, _Asset(id=7, owner_id=3))

    assert [column.name for column in statement.table.columns] == ["owner_id", "id"]
    assert row == {"owner_id": 3, "id": 7}


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        (_UndeclaredMode, "_UndeclaredMode.mode"),
        (_UndeclaredAmount, "_UndeclaredAmount.amount"),
        (_UndeclaredKind, "_UndeclaredKind.kind"),
    ],
    ids=["plain_enum", "decimal", "literal"],
)
def test_a_field_without_a_declared_column_type_is_refused(
    bridge: AsyncBridge, payload: type[Any], field: str
) -> None:
    sink = _sink(payload)
    config = _sink_config()
    session_manager = _RecordingSessionManager()

    with pytest.raises(ValueError, match=rf"^{re.escape(field)}: no column type is inferred"):
        sink.build_partition(config, 0, 1, bridge=bridge, session_manager=session_manager)


def test_a_time_with_an_offset_is_refused_for_a_time_column_as_the_repository_refuses_it(
    bridge: AsyncBridge,
) -> None:
    opening = _Opening(opening_id=1, opens_at=time(8, 30, tzinfo=timezone(timedelta(hours=2))))

    with pytest.raises(BoundaryValidationError) as refused:
        _write(bridge, opening)

    assert refused.value.code == ErrorCode.BOUNDARY_VALIDATION
    assert refused.value.violations == (("opens_at", "time must not carry a UTC offset"),)
