"""QuerySpec filter values against Postgres: each compares with what the write stored."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID

import msgspec
import pytest

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.command import Command
from loom.core.model import (
    BaseModel,
    BoundaryValidationError,
    ColumnField,
    ColumnType,
    Postgres,
    String,
)
from loom.core.repository.abc import CursorCodec
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec, QuerySpec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration


class Shade(Enum):
    LIGHT = "light"
    DARK = "dark"


class Sample(BaseModel):
    __tablename__ = "loom_it_typed_filter_samples"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    shade: Shade = ColumnField(String(16))
    seen_at: datetime = ColumnField()
    ref: UUID = ColumnField(Postgres.UUID)
    due_on: date = ColumnField()
    opens_at: time = ColumnField()
    closes_at: time = ColumnField(ColumnType("Time", kwargs={"timezone": True}))


class CreateSample(Command, frozen=True):
    shade: Shade
    seen_at: datetime
    ref: UUID
    due_on: date
    opens_at: time
    closes_at: time


_FIRST = CreateSample(
    shade=Shade.DARK,
    seen_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    ref=UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60"),
    due_on=date(2026, 1, 2),
    opens_at=time(8, 30),
    closes_at=time(18, 0, tzinfo=timezone(timedelta(hours=2))),
)
_SECOND = CreateSample(
    shade=Shade.LIGHT,
    seen_at=datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC),
    ref=UUID("0b9d2f1c-1111-4c1e-9d1e-1b2c3d4e5f60"),
    due_on=date(2026, 2, 3),
    opens_at=time(17, 0),
    closes_at=time(23, 0, tzinfo=UTC),
)


@pytest.fixture
async def repo(pg_admin_uri: str) -> AsyncIterator[RepositorySQLAlchemy[Sample, int]]:
    reset_registry()
    compile_all(Sample)
    manager = SessionManager(pg_admin_uri, pool_size=1, max_overflow=0, pool_pre_ping=False)
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
    try:
        yield RepositorySQLAlchemy(
            session_manager=manager, model=Sample, cursor_codec=CursorCodec(signing="ephemeral")
        )
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.dispose()
        reset_registry()


@pytest.mark.parametrize(
    ("field", "op", "value"),
    [
        ("shade", FilterOp.EQ, Shade.DARK),
        ("seen_at", FilterOp.LTE, datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("ref", FilterOp.EQ, UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")),
        ("due_on", FilterOp.LT, date(2026, 2, 1)),
        ("opens_at", FilterOp.EQ, time(8, 30)),
        ("closes_at", FilterOp.EQ, time(18, 0, tzinfo=timezone(timedelta(hours=2)))),
    ],
)
async def test_a_typed_filter_value_selects_the_row_written_with_it(
    repo: RepositorySQLAlchemy[Sample, int], field: str, op: FilterOp, value: Any
) -> None:
    first = await repo.create(_FIRST)
    await repo.create(_SECOND)

    page = await repo.list_with_query(
        QuerySpec(filters=FilterGroup(filters=(FilterSpec(field=field, op=op, value=value),)))
    )

    assert [item.id for item in page.items] == [first.id]


async def test_date_and_time_values_read_back_as_written(
    repo: RepositorySQLAlchemy[Sample, int],
) -> None:
    created = await repo.create(_FIRST)

    stored = await repo.get_by_id(created.id)

    assert stored is not None
    assert (stored.due_on, stored.opens_at) == (_FIRST.due_on, _FIRST.opens_at)


async def test_a_time_with_time_zone_column_keeps_the_offset(
    repo: RepositorySQLAlchemy[Sample, int],
) -> None:
    created = await repo.create(_FIRST)

    stored = await repo.get_by_id(created.id)

    assert stored is not None
    assert stored.closes_at.utcoffset() == timedelta(hours=2)
    assert stored.closes_at == _FIRST.closes_at


async def test_a_naive_time_for_a_time_with_time_zone_column_is_refused(
    repo: RepositorySQLAlchemy[Sample, int],
) -> None:
    naive = msgspec.structs.replace(_FIRST, closes_at=time(18, 0))

    with pytest.raises(
        BoundaryValidationError, match="closes_at: time must carry a UTC offset"
    ) as caught:
        await repo.create(naive)

    assert caught.value.code == "boundary_validation"
