"""A field that admits ``None`` stores ``None`` in Postgres; one that does not is refused."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration

_TABLE = "loom_it_nullable_gauges"


class Gauge(BaseModel):
    __tablename__ = _TABLE

    id: int = ColumnField(primary_key=True, autoincrement=True)
    label: str = ColumnField(length=32)
    level: int | None = None


class CreateGauge(Command, frozen=True):
    label: str
    level: int | None = None


@pytest.fixture
async def manager(pg_admin_uri: str) -> AsyncIterator[SessionManager]:
    reset_registry()
    compile_all(Gauge)
    session_manager = SessionManager(pg_admin_uri, pool_size=1, max_overflow=0, pool_pre_ping=False)
    async with session_manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
    try:
        yield session_manager
    finally:
        async with session_manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await session_manager.dispose()
        reset_registry()


async def test_an_optional_field_left_as_none_is_stored_and_read_back(
    manager: SessionManager,
) -> None:
    repo: RepositorySQLAlchemy[Gauge, int] = RepositorySQLAlchemy(
        session_manager=manager, model=Gauge
    )

    created = await repo.create(CreateGauge(label="idle"))
    stored = await repo.get_by_id(created.id)

    assert stored is not None
    assert (stored.label, stored.level) == ("idle", None)


async def test_a_field_that_does_not_admit_none_is_not_null_in_the_database(
    manager: SessionManager,
) -> None:
    with pytest.raises(IntegrityError):
        async with manager.engine.begin() as conn:
            await conn.execute(text(f"INSERT INTO {_TABLE} (label) VALUES (NULL)"))
