"""Datetime round trip on a real server: ``create`` output equals a later read.

BSON keeps milliseconds and no offset; the client is built ``tz_aware`` so an
aware-column read is UTC, and a ``DateTime(tz=False)`` column reads back naive.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import msgspec
import pytest
from pymongo import AsyncMongoClient

from loom.core.model import BaseModel, ColumnField, ServerDefault
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from tests.unit.core.repository.contract.conftest import SEED

from .conftest import _SessionDatabase

pytestmark = [pytest.mark.integration, pytest.mark.mongo]


class Event(BaseModel):
    __tablename__ = "events"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    at: datetime = ColumnField()
    created_at: datetime | None = ColumnField(
        server_default=ServerDefault.NOW, nullable=True, default=None
    )


class EventCreate(BaseModel):
    at: datetime


@pytest.fixture
async def events(
    mongo_client: AsyncMongoClient[Any], mongo_database: _SessionDatabase
) -> RepositoryMongo[Event, str]:
    return RepositoryMongo(
        Event,
        mongo_client[mongo_database.name]["events"],
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


async def test_aware_column_reads_back_as_utc_with_millisecond_precision(
    events: RepositoryMongo[Event, str],
) -> None:
    created = await events.create(
        EventCreate(at=datetime(2026, 1, 3, 12, 0, 0, 123_456, tzinfo=UTC))
    )

    found = await events.get_by_id(created.id)

    assert found == created
    assert found is not None
    assert found.at == datetime(2026, 1, 3, 12, 0, 0, 123_000, tzinfo=UTC)
    assert found.at.utcoffset() == timedelta(0)
    assert found.created_at is not None
    assert found.created_at.utcoffset() == timedelta(0)


async def test_naive_column_reads_back_naive(repository: Any) -> None:
    order = msgspec.structs.replace(SEED[0], created_at=datetime(2026, 1, 3, 12, 0, 0, 123_456))

    created = await repository.create(order)

    assert created.created_at == datetime(2026, 1, 3, 12, 0, 0, 123_000)
    assert await repository.get_by_id(created.id) == created
