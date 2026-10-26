"""A MANY_TO_MANY joins the target on the column its secondary foreign key references."""

from __future__ import annotations

import pytest

from loom.core.backend.sqlalchemy import compile_all
from loom.core.command import Command
from loom.core.model import BaseModel, Cardinality, ColumnField, RelationField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.unit.core.conftest import SessionManagerFactory


class _Device(BaseModel):
    __tablename__ = "m2m_devices"
    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True, unique=True)
    name: str = ColumnField(length=16)


class _Tag(BaseModel):
    __tablename__ = "m2m_tags"
    id: int = ColumnField(primary_key=True)
    devices: list[dict[str, object]] = RelationField(
        foreign_key="tag_id", cardinality=Cardinality.MANY_TO_MANY, secondary="m2m_tag_devices"
    )


class _TagDevice(BaseModel):
    __tablename__ = "m2m_tag_devices"
    id: int = ColumnField(primary_key=True)
    tag_id: int = ColumnField(foreign_key="m2m_tags.id")
    device_id: int = ColumnField(foreign_key="m2m_devices.id")


class _CreateDevice(Command, frozen=True):
    account_id: int
    id: int
    name: str


class _CreateTag(Command, frozen=True):
    id: int


class _CreateLink(Command, frozen=True):
    id: int
    tag_id: int
    device_id: int


@pytest.fixture
async def session_manager(sqlite_manager_for: SessionManagerFactory) -> SessionManager:
    compile_all(_Device, _Tag, _TagDevice)
    return await sqlite_manager_for(_Tag)


async def test_a_composite_key_target_is_joined_on_the_referenced_column(
    session_manager: SessionManager,
) -> None:
    devices = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Device,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    tags = RepositorySQLAlchemy(
        session_manager=session_manager, model=_Tag, cursor_codec=CursorCodec(signing="ephemeral")
    )
    links = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_TagDevice,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    # Device "A" is (account 1, id 7) and "B" is (account 7, id 1): joining on the
    # first key column, account_id, would pick "B" for the link to device id 7.
    await devices.create_many(
        [_CreateDevice(account_id=1, id=7, name="A"), _CreateDevice(account_id=7, id=1, name="B")]
    )
    await tags.create_many([_CreateTag(id=10)])
    await links.create_many([_CreateLink(id=1, tag_id=10, device_id=7)])

    tag = await tags.get_by_id(10)

    assert tag is not None
    assert [device["name"] for device in tag.devices] == ["A"]
