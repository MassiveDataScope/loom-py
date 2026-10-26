"""``validate_id_field``: which primary-key annotations can hold the id Mongo generates."""

from __future__ import annotations

from uuid import UUID

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.mongo.ids import validate_id_field


class _TextKey(BaseModel):
    __tablename__ = "ids_text_keys"

    id: str | msgspec.UnsetType = ColumnField(primary_key=True, default=msgspec.UNSET)


class _UuidKey(BaseModel):
    __tablename__ = "ids_uuid_keys"

    id: UUID | msgspec.UnsetType = ColumnField(primary_key=True, default=msgspec.UNSET)


class _IntKey(BaseModel):
    __tablename__ = "ids_int_keys"

    id: int | msgspec.UnsetType = ColumnField(primary_key=True, default=msgspec.UNSET)


@pytest.mark.parametrize("model", [_TextKey, _UuidKey])
def test_a_str_or_uuid_key_left_unset_until_generated_is_accepted(model: type) -> None:
    assert validate_id_field(model).name == "id"


def test_an_int_key_left_unset_is_still_refused() -> None:
    with pytest.raises(ConfigError, match="annotated int"):
        validate_id_field(_IntKey)
