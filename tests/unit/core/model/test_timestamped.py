"""Unit tests for TimestampedModel."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.model import TimestampedModel, get_column_fields
from loom.core.model.enums import ServerDefault, ServerOnUpdate
from loom.core.model.field import ColumnField


class Order(TimestampedModel):
    __tablename__ = "orders"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    total: float = 0.0


class TestTimestampedModelFields:
    def test_has_created_at_column_spec(self) -> None:
        # __loom_columns__ is per-class; timestamps live on TimestampedModel itself
        assert "created_at" in TimestampedModel.__loom_columns__

    def test_has_updated_at_column_spec(self) -> None:
        assert "updated_at" in TimestampedModel.__loom_columns__

    def test_created_at_server_default_is_now(self) -> None:
        spec = TimestampedModel.__loom_columns__["created_at"]
        assert spec.field.server_default == ServerDefault.NOW

    def test_updated_at_server_default_is_now(self) -> None:
        spec = TimestampedModel.__loom_columns__["updated_at"]
        assert spec.field.server_default == ServerDefault.NOW

    def test_updated_at_server_onupdate_is_now(self) -> None:
        spec = TimestampedModel.__loom_columns__["updated_at"]
        assert spec.field.server_onupdate == ServerOnUpdate.NOW

    def test_created_at_is_nullable(self) -> None:
        assert get_column_fields(Order)["created_at"].field.nullable is True

    def test_updated_at_is_nullable(self) -> None:
        assert get_column_fields(Order)["updated_at"].field.nullable is True


class TestTimestampedModelSerialization:
    def test_timestamps_omitted_when_none(self) -> None:
        order = Order(id=1, total=9.99)
        encoded = msgspec.json.encode(order)
        data = msgspec.json.decode(encoded, type=dict)
        assert "createdAt" not in data
        assert "updatedAt" not in data

    def test_user_fields_present(self) -> None:
        order = Order(id=1, total=9.99)
        encoded = msgspec.json.encode(order)
        data = msgspec.json.decode(encoded, type=dict)
        assert data["id"] == 1
        assert data["total"] == pytest.approx(9.99)


class TestTimestampedModelInheritance:
    def test_is_subclass_of_base_model(self) -> None:
        from loom.core.model import BaseModel

        assert issubclass(Order, BaseModel)

    def test_user_model_inherits_timestamps(self) -> None:
        field_names = {f.name for f in msgspec.structs.fields(Order)}
        assert "created_at" in field_names
        assert "updated_at" in field_names

    def test_no_trace_id_field(self) -> None:
        """trace_id must not be in the model — it belongs to observability."""
        field_names = {f.name for f in msgspec.structs.fields(Order)}
        assert "trace_id" not in field_names
        assert "created_trace_id" not in field_names
