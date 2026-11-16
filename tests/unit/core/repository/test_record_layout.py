"""``RecordLayout`` gives the values a struct persists when it is created."""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from unittest.mock import patch

import msgspec

from loom.core.command import Command
from loom.core.model import BaseModel, Cardinality, ColumnField, RelationField
from loom.core.stores.repository.record import RecordLayout
from loom.rest.autocrud import _derive_create_struct


class _Shade(StrEnum):
    DARK = "dark"


class _Wrapped(msgspec.Struct):
    shade: _Shade


class _Gift(Command, frozen=True):
    gift_name: str
    wrap_count: int = 0
    labels: list[str] = []
    note: str | msgspec.UnsetType = msgspec.UNSET


class _Plain(msgspec.Struct):
    label: str = msgspec.UNSET


class _Optional(msgspec.Struct):
    count: int | msgspec.UnsetType = 3


class _Tagged(msgspec.Struct, tag="tagged", tag_field="kind"):
    size: int = 1


class _Rack(BaseModel):
    __tablename__ = "record_layout_racks"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=32)
    trays: list[dict[str, object]] = RelationField(
        foreign_key="rack_id", cardinality=Cardinality.ONE_TO_MANY
    )


class _Tray(BaseModel):
    __tablename__ = "record_layout_trays"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=32)
    capacity: int = 10
    marks: list[str] = []


class TestRecordLayout:
    def test_a_field_left_at_its_default_is_included(self) -> None:
        values = RecordLayout.of(_Gift).values(_Gift(gift_name="a"))

        assert values == {"gift_name": "a", "wrap_count": 0, "labels": []}

    def test_an_unset_field_is_left_out_and_a_set_one_kept(self) -> None:
        layout = RecordLayout.of(_Gift)

        assert "note" not in layout.values(_Gift(gift_name="a"))
        assert layout.values(_Gift(gift_name="a", note="n"))["note"] == "n"

    def test_renamed_fields_are_keyed_by_their_field_name(self) -> None:
        assert set(RecordLayout.of(_Gift).values(_Gift(gift_name="a"))) >= {"gift_name"}

    def test_it_matches_the_wire_form_of_a_struct_with_no_defaults(self) -> None:
        class Plain(Command, frozen=True):
            gift_name: str
            wrap_count: int

        plain = Plain(gift_name="a", wrap_count=2)
        wire = msgspec.to_builtins(plain)
        internal = {"giftName": "gift_name", "wrapCount": "wrap_count"}

        assert RecordLayout.of(Plain).values(plain) == {
            internal[key]: value for key, value in wire.items()
        }

    def test_values_are_not_converted(self) -> None:
        wrapped = _Wrapped(shade=_Shade.DARK)

        assert RecordLayout.of(_Wrapped).values(wrapped) == {"shade": _Shade.DARK}

    def test_the_layout_of_a_type_is_built_once(self) -> None:
        assert RecordLayout.of(_Gift) is RecordLayout.of(_Gift)

    def test_a_model_keeps_its_defaults(self) -> None:
        tray = _Tray(name="t")

        assert RecordLayout.of(_Tray).values(tray) == {
            "id": None,
            "name": "t",
            "capacity": 10,
            "marks": [],
        }

    def test_an_unloaded_relation_is_left_out(self) -> None:
        values = RecordLayout.of(_Rack).values(_Rack(name="r"))

        assert "trays" not in values

    def test_a_plain_annotated_field_left_unset_is_left_out(self) -> None:
        assert RecordLayout.of(_Plain).values(_Plain()) == {}

    def test_an_unset_value_in_an_unset_capable_field_is_left_out(self) -> None:
        layout = RecordLayout.of(_Optional)

        assert layout.values(_Optional(count=msgspec.UNSET)) == {}
        assert layout.values(_Optional()) == {"count": 3}

    def test_a_tagged_struct_keeps_its_tag_field(self) -> None:
        assert RecordLayout.of(_Tagged).values(_Tagged()) == {"kind": "tagged", "size": 1}

    def test_a_derived_create_input_keeps_its_defaulted_fields(self) -> None:
        create_input: type[Any] = _derive_create_struct(_Tray)

        values = RecordLayout.of(create_input).values(create_input(name="t"))

        assert (values["capacity"], values["marks"]) == (10, [])

    def test_no_conversion_runs_for_a_row(self) -> None:
        row = _Gift(gift_name="a")
        layout = RecordLayout.of(_Gift)

        with patch.object(msgspec, "to_builtins", side_effect=AssertionError) as converter:
            layout.values(row)

        assert converter.call_count == 0
