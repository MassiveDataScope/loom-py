"""An annotation maps to the frame type that ``resolve_schema`` gives the same field."""

from __future__ import annotations

import typing
from datetime import date, datetime

import msgspec

from loom.etl.schema import annotation_to_frame_type, resolve_schema


class _Line(msgspec.Struct):
    sku: str
    quantity: int


class _Order(msgspec.Struct):
    order_id: str
    placed_at: datetime
    lines: list[_Line]
    shipped_on: date | None = None


class TestAnnotationToFrameType:
    def test_each_field_maps_as_resolve_schema_maps_it(self) -> None:
        by_name = {column.name: column.dtype for column in resolve_schema(_Order)}

        for name, annotation in typing.get_type_hints(_Order).items():
            assert annotation_to_frame_type(annotation) == by_name[name]
