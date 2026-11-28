"""Unit tests for column type inference in ``model.inference``."""

from __future__ import annotations

import gc
import weakref
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal

import msgspec
import pytest

from loom.core.model.field import ColumnFieldSpec
from loom.core.model.inference import AnnotatedClass, ListShape, Opaque
from loom.core.model.types import Float, JsonStr, Numeric, String

if TYPE_CHECKING:
    from fractions import Fraction as NotImported


class _Tone(Enum):
    LOW = "low"


class _Money:
    pass


class _Foreign:
    value: NotImported


class _HoldsForeign(msgspec.Struct):
    item: _Foreign


@dataclass
class _Parcel:
    weight: float
    label: str | None = None


class _Tree(msgspec.Struct):
    label: str
    children: list[_Tree]


class _Scalars(msgspec.Struct):
    amount: Decimal
    span: timedelta
    tone: _Tone
    mode: Literal["a", "b"]
    anything: Any
    mixed: int | str
    money: _Money
    payload: JsonStr


_UNDECLARED_REASONS = {
    "amount": "a Decimal",
    "span": "a timedelta",
    "tone": "a plain Enum",
    "mode": "a Literal",
    "anything": "Any",
    "mixed": "a union of types that are not all JSON",
    "money": "_Money, a class that is not a record",
}


class TestColumnTypes:
    @pytest.mark.parametrize(("name", "reason"), _UNDECLARED_REASONS.items())
    def test_a_field_with_no_rule_is_undeclared_with_its_reason(
        self, name: str, reason: str
    ) -> None:
        field = {f.name: f for f in AnnotatedClass(_Scalars).resolve()}[name]

        assert (field.column_type, field.reason) == (None, reason)

    def test_an_annotation_that_is_not_a_class_is_undeclared(self) -> None:
        assert AnnotatedClass(list[int]).column_type.reason == "the annotation list[int]"

    def test_a_column_field_type_wins_over_the_annotation(self) -> None:
        class Reading(msgspec.Struct):
            value: Annotated[float, Float]

        declared = {"value": ColumnFieldSpec(Numeric(precision=9, scale=3))}

        (value,) = AnnotatedClass(Reading).resolve(declared)

        assert value.column_type == Numeric(precision=9, scale=3)

    def test_a_foreign_class_whose_hints_cannot_resolve_is_undeclared(self) -> None:
        (item,) = AnnotatedClass(_HoldsForeign).resolve()

        assert (item.column_type, item.reason) == (
            None,
            "_Foreign, a class whose annotations could not be resolved",
        )


class TestFieldReaders:
    def test_a_struct_keeps_its_encode_names(self) -> None:
        class Customer(msgspec.Struct, rename="camel"):
            given_name: str

        (field,) = AnnotatedClass(Customer).resolve()

        assert (field.name, field.encode_name) == ("given_name", "givenName")

    def test_a_dataclass_encodes_as_its_field_names(self) -> None:
        names = [(f.name, f.encode_name) for f in AnnotatedClass(_Parcel).resolve()]

        assert names == [("weight", "weight"), ("label", "label")]

    def test_a_plain_class_leaves_its_class_vars_out(self) -> None:
        class Plain:
            code: str
            registry: ClassVar[dict[str, str]]

        names = [(f.name, f.encode_name) for f in AnnotatedClass(Plain).resolve()]

        assert names == [("code", "code")]

    def test_the_fields_of_a_class_built_at_run_time_are_collected_with_it(self) -> None:
        built = msgspec.defstruct("Built", [("code", str)])
        alive = weakref.ref(built)

        _ = AnnotatedClass(built).fields
        del built
        gc.collect()

        assert alive() is None

    def test_a_class_whose_metaclass_refuses_attributes_still_resolves(self) -> None:
        class Frozen(type):
            def __setattr__(cls, name: str, value: object) -> None:
                raise TypeError("class attributes are frozen")

        class Sealed(metaclass=Frozen):
            code: str

        (field,) = AnnotatedClass(Sealed).resolve()

        assert (field.name, field.column_type) == ("code", String())

    def test_a_class_met_again_becomes_opaque(self) -> None:
        (_, children) = AnnotatedClass(_Tree).resolve()

        assert isinstance(children.shape, ListShape)
        assert children.shape.item.shape == Opaque()
