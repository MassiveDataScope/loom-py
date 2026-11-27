"""Unit tests for column type inference in ``model.inference``."""

from __future__ import annotations

import gc
import weakref
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal
from uuid import UUID

import msgspec
import pytest

from loom.core.model.field import ColumnFieldSpec
from loom.core.model.inference import (
    ListShape,
    Opaque,
    Outcome,
    ResolvedField,
    StructShape,
    read_fields,
    resolve_fields,
)
from loom.core.model.types import (
    JSON,
    BigInteger,
    ColumnType,
    Date,
    DateTime,
    Float,
    Integer,
    JsonStr,
    Numeric,
    Postgres,
    String,
    Text,
    Uuid,
)

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


class _Address(msgspec.Struct):
    street: str
    zip_code: str | None = None


@dataclass
class _Parcel:
    weight: float
    label: str | None = None


class _Label:
    text: str


class _Box(msgspec.Struct):
    label: _Label


class _Tree(msgspec.Struct):
    label: str
    children: list[_Tree]


class _Scalars(msgspec.Struct):
    count: int
    name: str
    moment: datetime
    day: date
    ref: UUID
    note: str | None
    unset: int | msgspec.UnsetType
    payload: JsonStr
    address: _Address
    parcels: list[_Parcel]
    extras: dict[str, Any]
    tags: tuple[str, ...]
    declared: Annotated[int, Integer]
    amount: Decimal
    span: timedelta
    tone: _Tone
    mode: Literal["a", "b"]
    anything: Any
    mixed: int | str
    money: _Money


@pytest.fixture(scope="module")
def scalars() -> dict[str, ResolvedField]:
    return {field.name: field for field in resolve_fields(_Scalars)}


class TestInferredColumnTypes:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("count", BigInteger),
            ("name", String()),
            ("moment", DateTime(tz=True)),
            ("day", Date),
            ("ref", Uuid),
            ("note", String()),
            ("unset", BigInteger),
            ("payload", Text),
            ("address", JSON),
            ("parcels", JSON),
            ("extras", JSON),
            ("tags", JSON),
        ],
    )
    def test_a_python_type_follows_its_rule(
        self, scalars: dict[str, ResolvedField], name: str, expected: ColumnType
    ) -> None:
        assert (scalars[name].column_type, scalars[name].outcome) == (expected, Outcome.INFERRED)

    def test_a_declared_marker_wins(self, scalars: dict[str, ResolvedField]) -> None:
        assert (scalars["declared"].column_type, scalars["declared"].outcome) == (
            Integer,
            Outcome.DECLARED,
        )

    def test_a_column_field_type_wins_over_the_annotation(self) -> None:
        class Reading(msgspec.Struct):
            value: Annotated[float, Float]

        declared = {"value": ColumnFieldSpec(Numeric(precision=9, scale=3))}

        (value,) = resolve_fields(Reading, declared)

        assert value.column_type == Numeric(precision=9, scale=3)


class TestUndeclaredFields:
    @pytest.mark.parametrize(
        ("name", "reason"),
        [
            ("amount", "a Decimal"),
            ("span", "a timedelta"),
            ("tone", "a plain Enum"),
            ("mode", "a Literal"),
            ("anything", "Any"),
            ("mixed", "a union of types that are not all JSON"),
            ("money", "_Money, a class that is not a record"),
        ],
    )
    def test_a_type_with_no_column_is_undeclared_with_its_reason(
        self, scalars: dict[str, ResolvedField], name: str, reason: str
    ) -> None:
        field = scalars[name]

        assert (field.outcome, field.reason, field.column_type) == (
            Outcome.UNDECLARED,
            reason,
            None,
        )

    def test_a_foreign_class_whose_hints_cannot_resolve_is_not_a_record(self) -> None:
        (item,) = resolve_fields(_HoldsForeign)

        assert (item.outcome, item.column_type, "__loom_fields__" in vars(_Foreign)) == (
            Outcome.UNDECLARED,
            None,
            False,
        )
        assert item.reason == "_Foreign, a class whose annotations could not be resolved"


class TestFieldReaders:
    def test_a_struct_keeps_its_encode_names(self) -> None:
        class Customer(msgspec.Struct, rename="camel"):
            given_name: str

        (field,) = resolve_fields(Customer)

        assert (field.name, field.encode_name) == ("given_name", "givenName")

    def test_a_dataclass_and_a_plain_class_encode_as_their_names_without_class_vars(self) -> None:
        class Plain:
            code: str
            registry: ClassVar[dict[str, str]]

        names = [
            (f.name, f.encode_name) for f in (*resolve_fields(_Parcel), *resolve_fields(Plain))
        ]

        assert names == [("weight", "weight"), ("label", "label"), ("code", "code")]

    def test_the_fields_of_a_class_built_at_run_time_are_collected_with_it(self) -> None:
        built = msgspec.defstruct("Built", [("code", str)])
        alive = weakref.ref(built)

        read_fields(built)
        del built
        gc.collect()

        assert alive() is None

    def test_a_field_that_admits_none_is_nullable(self, scalars: dict[str, ResolvedField]) -> None:
        assert (scalars["note"].nullable, scalars["count"].nullable) == (True, False)


class TestJsonShape:
    def test_a_struct_describes_its_fields(self, scalars: dict[str, ResolvedField]) -> None:
        shape = scalars["address"].shape

        assert isinstance(shape, StructShape)
        assert [f.name for f in shape.fields] == ["street", "zip_code"]

    def test_a_nested_plain_class_describes_its_fields(self) -> None:
        (label,) = resolve_fields(_Box)

        assert (label.column_type, label.shape) == (
            JSON,
            StructShape(resolve_fields(_Label)),
        )

    def test_a_list_describes_its_item(self, scalars: dict[str, ResolvedField]) -> None:
        shape = scalars["parcels"].shape

        assert isinstance(shape, ListShape)
        assert isinstance(shape.item.shape, StructShape)

    def test_a_list_of_scalars_has_a_scalar_item(self) -> None:
        class Basket(msgspec.Struct):
            labels: list[str]

        (labels,) = resolve_fields(Basket)

        assert isinstance(labels.shape, ListShape)
        assert labels.shape.item.column_type == String()

    def test_a_class_met_again_becomes_opaque(self) -> None:
        (_, children) = resolve_fields(_Tree)

        assert isinstance(children.shape, ListShape)
        assert children.shape.item.shape == Opaque()

    @pytest.mark.parametrize("name", ["extras", "tags"])
    def test_a_container_without_a_record_is_opaque(
        self, scalars: dict[str, ResolvedField], name: str
    ) -> None:
        assert scalars[name].shape == Opaque()

    def test_a_declared_array_is_a_list_of_its_item(self) -> None:
        class Device(msgspec.Struct):
            marks: Annotated[list[int], Postgres.ARRAY(Integer)]

        (marks,) = resolve_fields(Device)

        assert isinstance(marks.shape, ListShape)
        assert marks.shape.item.column_type == Integer
