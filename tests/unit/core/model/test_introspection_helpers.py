"""Unit tests for the shared annotation helpers in ``model.introspection``."""

from __future__ import annotations

import typing
from datetime import date
from decimal import Decimal
from enum import Enum, IntEnum, IntFlag, StrEnum
from typing import Annotated, Any, Generic, Literal, TypeVar

import msgspec
import pytest

from loom.core.model import BaseModel, ColumnField, Field
from loom.core.model.field import ColumnType
from loom.core.model.introspection import (
    _union_inner_args,
    extract_model_from_hint,
    generic_type_arg,
    get_column_fields,
    list_element_type,
    resolve_type_hints,
)
from loom.core.model.types import JSON, Float, Integer, Numeric, String


class _Note:
    pass


class _Parent:
    note: _Note
    notes: list[_Note] | msgspec.UnsetType
    tagged: Annotated[int, "unit"]


class _Unresolvable:
    """A class whose annotation never resolves to a real name."""


# Assigned after the fact: a quoted annotation in the class body would be
# reported as an undefined name by the type checker, and the point of this
# fixture is precisely that the name does not exist at runtime.
_Unresolvable.__annotations__ = {"later": "_NeverDefined"}


_T = TypeVar("_T")


class _Box(Generic[_T]):
    pass


class _OtherBox(Generic[_T]):
    pass


class TestResolveTypeHints:
    def test_resolves_annotations_of_a_plain_class(self) -> None:
        hints = resolve_type_hints(_Parent)

        assert hints["note"] is _Note

    def test_returns_empty_mapping_for_unresolvable_forward_reference(self) -> None:
        assert resolve_type_hints(_Unresolvable) == {}

    def test_include_extras_keeps_annotated_metadata(self) -> None:
        hints = resolve_type_hints(_Parent, include_extras=True)

        assert getattr(hints["tagged"], "__metadata__", ()) == ("unit",)

    def test_without_include_extras_strips_annotated_metadata(self) -> None:
        hints = resolve_type_hints(_Parent)

        assert hints["tagged"] is int


class TestUnionInnerArgs:
    def test_drops_none_from_a_pep604_union(self) -> None:
        assert _union_inner_args(int | None) == (int,)

    def test_drops_unset_type_from_a_pep604_union(self) -> None:
        assert _union_inner_args(_Note | msgspec.UnsetType) == (_Note,)

    def test_normalises_the_typing_union_form(self) -> None:
        # The legacy spelling is the point of this test, so it stays verbatim.
        hint = typing.Union[int, str]  # noqa: UP007

        assert _union_inner_args(hint) == (int, str)

    def test_returns_none_for_a_non_union_annotation(self) -> None:
        assert _union_inner_args(list[int]) is None


class TestExtractModelFromHint:
    def test_unwraps_a_list_inside_a_union(self) -> None:
        hints = resolve_type_hints(_Parent)

        assert extract_model_from_hint(hints["notes"]) is _Note

    def test_returns_a_bare_class_unchanged(self) -> None:
        assert extract_model_from_hint(_Note) is _Note

    def test_returns_none_for_a_parametrised_dict(self) -> None:
        assert extract_model_from_hint(list[dict[str, Any]]) is None

    def test_returns_none_for_a_non_type_annotation(self) -> None:
        assert extract_model_from_hint(None) is None


class TestListElementType:
    def test_returns_the_element_of_a_plain_list(self) -> None:
        assert list_element_type(list[_Note]) is _Note

    def test_returns_the_element_of_a_list_widened_with_unset(self) -> None:
        assert list_element_type(list[_Note] | msgspec.UnsetType) is _Note

    def test_returns_none_for_a_non_list_annotation(self) -> None:
        assert list_element_type(_Note) is None

    def test_returns_none_when_the_element_is_not_a_class(self) -> None:
        assert list_element_type(list[dict[str, Any]]) is None


class TestGenericTypeArg:
    def test_returns_the_single_argument_of_the_expected_origin(self) -> None:
        assert generic_type_arg(_Box[_Note], _Box) is _Note

    def test_returns_none_for_a_different_origin(self) -> None:
        assert generic_type_arg(_OtherBox[_Note], _Box) is None

    def test_returns_none_for_a_non_generic_annotation(self) -> None:
        assert generic_type_arg(_Note, _Box) is None

    def test_returns_none_when_the_argument_is_not_a_class(self) -> None:
        assert generic_type_arg(_Box[list[int]], _Box) is None


class _Optionals(msgspec.Struct):
    count: int | None = None
    label: str | None = None


class TestOptionalColumnTypes:
    def test_maps_a_pep604_optional_to_its_inner_column_type(self) -> None:
        assert get_column_fields(_Optionals)["count"].column_type == Integer

    def test_maps_a_typing_optional_to_its_inner_column_type(self) -> None:
        assert get_column_fields(_Optionals)["label"].column_type == String()


class _Status(StrEnum):
    PAID = "paid"


class _Priority(IntEnum):
    HIGH = 1


class _Colour(Enum):
    RED = "red"


class _Mask(IntFlag):
    READ = 1


class _Ratio(float, Enum):
    HALF = 0.5


class _Amount(Decimal, Enum):
    ONE = Decimal("1")


class _Day(date, Enum):
    FIRST = (2026, 1, 1)


class _Enums(BaseModel):
    __tablename__ = "enums"
    status: _Status
    sized: _Status = ColumnField(length=12)
    maybe: _Status | None = None
    priority: _Priority = _Priority.HIGH
    colour: _Colour = ColumnField(String(8), default=_Colour.RED)
    mode: Annotated[Literal["a", "b"], String(1)] = "a"
    maybe_mode: Annotated[Literal["a", "b"], String(1)] | None = None
    annotated_maybe: Annotated[_Status | None, Field(length=12)] = None


class _MixedEnums(BaseModel):
    __tablename__ = "mixed_enums"
    status: _Status
    priority: _Priority
    mask: _Mask
    ratio: _Ratio
    amount: _Amount
    day: _Day


class _PlainEnum(BaseModel):
    __tablename__ = "plain_enums"
    colour: _Colour


class _Literal(BaseModel):
    __tablename__ = "literals"
    mode: Literal["a", "b"] | None = ColumnField(nullable=True)


class _AnnotatedLiteral(BaseModel):
    __tablename__ = "annotated_literals"
    mode: Annotated[Literal["a", "b"], "a note"] = "a"


class TestEnumColumnTypes:
    def test_a_str_enum_follows_the_str_rule(self) -> None:
        fields = get_column_fields(_Enums)

        assert fields["status"].column_type == String()
        assert fields["sized"].column_type == String(12)

    def test_an_optional_str_enum_follows_the_str_rule(self) -> None:
        assert get_column_fields(_Enums)["maybe"].column_type == String()

    def test_an_int_enum_follows_the_int_rule(self) -> None:
        assert get_column_fields(_Enums)["priority"].column_type == Integer

    def test_a_declared_column_type_wins(self) -> None:
        fields = get_column_fields(_Enums)

        assert fields["colour"].column_type == String(8)
        assert fields["mode"].column_type == String(1)

    def test_annotated_metadata_inside_optional_or_around_it_is_honoured(self) -> None:
        fields = get_column_fields(_Enums)

        assert fields["maybe_mode"].column_type == String(1)
        assert fields["annotated_maybe"].column_type == String(12)

    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("status", String()),
            ("priority", Integer),
            ("mask", Integer),
            ("ratio", Float),
            ("amount", Numeric()),
            ("day", String(None)),
        ],
    )
    def test_an_enum_follows_the_scalar_type_it_is_mixed_with(
        self, field: str, expected: ColumnType
    ) -> None:
        info = get_column_fields(_MixedEnums)[field]

        assert (info.column_type, info.needs_declared_type) == (expected, False)

    @pytest.mark.parametrize(
        ("model", "field"),
        [(_PlainEnum, "colour"), (_Literal, "mode"), (_AnnotatedLiteral, "mode")],
    )
    def test_a_plain_enum_or_literal_without_a_column_type_needs_one_declared(
        self, model: type, field: str
    ) -> None:
        info = get_column_fields(model)[field]

        assert (info.column_type, info.needs_declared_type) == (JSON, True)

    def test_declared_and_inferable_fields_need_no_declared_type(self) -> None:
        assert not any(info.needs_declared_type for info in get_column_fields(_Enums).values())
