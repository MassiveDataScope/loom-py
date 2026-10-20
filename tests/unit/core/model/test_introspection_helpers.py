"""Unit tests for the shared annotation helpers in ``model.introspection``."""

from __future__ import annotations

import typing
from datetime import date, time
from typing import Annotated, Any, Generic, TypeVar

import msgspec

from loom.core.model import JSON, BaseModel, Cardinality, ColumnField, Postgres, RelationField
from loom.core.model.introspection import (
    _union_inner_args,
    extract_model_from_hint,
    generic_type_arg,
    get_column_fields,
    list_element_type,
    resolve_type_hints,
    scalar_column_annotations,
)
from loom.core.model.types import Date, Integer, String, Time


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


class _Temporal(msgspec.Struct):
    day: date
    at: time | None = None


class TestTemporalColumnTypes:
    def test_infers_date_and_time_columns_for_date_and_time_annotations(self) -> None:
        fields = get_column_fields(_Temporal)

        assert (fields["day"].column_type, fields["at"].column_type) == (Date, Time)


class _Shelf(BaseModel):
    __tablename__ = "introspection_shelves"

    id: int = ColumnField(primary_key=True)
    label: str = ColumnField(length=2)
    tags: list[str] = ColumnField()
    words: str = ColumnField(Postgres.TSVECTOR)
    marks: list[int] = ColumnField(Postgres.ARRAY(Integer))


class _Book(BaseModel):
    __tablename__ = "introspection_books"

    id: int = ColumnField(primary_key=True)
    rank: Annotated[int, msgspec.Meta(ge=1)] | None = ColumnField()
    settings: dict[str, Any] = ColumnField(JSON)
    shelf_id: int = ColumnField(foreign_key="introspection_shelves.id")
    shelf: _Shelf | None = RelationField(
        foreign_key="shelf_id", cardinality=Cardinality.MANY_TO_ONE
    )


class TestScalarColumnAnnotations:
    def test_scalar_columns_map_to_their_annotation_without_metadata(self) -> None:
        assert scalar_column_annotations(_Book) == {"id": int, "rank": int | None, "shelf_id": int}

    def test_json_array_and_text_search_columns_are_not_scalar(self) -> None:
        assert set(scalar_column_annotations(_Shelf)) == {"id", "label"}
