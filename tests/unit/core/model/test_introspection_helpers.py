"""Unit tests for the shared annotation helpers in ``model.introspection``."""

from __future__ import annotations

import re
import typing
from typing import Annotated, Any, Generic, TypeVar

import msgspec
import pytest

from loom.core.model import JSON, BaseModel, Cardinality, ColumnField, RelationField
from loom.core.model.introspection import (
    _union_inner_args,
    extract_model_from_hint,
    filter_field_annotations,
    generic_type_arg,
    get_column_fields,
    list_element_type,
    resolve_type_hints,
)
from loom.core.model.types import Integer, String


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


class _Shelf(BaseModel):
    __tablename__ = "introspection_shelves"

    id: int = ColumnField(primary_key=True)
    label: str = ColumnField(length=2)
    tags: list[str] = ColumnField()


class _Book(BaseModel):
    __tablename__ = "introspection_books"

    id: int = ColumnField(primary_key=True)
    rank: Annotated[int, msgspec.Meta(ge=1)] | None = ColumnField()
    settings: dict[str, Any] = ColumnField(JSON)
    shelf_id: int = ColumnField(foreign_key="introspection_shelves.id")
    shelf: _Shelf | None = RelationField(
        foreign_key="shelf_id", cardinality=Cardinality.MANY_TO_ONE
    )


class TestFilterFieldAnnotations:
    def test_declared_paths_resolve_to_their_annotation_without_metadata(self) -> None:
        annotations = filter_field_annotations(_Book, ("rank", "shelf.label"))

        assert annotations == {"rank": int | None, "shelf.label": str}

    def test_without_paths_every_scalar_column_one_relation_down_is_filterable(self) -> None:
        assert set(filter_field_annotations(_Book)) == {
            "id",
            "rank",
            "shelf_id",
            "shelf.id",
            "shelf.label",
        }

    @pytest.mark.parametrize(
        "path",
        ["missing", "settings", "shelf.tags", "shelf", "shelf.label.code", "shelf.missing"],
    )
    def test_a_path_that_is_not_a_scalar_column_is_refused(self, path: str) -> None:
        with pytest.raises(ValueError, match=re.escape(repr(path))):
            filter_field_annotations(_Book, (path,))
