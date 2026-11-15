"""Unit tests for contract introspection in loom.etl._contract."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any

import msgspec
import pytest

from loom.etl.schema._contract import (
    _strip_optional,
    annotation_to_loom_type,
    resolve_json_type,
    resolve_schema,
)
from loom.etl.schema._schema import (
    ColumnSchema,
    DatetimeType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)


class PlainRow:
    id: int
    name: str
    score: float


@dataclasses.dataclass
class DataclassRow:
    order_id: int
    amount: float
    active: bool


class MsgspecRow(msgspec.Struct):
    user_id: int
    email: str
    age: float


class NestedAddress(msgspec.Struct):
    street: str
    zip_code: str


class NestedRow(msgspec.Struct):
    id: int
    address: NestedAddress


class RowWithOptional(msgspec.Struct):
    id: int
    label: str | None


class NoAnnotations:
    pass


class TestResolveSchema:
    @pytest.mark.parametrize(
        "contract,expected",
        [
            (
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("name", LoomDtype.UTF8),
                ),
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("name", LoomDtype.UTF8),
                ),
            ),
            ((), ()),
            (
                PlainRow,
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("name", LoomDtype.UTF8),
                    ColumnSchema("score", LoomDtype.FLOAT64),
                ),
            ),
            (
                DataclassRow,
                (
                    ColumnSchema("order_id", LoomDtype.INT64),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                    ColumnSchema("active", LoomDtype.BOOLEAN),
                ),
            ),
            (
                MsgspecRow,
                (
                    ColumnSchema("user_id", LoomDtype.INT64),
                    ColumnSchema("email", LoomDtype.UTF8),
                    ColumnSchema("age", LoomDtype.FLOAT64),
                ),
            ),
            (
                RowWithOptional,
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("label", LoomDtype.UTF8),
                ),
            ),
        ],
    )
    def test_resolve_schema_contracts(
        self,
        contract: tuple[ColumnSchema, ...] | type[Any],
        expected: tuple[ColumnSchema, ...],
    ) -> None:
        assert resolve_schema(contract) == expected

    @pytest.mark.parametrize(
        "contract,error",
        [
            (NoAnnotations, "no type annotations"),
            ("not_a_class", "annotated class"),
        ],
    )
    def test_resolve_schema_errors(self, contract: object, error: str) -> None:
        with pytest.raises(TypeError, match=error):
            resolve_schema(contract)  # type: ignore[arg-type]


class TestResolveJsonType:
    @pytest.mark.parametrize(
        "contract",
        [
            LoomDtype.UTF8,
            StructType(fields=(StructField("x", LoomDtype.FLOAT64),)),
            ListType(inner=LoomDtype.INT64),
            DatetimeType("us", "UTC"),
        ],
    )
    def test_passthrough_loom_types(self, contract: Any) -> None:
        assert resolve_json_type(contract) == contract

    @pytest.mark.parametrize(
        "contract,expected",
        [
            (
                MsgspecRow,
                StructType(
                    fields=(
                        StructField("user_id", LoomDtype.INT64),
                        StructField("email", LoomDtype.UTF8),
                        StructField("age", LoomDtype.FLOAT64),
                    )
                ),
            ),
            (
                NestedRow,
                StructType(
                    fields=(
                        StructField("id", LoomDtype.INT64),
                        StructField(
                            "address",
                            StructType(
                                fields=(
                                    StructField("street", LoomDtype.UTF8),
                                    StructField("zip_code", LoomDtype.UTF8),
                                )
                            ),
                        ),
                    )
                ),
            ),
            (list[str], ListType(inner=LoomDtype.UTF8)),
            (list[int], ListType(inner=LoomDtype.INT64)),
            (
                list[MsgspecRow],
                ListType(
                    inner=StructType(
                        fields=(
                            StructField("user_id", LoomDtype.INT64),
                            StructField("email", LoomDtype.UTF8),
                            StructField("age", LoomDtype.FLOAT64),
                        )
                    )
                ),
            ),
        ],
    )
    def test_resolve_json_type_contracts(self, contract: Any, expected: Any) -> None:
        assert resolve_json_type(contract) == expected

    def test_resolve_json_type_unknown_raises(self) -> None:
        with pytest.raises(TypeError, match="annotated class"):
            resolve_json_type(42)


class TestAnnotationConversion:
    @pytest.mark.parametrize(
        "annotation,expected",
        [
            (int, LoomDtype.INT64),
            (float, LoomDtype.FLOAT64),
            (str, LoomDtype.UTF8),
            (bool, LoomDtype.BOOLEAN),
            (bytes, LoomDtype.BINARY),
            (datetime.datetime, LoomDtype.DATETIME),
            (datetime.date, LoomDtype.DATE),
            (list[str], ListType(inner=LoomDtype.UTF8)),
            (
                NestedAddress,
                StructType(
                    fields=(
                        StructField("street", LoomDtype.UTF8),
                        StructField("zip_code", LoomDtype.UTF8),
                    )
                ),
            ),
            (str | None, LoomDtype.UTF8),
        ],
    )
    def testannotation_to_loom_type(self, annotation: Any, expected: Any) -> None:
        assert annotation_to_loom_type(annotation) == expected

    def test_annotation_to_loom_type_unknown_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert annotation"):
            annotation_to_loom_type(object)


class TestStripOptional:
    @pytest.mark.parametrize(
        "annotation,is_optional,inner",
        [
            (str | None, True, str),
            (int, False, int),
            (str | int | None, False, str | int | None),
        ],
    )
    def test_strip_optional(
        self,
        annotation: Any,
        is_optional: bool,
        inner: Any,
    ) -> None:
        actual_is_optional, actual_inner = _strip_optional(annotation)
        assert actual_is_optional is is_optional
        assert actual_inner == inner
