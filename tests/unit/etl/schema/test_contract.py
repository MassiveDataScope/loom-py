"""Unit tests for contract introspection in loom.etl._contract."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any

import msgspec
import pytest

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model import BaseModel
from loom.etl.schema import LakeTranslator, resolve_schema
from loom.etl.schema._schema import (
    ColumnSchema,
    DatetimeType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.models import Priced


class MsgspecRow(msgspec.Struct):
    user_id: int
    email: str
    age: float


class RowWithOptional(msgspec.Struct):
    id: int
    label: str | None


class NoAnnotations:
    pass


class OrderModel(BaseModel):
    __tablename__ = "orders"
    id: int


class SameStruct(msgspec.Struct):
    id: int
    placed_at: datetime.datetime
    tags: list[str]


@dataclasses.dataclass
class SameDataclass:
    id: int
    placed_at: datetime.datetime
    tags: list[str]


class SamePlain:
    id: int
    placed_at: datetime.datetime
    tags: list[str]


class SameModel(BaseModel):
    __tablename__ = "same"
    id: int
    placed_at: datetime.datetime
    tags: list[str]


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
                RowWithOptional,
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("label", LoomDtype.UTF8),
                ),
            ),
            (OrderModel, (ColumnSchema("id", LoomDtype.INT64),)),
        ],
        ids=["tuple", "empty", "optional", "model"],
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
        ids=["no_annotations", "not_a_class"],
    )
    def test_resolve_schema_errors(self, contract: object, error: str) -> None:
        with pytest.raises(InvalidType, match=error):
            resolve_schema(contract)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "contract", [SameStruct, SameDataclass, SamePlain, SameModel], ids=lambda c: c.__name__
    )
    def test_the_same_annotations_resolve_to_the_same_schema(self, contract: type[Any]) -> None:
        assert resolve_schema(contract) == (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("placed_at", DatetimeType("us", "UTC")),
            ColumnSchema("tags", ListType(LoomDtype.UTF8)),
        )

    def test_an_undeclared_decimal_is_refused_with_the_lake_message(self) -> None:
        with pytest.raises(InvalidValue, match=r"Priced\.price .*no column type \(a Decimal\)"):
            resolve_schema(Priced)

    def test_a_string_annotation_is_refused_asking_for_resolved_hints(self) -> None:
        with pytest.raises(
            InvalidValue, match=r"unresolved string annotation 'str'.*get_type_hints"
        ):
            LakeTranslator.annotation_type("str")


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
    def test_passthrough_frame_types(self, contract: Any) -> None:
        assert LakeTranslator.json_type(contract) == contract

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
            (list[str], ListType(inner=LoomDtype.UTF8)),
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
        assert LakeTranslator.json_type(contract) == expected

    def test_resolve_json_type_unknown_raises(self) -> None:
        with pytest.raises(TypeError, match="annotated class"):
            LakeTranslator.json_type(42)


class TestAnnotationConversion:
    def test_annotation_to_frame_type_unknown_raises(self) -> None:
        with pytest.raises(InvalidValue, match="not a record"):
            LakeTranslator.annotation_type(object)
