"""The lake table of a model: what each column kind becomes, and what the lake refuses."""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from typing import Annotated, Any

import pytest

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model import BaseModel
from loom.core.model.types import Numeric, Postgres
from loom.etl.schema import ColumnSchema, FrameType, ModelTable
from tests.unit.etl.models import LAKE_ROWS, Priced, probe_model

_REFUSALS = {
    "unbounded-numeric": (Annotated[Decimal, Numeric()], "unbounded Numeric"),
    "bare-decimal": (Decimal, r"no column type \(a Decimal\)"),
    "wide-numeric": (Annotated[Decimal, Numeric(precision=39)], "precision 39"),
    "time": (time, "Time"),
    "tsvector": (Annotated[str, Postgres.TSVECTOR], "tsvector"),
    "undeclared": (Any, "no column type"),
    "sub-entity-decimal": (Priced, r"Probe\.value\.price .*no column type \(a Decimal\)"),
    "list-of-sub-entity-decimal": (list[Priced], r"Probe\.value\.item\.price"),
}


class TestLakeTable:
    @pytest.mark.parametrize(("annotation", "expected"), LAKE_ROWS.values(), ids=list(LAKE_ROWS))
    def test_a_column_becomes_its_lake_type(self, annotation: Any, expected: FrameType) -> None:
        table = ModelTable.from_model(probe_model(annotation))

        assert table.schema == (ColumnSchema("value", expected),)

    @pytest.mark.parametrize(("annotation", "message"), _REFUSALS.values(), ids=list(_REFUSALS))
    def test_a_column_the_lake_cannot_hold_is_refused_naming_the_field(
        self, annotation: Any, message: str
    ) -> None:
        with pytest.raises(InvalidValue, match=message):
            ModelTable.from_model(probe_model(annotation))

    def test_a_model_is_translated_once(self) -> None:
        model = probe_model(int)

        assert ModelTable.from_model(model) is ModelTable.from_model(model)

    def test_a_model_without_a_table_name_is_refused(self) -> None:
        with pytest.raises(InvalidType, match="__tablename__"):
            ModelTable.from_model(type("Nameless", (BaseModel,), {"__annotations__": {"id": int}}))
