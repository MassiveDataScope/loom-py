"""The lake table of a model: what each column kind becomes, and what the lake refuses."""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from typing import Annotated, Any

import pytest

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model import BaseModel
from loom.core.model.inference import Outcome, ResolvedField
from loom.core.model.types import (
    ColumnKind,
    ColumnType,
    Numeric,
    Postgres,
)
from loom.etl.schema import (
    ColumnSchema,
    DatetimeType,
    FrameType,
    ListType,
    LoomDtype,
    ModelTable,
    StructType,
)
from loom.etl.schema._model import _REFUSED_KINDS, LakeTranslator
from tests.unit.etl.engines.frame_types import LAKE_ROWS, Priced, probe_model

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


_BY_KIND_REFUSED = (*_REFUSED_KINDS, ColumnKind.NUMERIC)


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

    @pytest.mark.parametrize("kind", [kind for kind in ColumnKind if kind not in _BY_KIND_REFUSED])
    def test_every_column_kind_the_lake_stores_translates(self, kind: ColumnKind) -> None:
        field = ResolvedField("value", "value", ColumnType(kind), True, Outcome.DECLARED)

        assert isinstance(
            LakeTranslator("Probe").frame_type(field),
            LoomDtype | DatetimeType | ListType | StructType,
        )

    def test_the_table_carries_the_model_and_its_logical_name(self) -> None:
        model = probe_model(int)

        table = ModelTable.from_model(model)

        assert (table.model, table.name) == (model, "probes")

    def test_a_model_without_a_table_name_is_refused(self) -> None:
        with pytest.raises(InvalidType, match="__tablename__"):
            ModelTable.from_model(type("Nameless", (BaseModel,), {"__annotations__": {"id": int}}))
