"""A JSON column that does not decode fails naming the declared column, on every engine."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any

import msgspec
import polars as pl
import pytest

from loom.core.errors import InvalidValue
from loom.core.model.types import Numeric
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import JsonMismatch
from loom.etl.executor import ETLExecutor
from tests.unit.etl.engines.doubles import ModelEngine, NoParams, PolarsEngine
from tests.unit.etl.models import Buyer

_RECOGNISED = {
    "json-document": (
        pl.exceptions.ComputeError("error deserializing JSON: json parsing error"),
        True,
    ),
    "parquet-metadata": (
        pl.exceptions.ComputeError("error deserializing parquet metadata"),
        False,
    ),
    "not-a-compute-error": (ValueError("error deserializing JSON"), False),
}


class _DecodeBuyer(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        return customers


class _OwnJsonFailure(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers")
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        raise pl.exceptions.ComputeError("error deserializing JSON: json parsing error")


def _run(engine: ModelEngine, step: type[ETLStep[Any]]) -> None:
    ETLExecutor(engine.reader, engine.writer).run_step(ETLCompiler().compile_step(step), NoParams())


def _seed_customers(engine: ModelEngine, document: str) -> None:
    engine.writer.write(
        engine.frame({"buyer": [document]}), IntoTable("raw.customers").append()._to_spec(), None
    )


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self, engine: ModelEngine, document: str
    ) -> None:
        _seed_customers(engine, document)

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            _run(engine, _DecodeBuyer)

    def test_a_step_declaring_no_json_column_keeps_the_failure_of_its_own_code(
        self, polars_engine: PolarsEngine
    ) -> None:
        _seed_customers(polars_engine, "{}")

        with pytest.raises(pl.exceptions.ComputeError):
            _run(polars_engine, _OwnJsonFailure)

    @pytest.mark.parametrize(("error", "recognised"), _RECOGNISED.values(), ids=list(_RECOGNISED))
    def test_polars_recognises_only_a_json_document_refusal(
        self, polars_engine: PolarsEngine, error: Exception, recognised: bool
    ) -> None:
        assert polars_engine.reader.is_json_decode_failure(error) is recognised


_PRICE = Annotated[Decimal, Numeric(precision=10, scale=2)]
_NULLED_FIELD = {
    "int-from-string": (int, '"oops"'),
    "int-from-float": (int, "1.5"),
    "int-from-bool": (int, "true"),
    "int-from-object": (int, '{"a": 1}'),
    "float-from-string": (float, '"oops"'),
    "float-from-bool": (float, "true"),
    "float-from-array": (float, "[1]"),
    "bool-from-number": (bool, "1"),
    "bool-from-string": (bool, '"true"'),
    "decimal-from-string": (_PRICE, '"oops"'),
    "decimal-from-bool": (_PRICE, "true"),
    "datetime-from-string": (datetime, '"nope"'),
    "datetime-from-object": (datetime, '{"a": 1}'),
    "date-from-string": (date, '"hello"'),
    "date-from-number": (date, "5"),
    "str-from-bool": (str, "true"),
}


class TestNullModeNeverRaises:
    @pytest.mark.parametrize(
        ("annotation", "value"), _NULLED_FIELD.values(), ids=list(_NULLED_FIELD)
    )
    def test_a_wrongly_typed_field_is_nulled_and_its_siblings_are_kept(
        self, engine: ModelEngine, annotation: Any, value: str
    ) -> None:
        probe = msgspec.defstruct("Probe", [("v", annotation | None), ("w", int)])
        _seed_customers(engine, f'{{"v": {value}, "w": 9}}')
        source = FromTable("raw.customers").parse_json(
            "buyer", probe, on_mismatch=JsonMismatch.NULL
        )
        decoded = engine.reader.read(source._to_spec("customers"), None)
        engine.writer.write(decoded, IntoTable("staging.customers").replace()._to_spec(), None)

        (row,) = engine.stored("staging.customers")

        assert row["buyer"]["w"] == 9
        assert row["buyer"]["v"] == ("true" if annotation is str else None)
