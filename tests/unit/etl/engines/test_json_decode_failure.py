"""A JSON column that does not decode fails naming the declared column, on every engine."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from loom.core.errors import InvalidValue
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor
from tests.unit.etl.engines.doubles import ModelEngine, NoParams, PolarsEngine
from tests.unit.etl.models import Buyer

_JSON_REFUSAL = pl.exceptions.ComputeError("error deserializing JSON: json parsing error")
_RECOGNISED = {
    "json-document": (_JSON_REFUSAL, True),
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
        raise _JSON_REFUSAL


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
