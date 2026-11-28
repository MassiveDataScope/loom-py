"""A JSON column reads the same rows on Polars and Spark, and each engine its own."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import polars as pl
import pytest

from loom.core.errors import InvalidValue
from loom.etl import ETLStep, FromTable, IntoTable, col
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import JsonMismatch
from loom.etl.executor import ETLExecutor
from loom.etl.schema import JsonRefusal
from tests.unit.etl.engines.doubles import ModelEngine, NoParams, PolarsEngine, SparkEngine
from tests.unit.etl.engines.json_readings import (
    COMMON_FITS,
    COMMON_MISFITS,
    NAME,
    POLARS_FITS,
    POLARS_MISFITS,
    SIBLING,
    SPARK_FITS,
    SPARK_MISFITS,
    Fit,
    Misfit,
    probe,
    probe_document,
)
from tests.unit.etl.models import Buyer

_NAMED = r"JSON column.*buyer"
_POLARS_FITS = [*COMMON_FITS, *POLARS_FITS]
_POLARS_MISFITS = [*COMMON_MISFITS, *POLARS_MISFITS]
_SPARK_FITS = [*COMMON_FITS, *SPARK_FITS]
_SPARK_MISFITS = [*COMMON_MISFITS, *SPARK_MISFITS]


def _seed_customers(engine: ModelEngine, *documents: str) -> None:
    engine.writer.write(
        engine.frame({"buyer": list(documents)}),
        IntoTable("raw.customers").append()._to_spec(),
        None,
    )


def _seeded(engine: ModelEngine, rows: Sequence[Fit | Misfit]) -> ModelEngine:
    engine.writer.write(
        engine.frame(
            {"reading": [row.name for row in rows], "buyer": [probe_document(r) for r in rows]}
        ),
        IntoTable("raw.readings").append()._to_spec(),
        None,
    )
    return engine


@pytest.fixture(scope="module")
def seeded_polars(tmp_path_factory: pytest.TempPathFactory) -> ModelEngine:
    """A Polars engine whose ``raw.readings`` holds the document of every Polars row."""
    rows = [*_POLARS_FITS, *_POLARS_MISFITS]
    return _seeded(PolarsEngine(tmp_path_factory.mktemp("polars")), rows)


@pytest.fixture(scope="module")
def seeded_spark(spark: Any, tmp_path_factory: pytest.TempPathFactory) -> ModelEngine:
    """A Spark engine whose ``raw.readings`` holds the document of every Spark row."""
    rows = [*_SPARK_FITS, *_SPARK_MISFITS]
    return _seeded(SparkEngine(spark, tmp_path_factory.mktemp("spark")), rows)


def _source(row: Fit | Misfit, on_mismatch: JsonMismatch) -> FromTable:
    return (
        FromTable("raw.readings")
        .where(col("reading") == row.name)
        .parse_json("buyer", probe(row.frame_type), on_mismatch=on_mismatch)
    )


def _read(engine: ModelEngine, row: Fit | Misfit, on_mismatch: JsonMismatch) -> Any:
    (read,) = engine.rows(engine.reader.read(_source(row, on_mismatch)._to_spec("readings"), None))
    return read["buyer"]


def _decode(engine: ModelEngine, row: Fit | Misfit) -> None:
    class _Decode(ETLStep[NoParams]):
        readings: FromTable = _source(row, JsonMismatch.FAIL)  # type: ignore[assignment]
        target = IntoTable("staging.readings").replace()

        def execute(self, params: NoParams, *, readings: object) -> object:  # type: ignore[override]
            return readings

    _run(engine, _Decode)


def _run(engine: ModelEngine, step: type[ETLStep[Any]]) -> None:
    ETLExecutor(engine.reader, engine.writer).run_step(ETLCompiler().compile_step(step), NoParams())


class TestPolarsReadings:
    @pytest.mark.parametrize("row", _POLARS_FITS, ids=NAME)
    @pytest.mark.parametrize("policy", list(JsonMismatch), ids=lambda policy: policy.value)
    def test_a_value_that_fits_reads_under_both_policies(
        self, seeded_polars: ModelEngine, row: Fit, policy: JsonMismatch
    ) -> None:
        assert _read(seeded_polars, row, policy) == {"v": row.value, "w": SIBLING}

    @pytest.mark.parametrize("row", _POLARS_MISFITS, ids=NAME)
    def test_null_policy_nulls_the_misfit_and_keeps_its_sibling(
        self, seeded_polars: ModelEngine, row: Misfit
    ) -> None:
        assert _read(seeded_polars, row, JsonMismatch.NULL) == {"v": row.null, "w": SIBLING}

    @pytest.mark.parametrize("row", _POLARS_MISFITS, ids=NAME)
    def test_fail_policy_refuses_naming_the_path_the_rule_and_the_column(
        self, seeded_polars: ModelEngine, row: Misfit
    ) -> None:
        with pytest.raises(
            JsonRefusal, match=re.escape(f"{row.path} expected {row.rule}, in JSON column buyer")
        ):
            _decode(seeded_polars, row)


class TestSparkReadings:
    @pytest.mark.parametrize("row", _SPARK_FITS, ids=NAME)
    @pytest.mark.parametrize("policy", list(JsonMismatch), ids=lambda policy: policy.value)
    def test_a_value_that_fits_reads_under_both_policies(
        self, seeded_spark: ModelEngine, row: Fit, policy: JsonMismatch
    ) -> None:
        assert _read(seeded_spark, row, policy) == {"v": row.value, "w": SIBLING}

    @pytest.mark.parametrize("row", _SPARK_MISFITS, ids=NAME)
    def test_null_policy_nulls_the_misfit(self, seeded_spark: ModelEngine, row: Misfit) -> None:
        assert _read(seeded_spark, row, JsonMismatch.NULL) == {"v": row.null, "w": SIBLING}

    @pytest.mark.parametrize("row", _SPARK_MISFITS, ids=NAME)
    def test_fail_policy_refuses_naming_the_column(
        self, seeded_spark: ModelEngine, row: Misfit
    ) -> None:
        with pytest.raises(InvalidValue, match=_NAMED):
            _decode(seeded_spark, row)


class _OwnJsonFailure(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers")
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        raise pl.exceptions.ComputeError("error deserializing JSON: json parsing error")


class _OwnJsonFailureOverDecodedColumn(_OwnJsonFailure):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]


class _DecodeBuyers(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        return customers


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self, engine: ModelEngine, document: str
    ) -> None:
        _seed_customers(engine, document)

        with pytest.raises(InvalidValue, match=_NAMED):
            _run(engine, _DecodeBuyers)

    @pytest.mark.parametrize(
        "step",
        [_OwnJsonFailure, _OwnJsonFailureOverDecodedColumn],
        ids=["no-json-column", "decoded-column"],
    )
    def test_a_failure_of_the_steps_own_code_is_kept(
        self, polars_engine: PolarsEngine, step: type[ETLStep[Any]]
    ) -> None:
        _seed_customers(polars_engine, '{"name": "Ada", "order_count": 1}')

        with pytest.raises(pl.exceptions.ComputeError, match="error deserializing JSON"):
            _run(polars_engine, step)
