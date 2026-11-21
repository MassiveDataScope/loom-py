"""Tests for predicate pushdown — _predicate.py converter and reader integration."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from loom.etl import col
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.engines._predicate import predicate_to_polars
from loom.etl.engines.polars import PolarsSourceReader
from tests.unit.etl.doubles import RunDateETLParams

from .conftest import table_path

_PARAMS = RunDateETLParams(run_date=date(2024, 3, 15))


class TestPredicateToPolars:
    # ---------------------------------------------------------------------------
    # predicate_to_polars — unit tests (no I/O)
    # ---------------------------------------------------------------------------

    def test_eq_col_literal(self) -> None:
        expr = predicate_to_polars(col("year") == 2024, _PARAMS)
        df = pl.DataFrame({"year": [2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2024]

    def test_ne_col_literal(self) -> None:
        expr = predicate_to_polars(col("year") != 2024, _PARAMS)
        df = pl.DataFrame({"year": [2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2023, 2025]

    def test_gt_col_literal(self) -> None:
        expr = predicate_to_polars(col("v") > 10, _PARAMS)
        df = pl.DataFrame({"v": [5, 10, 15]})
        assert df.filter(expr)["v"].to_list() == [15]

    def test_ge_col_literal(self) -> None:
        expr = predicate_to_polars(col("v") >= 10, _PARAMS)
        df = pl.DataFrame({"v": [5, 10, 15]})
        assert df.filter(expr)["v"].to_list() == [10, 15]

    def test_lt_col_literal(self) -> None:
        expr = predicate_to_polars(col("v") < 10, _PARAMS)
        df = pl.DataFrame({"v": [5, 10, 15]})
        assert df.filter(expr)["v"].to_list() == [5]

    def test_le_col_literal(self) -> None:
        expr = predicate_to_polars(col("v") <= 10, _PARAMS)
        df = pl.DataFrame({"v": [5, 10, 15]})
        assert df.filter(expr)["v"].to_list() == [5, 10]

    def test_and_pred(self) -> None:
        expr = predicate_to_polars((col("year") == 2024) & (col("month") == 3), _PARAMS)
        df = pl.DataFrame({"year": [2024, 2024, 2023], "month": [3, 1, 3]})
        assert df.filter(expr).shape[0] == 1

    def test_or_pred(self) -> None:
        expr = predicate_to_polars((col("year") == 2024) | (col("year") == 2023), _PARAMS)
        df = pl.DataFrame({"year": [2022, 2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2023, 2024]

    def test_not_pred(self) -> None:
        expr = predicate_to_polars(~(col("year") == 2024), _PARAMS)
        df = pl.DataFrame({"year": [2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2023, 2025]

    def test_isin_pred(self) -> None:
        expr = predicate_to_polars(col("year").isin([2023, 2024]), _PARAMS)
        df = pl.DataFrame({"year": [2022, 2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2023, 2024]

    def test_param_expr_resolved(self) -> None:
        expr = predicate_to_polars(col("year") == p.run_date.year, _PARAMS)
        df = pl.DataFrame({"year": [2023, 2024, 2025]})
        assert df.filter(expr)["year"].to_list() == [2024]

    def test_between_shorthand(self) -> None:
        expr = predicate_to_polars(col("v").between(10, 20), _PARAMS)
        df = pl.DataFrame({"v": [5, 10, 15, 20, 25]})
        assert df.filter(expr)["v"].to_list() == [10, 15, 20]

    def test_unsupported_node_raises(self) -> None:
        with pytest.raises(TypeError, match="unsupported node type"):
            predicate_to_polars(object(), _PARAMS)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Reader integration — predicates applied as LazyFrame.filter
# ---------------------------------------------------------------------------


def _seed_partitioned(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")


def _spec_with_pred(ref: str, *predicates: object) -> TableSourceSpec:
    return TableSourceSpec(alias="data", table_ref=TableRef(ref), predicates=tuple(predicates))


class TestReaderPredicatePushdown:
    def test_reader_applies_eq_predicate(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"year": [2023, 2024, 2024], "v": [1, 2, 3]})
        _seed_partitioned(tmp_path, "raw.facts", data)
        reader = PolarsSourceReader(tmp_path)
        result = reader.read(_spec_with_pred("raw.facts", col("year") == 2024), _PARAMS).collect()
        assert result["year"].to_list() == [2024, 2024]
        assert result["v"].to_list() == [2, 3]

    def test_reader_applies_param_predicate(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"year": [2023, 2024, 2025], "v": [10, 20, 30]})
        _seed_partitioned(tmp_path, "raw.facts", data)
        reader = PolarsSourceReader(tmp_path)
        result = reader.read(
            _spec_with_pred("raw.facts", col("year") == p.run_date.year), _PARAMS
        ).collect()
        assert result["year"].to_list() == [2024]

    def test_reader_applies_compound_predicate(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"year": [2024, 2024, 2023], "month": [1, 3, 3], "v": [1, 2, 3]})
        _seed_partitioned(tmp_path, "raw.facts", data)
        reader = PolarsSourceReader(tmp_path)
        pred = (col("year") == p.run_date.year) & (col("month") == p.run_date.month)
        result = reader.read(_spec_with_pred("raw.facts", pred), _PARAMS).collect()
        assert result["v"].to_list() == [2]

    def test_reader_no_predicate_returns_full_table(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"year": [2023, 2024], "v": [1, 2]})
        _seed_partitioned(tmp_path, "raw.facts", data)
        reader = PolarsSourceReader(tmp_path)
        spec = TableSourceSpec(alias="data", table_ref=TableRef("raw.facts"))
        result = reader.read(spec, _PARAMS).collect()
        assert result.height == 2
