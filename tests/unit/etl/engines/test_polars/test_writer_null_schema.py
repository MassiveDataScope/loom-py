"""Unit tests for null-dtype column detection before Delta write."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.engines.polars._writer import _check_null_dtype_columns
from loom.etl.schema._schema import SchemaError


class TestNullDtypeColumnCheck:
    def test_check_passes_for_typed_columns(self) -> None:
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "score": [1.0, None]})
        _check_null_dtype_columns(df)  # must not raise

    def test_check_raises_for_single_null_column(self) -> None:
        df = pl.DataFrame({"id": [1, 2], "missing": pl.Series([None, None], dtype=pl.Null)})
        with pytest.raises(SchemaError) as exc_info:
            _check_null_dtype_columns(df)
        error = str(exc_info.value)
        assert "missing" in error
        assert "Null" in error

    def test_check_raises_for_multiple_null_columns(self) -> None:
        df = pl.DataFrame(
            {
                "id": [1],
                "col_a": pl.Series([None], dtype=pl.Null),
                "name": ["ok"],
                "col_b": pl.Series([None], dtype=pl.Null),
            }
        )
        with pytest.raises(SchemaError) as exc_info:
            _check_null_dtype_columns(df)
        error = str(exc_info.value)
        # Both null columns must appear in the error
        assert "col_a" in error
        assert "col_b" in error
        # The typed column must not appear in the null-columns list (first line)
        first_line = error.splitlines()[0]
        assert "name" not in first_line

    def test_check_error_mentions_delta(self) -> None:
        df = pl.DataFrame({"bad": pl.Series([None], dtype=pl.Null)})
        with pytest.raises(SchemaError) as exc_info:
            _check_null_dtype_columns(df)
        assert "Delta" in str(exc_info.value)

    def test_check_passes_for_empty_dataframe(self) -> None:
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
        _check_null_dtype_columns(df)  # must not raise
