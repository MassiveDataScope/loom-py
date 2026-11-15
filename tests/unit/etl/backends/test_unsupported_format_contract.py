"""Both backends refuse a format they cannot write with the same coded error."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.backends._format_registry import UnsupportedFormatError, resolve_format_handler
from loom.etl.backends.polars._file_writer import PolarsFileWriter
from loom.etl.declarative._format import Format
from loom.etl.declarative.target._file import FileSpec
from tests.helpers.version_limited_extras import require_pyspark

require_pyspark()

from loom.etl.backends.spark import _writer as spark_writer  # noqa: E402


class TestUnsupportedFormatContract:
    def test_spark_refuses_a_format_it_cannot_write_with_the_coded_error(self) -> None:
        with pytest.raises(UnsupportedFormatError) as excinfo:
            resolve_format_handler(Format.XLSX, spark_writer._FILE_WRITERS)

        assert excinfo.value.format is Format.XLSX

    def test_spark_does_not_advertise_a_format_it_cannot_write(self) -> None:
        with pytest.raises(UnsupportedFormatError) as excinfo:
            resolve_format_handler(Format.XLSX, spark_writer._FILE_WRITERS)

        assert "xlsx" not in str(excinfo.value).split("Supported here:")[1]

    def test_polars_refuses_the_same_format_with_the_same_error(self) -> None:
        writer = PolarsFileWriter()
        spec = FileSpec(path="out.xlsx", format=Format.XLSX)
        frame = pl.DataFrame({"id": [1]}).lazy()

        with pytest.raises(UnsupportedFormatError):
            writer.write(frame, spec)
