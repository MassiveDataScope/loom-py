"""Unit tests for PolarsFileWriter dispatch and streaming behavior."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import CsvWriteOptions
from loom.etl.declarative.target._file import FileSpec
from loom.etl.engines._format_registry import UnsupportedFormatError
from loom.etl.engines.polars import _file_writer as file_mod
from loom.etl.engines.polars._file_writer import PolarsFileWriter


class TestPolarsFileWriteDispatch:
    def test_write_streaming_dispatches_to_sink_writer(self, monkeypatch) -> None:
        calls: list[tuple[str, str]] = []

        def _spy_sink(frame: pl.LazyFrame, _path: str, _options: object) -> None:
            calls.append(("sink", type(frame).__name__))

        def _spy_collect(_df: pl.DataFrame, _path: str, _options: object) -> None:
            calls.append(("collect", "DataFrame"))

        monkeypatch.setitem(file_mod._STREAMING_WRITERS, Format.CSV, _spy_sink)
        monkeypatch.setitem(file_mod._FILE_WRITERS, Format.CSV, _spy_collect)

        writer = PolarsFileWriter()
        writer.write(
            pl.DataFrame({"id": [1]}).lazy(),
            FileSpec(path="out.csv", format=Format.CSV),
            streaming=True,
        )

        assert calls == [("sink", "LazyFrame")]

    def test_write_non_streaming_dispatches_to_collect_writer(self, monkeypatch) -> None:
        calls: list[tuple[str, str]] = []

        def _spy_sink(_frame: pl.LazyFrame, _path: str, _options: object) -> None:
            calls.append(("sink", "LazyFrame"))

        def _spy_collect(df: pl.DataFrame, _path: str, _options: object) -> None:
            calls.append(("collect", type(df).__name__))

        monkeypatch.setitem(file_mod._STREAMING_WRITERS, Format.CSV, _spy_sink)
        monkeypatch.setitem(file_mod._FILE_WRITERS, Format.CSV, _spy_collect)

        writer = PolarsFileWriter()
        writer.write(
            pl.DataFrame({"id": [1]}).lazy(),
            FileSpec(path="out.csv", format=Format.CSV),
            streaming=False,
        )

        assert calls == [("collect", "DataFrame")]

    def test_streaming_csv_write_applies_options(self, tmp_path: Path) -> None:
        out = tmp_path / "sink.csv"
        writer = PolarsFileWriter()
        spec = FileSpec(
            path=str(out),
            format=Format.CSV,
            write_options=CsvWriteOptions(separator=";", has_header=False),
        )

        writer.write(pl.DataFrame({"id": [1], "value": [10]}).lazy(), spec, streaming=True)

        assert out.read_text(encoding="utf-8").strip() == "1;10"


class TestUnsupportedPolarsFileFormat:
    def test_a_format_polars_cannot_write_fails_with_a_coded_error(self) -> None:
        writer = PolarsFileWriter()
        spec = FileSpec(path="out.xlsx", format=Format.XLSX)
        frame = pl.DataFrame({"id": [1]}).lazy()

        with pytest.raises(UnsupportedFormatError) as excinfo:
            writer.write(frame, spec, streaming=False)

        assert excinfo.value.code == "unsupported_format"
        assert "xlsx" in str(excinfo.value)
        assert "csv" in str(excinfo.value)

    def test_a_format_polars_cannot_stream_fails_with_a_coded_error(self) -> None:
        writer = PolarsFileWriter()
        spec = FileSpec(path="out.xlsx", format=Format.XLSX)
        frame = pl.DataFrame({"id": [1]}).lazy()

        with pytest.raises(UnsupportedFormatError) as excinfo:
            writer.write(frame, spec, streaming=True)

        assert excinfo.value.code == "unsupported_format"
        assert "xlsx" in str(excinfo.value)

    def test_an_unwritable_format_stays_catchable_as_a_value_error(self) -> None:
        writer = PolarsFileWriter()
        spec = FileSpec(path="out.xlsx", format=Format.XLSX)
        frame = pl.DataFrame({"id": [1]}).lazy()

        with pytest.raises(ValueError, match="Unsupported format: xlsx"):
            writer.write(frame, spec, streaming=False)

    def test_an_unwritable_streaming_format_names_the_supported_formats(self) -> None:
        writer = PolarsFileWriter()
        spec = FileSpec(path="out.xlsx", format=Format.XLSX)
        frame = pl.DataFrame({"id": [1]}).lazy()

        with pytest.raises(UnsupportedFormatError) as excinfo:
            writer.write(frame, spec, streaming=True)

        assert excinfo.value.supported == (Format.CSV, Format.JSON, Format.PARQUET)
