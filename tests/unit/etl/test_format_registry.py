"""Unit tests for backend format dispatch helper."""

from __future__ import annotations

import pytest

from loom.core.errors.codes import ErrorCode
from loom.etl import UnsupportedFormatError as public_unsupported_format
from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import (
    CsvWriteOptions,
    JsonWriteOptions,
    ParquetWriteOptions,
)
from loom.etl.engines._format_registry import (
    UnsupportedFormatError,
    resolve_format_handler,
    write_options_or_default,
)


class TestFormatRegistry:
    def test_resolve_format_handler_accepts_enum(self) -> None:
        handlers = {Format.CSV: "csv-handler"}
        assert resolve_format_handler(Format.CSV, handlers) == "csv-handler"

    def test_resolve_format_handler_accepts_string_value(self) -> None:
        handlers = {Format.JSON: "json-handler"}
        assert resolve_format_handler("json", handlers) == "json-handler"

    def test_resolve_format_handler_raises_for_unregistered_format(self) -> None:
        handlers = {Format.CSV: "csv-handler"}
        with pytest.raises(ValueError, match="Unsupported format: json"):
            resolve_format_handler(Format.JSON, handlers)

    def test_resolve_format_handler_raises_for_unknown_string(self) -> None:
        handlers = {Format.CSV: "csv-handler"}
        with pytest.raises(ValueError, match="'xml' is not a valid Format"):
            resolve_format_handler("xml", handlers)

    def test_an_unsupported_format_carries_a_machine_readable_code(self) -> None:
        handlers = {Format.CSV: "csv-handler"}
        with pytest.raises(UnsupportedFormatError) as excinfo:
            resolve_format_handler(Format.XLSX, handlers)
        assert excinfo.value.code == "unsupported_format"

    def test_an_unsupported_format_names_the_formats_that_are_supported(self) -> None:
        handlers = {Format.CSV: "csv-handler", Format.PARQUET: "parquet-handler"}
        with pytest.raises(UnsupportedFormatError, match="csv, parquet"):
            resolve_format_handler(Format.XLSX, handlers)

    def test_an_unsupported_format_is_still_a_value_error(self) -> None:
        handlers: dict[Format, str] = {}
        with pytest.raises(ValueError, match="Unsupported format: csv"):
            resolve_format_handler(Format.CSV, handlers)

    def test_matching_write_options_are_used_as_declared(self) -> None:
        declared = CsvWriteOptions(separator=";")
        assert write_options_or_default(declared, CsvWriteOptions) is declared

    def test_absent_write_options_fall_back_to_the_format_defaults(self) -> None:
        assert write_options_or_default(None, ParquetWriteOptions) == ParquetWriteOptions()

    def test_write_options_of_another_format_fall_back_to_the_defaults(self) -> None:
        assert write_options_or_default(JsonWriteOptions(), CsvWriteOptions) == CsvWriteOptions()

    def test_an_unsupported_format_uses_a_canonical_error_code(self) -> None:
        handlers = {Format.CSV: "csv-handler"}
        with pytest.raises(UnsupportedFormatError) as excinfo:
            resolve_format_handler(Format.XLSX, handlers)
        assert excinfo.value.code == ErrorCode.UNSUPPORTED_FORMAT

    def test_an_unsupported_format_exposes_the_formats_as_data(self) -> None:
        handlers = {Format.PARQUET: "parquet-handler", Format.CSV: "csv-handler"}
        with pytest.raises(UnsupportedFormatError) as excinfo:
            resolve_format_handler(Format.XLSX, handlers)
        assert excinfo.value.format is Format.XLSX
        assert excinfo.value.supported == (Format.CSV, Format.PARQUET)

    def test_the_error_is_catchable_from_the_public_surface(self) -> None:
        assert public_unsupported_format is UnsupportedFormatError
