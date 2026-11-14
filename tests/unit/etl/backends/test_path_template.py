"""Tests for file path template resolution (FromFile/IntoFile ``{field}``)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import msgspec
import polars as pl
import pytest

from loom.etl.backends._path_template import (
    extract_template_fields,
    resolve_file_uri,
    resolve_path_template,
)
from loom.etl.backends.polars._reader import PolarsSourceReader
from loom.etl.backends.polars._writer import PolarsTargetWriter
from loom.etl.declarative._format import Format
from loom.etl.declarative.source import FileSourceSpec
from loom.etl.declarative.target._file import FileSpec


class RunDateCountryParams(msgspec.Struct, frozen=True):
    run_date: date
    country: str


PARAMS = RunDateCountryParams(run_date=date(2026, 8, 3), country="es")


class TestExtractTemplateFields:
    def test_plain_path_has_no_fields(self) -> None:
        assert extract_template_fields("s3://bucket/plain.csv") == ()

    def test_named_fields_deduplicated_in_order(self) -> None:
        path = "s3://raw/{country}/{run_date:%Y}/{country}.csv"
        assert extract_template_fields(path) == ("country", "run_date")

    def test_attribute_access_resolves_to_root_field(self) -> None:
        assert extract_template_fields("s3://raw/{run_date.month}/f.csv") == ("run_date",)

    def test_positional_placeholder_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="positional"):
            extract_template_fields("s3://raw/{}.csv")

    def test_escaped_braces_are_not_fields(self) -> None:
        assert extract_template_fields("s3://raw/{{literal}}.csv") == ()


class TestResolvePathTemplate:
    def test_plain_path_passes_through_without_params(self) -> None:
        assert resolve_path_template("s3://bucket/plain.csv", None) == "s3://bucket/plain.csv"

    def test_substitutes_fields_with_format_spec_and_attribute(self) -> None:
        path = "s3://raw/{country}/{run_date:%Y%m%d}/m{run_date.month}.csv"
        assert resolve_path_template(path, PARAMS) == "s3://raw/es/20260803/m8.csv"

    def test_missing_field_raises_with_available_fields(self) -> None:
        with pytest.raises(ValueError, match="unknown params field.*nope"):
            resolve_path_template("s3://raw/{nope}.csv", PARAMS)

    def test_template_without_params_instance_raises(self) -> None:
        with pytest.raises(ValueError, match="no params instance"):
            resolve_path_template("s3://raw/{run_date}.csv", None)


class TestResolveFileUri:
    def test_alias_without_locator_names_the_declaration(self) -> None:
        with pytest.raises(ValueError, match=r"IntoFile\.alias\('exports'\) requires"):
            resolve_file_uri(
                "exports", is_alias=True, locator=None, params_instance=PARAMS, role="IntoFile"
            )

    def test_literal_path_resolves_template(self) -> None:
        uri = resolve_file_uri(
            "s3://raw/{run_date:%Y}.csv",
            is_alias=False,
            locator=None,
            params_instance=PARAMS,
            role="FromFile",
        )
        assert uri == "s3://raw/2026.csv"


class TestPolarsFileTemplating:
    def test_reader_resolves_template_from_params(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "orders_20260803_es.csv"
        csv_path.write_text("id,amount\n1,9.99\n")
        spec = FileSourceSpec(
            alias="orders",
            path=str(tmp_path / "orders_{run_date:%Y%m%d}_{country}.csv"),
            format=Format.CSV,
        )
        reader = PolarsSourceReader(tmp_path)
        df = reader.read(spec, PARAMS).collect()
        assert df.shape == (1, 2)

    def test_writer_resolves_template_from_params(self, tmp_path: Path) -> None:
        frame = pl.DataFrame({"id": [1, 2]}).lazy()
        writer = PolarsTargetWriter(tmp_path)
        spec = FileSpec(path=str(tmp_path / "out_{run_date:%Y%m%d}.csv"), format=Format.CSV)
        writer.write(frame, spec, PARAMS)
        assert (tmp_path / "out_20260803.csv").exists()

    def test_writer_missing_template_field_raises(self, tmp_path: Path) -> None:
        frame = pl.DataFrame({"id": [1]}).lazy()
        writer = PolarsTargetWriter(tmp_path)
        spec = FileSpec(path=str(tmp_path / "out_{nope}.csv"), format=Format.CSV)
        with pytest.raises(ValueError, match="unknown params field"):
            writer.write(frame, spec, PARAMS)
