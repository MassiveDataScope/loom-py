"""Unit tests for compile-time validation of file path templates."""

from __future__ import annotations

from datetime import date

import pytest

from loom.etl import ETLParams
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler._validators_step import validate_file_path_templates
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import FileSourceSpec
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import AppendSpec
from tests.unit.etl.compiler.doubles import AppendOutStep
from tests.unit.etl.doubles import RunDateETLParams


def _file_source(path: str, *, is_alias: bool = False) -> SourceBinding:
    return SourceBinding(
        alias="data",
        spec=FileSourceSpec(alias="data", path=path, format=Format.CSV, is_alias=is_alias),
    )


def _table_target() -> TargetBinding:
    return TargetBinding(spec=AppendSpec(table_ref=TableRef("t.out")))


class TestLiteralFilePathTemplates:
    def test_known_field_in_source_path_passes(self) -> None:
        validate_file_path_templates(
            AppendOutStep,
            RunDateETLParams,
            (_file_source("s3://raw/{run_date:%Y%m%d}.csv"),),
            _table_target(),
        )

    def test_unknown_field_in_source_path_raises(self) -> None:
        sources = (_file_source("s3://raw/{nope}.csv"),)
        target = _table_target()

        with pytest.raises(ETLCompilationError) as excinfo:
            validate_file_path_templates(AppendOutStep, RunDateETLParams, sources, target)
        assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD
        assert excinfo.value.field == "nope"

    def test_unknown_field_in_target_path_raises(self) -> None:
        target = TargetBinding(spec=FileSpec(path="s3://out/{nope}.csv", format=Format.CSV))
        with pytest.raises(ETLCompilationError) as excinfo:
            validate_file_path_templates(AppendOutStep, RunDateETLParams, (), target)
        assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD

    def test_known_field_in_target_path_passes(self) -> None:
        target = TargetBinding(spec=FileSpec(path="s3://out/{run_date}.csv", format=Format.CSV))
        validate_file_path_templates(AppendOutStep, RunDateETLParams, (), target)

    def test_alias_paths_are_not_validated_at_compile_time(self) -> None:
        """Alias URIs live in storage config; template check happens at runtime."""
        target = TargetBinding(
            spec=FileSpec(path="exports_daily", format=Format.CSV, is_alias=True)
        )
        validate_file_path_templates(
            AppendOutStep, RunDateETLParams, (_file_source("events_raw", is_alias=True),), target
        )


# ---------------------------------------------------------------------------
# Read-only properties on the params class are known template fields
# ---------------------------------------------------------------------------


class PartitionDayParams(ETLParams):
    run_date: date

    @property
    def partition_day(self) -> date:
        return self.run_date


class TestPropertyFilePathTemplates:
    def test_property_in_source_path_passes(self) -> None:
        validate_file_path_templates(
            AppendOutStep,
            PartitionDayParams,
            (_file_source("s3://raw/{partition_day}.csv"),),
            _table_target(),
        )

    def test_unknown_field_still_raises_with_property_params(self) -> None:
        sources = (_file_source("s3://raw/{nope}.csv"),)
        target = _table_target()

        with pytest.raises(ETLCompilationError) as excinfo:
            validate_file_path_templates(AppendOutStep, PartitionDayParams, sources, target)
        assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD
        assert excinfo.value.field == "nope"
