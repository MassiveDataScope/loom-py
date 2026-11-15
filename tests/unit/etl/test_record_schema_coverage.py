"""Schema coverage: assert _RECORD_SCHEMA_MAP columns match each record dataclass.

If a field is added to a RunRecord dataclass but the corresponding
tuple[ColumnSchema, ...] in records.py is not updated, to_frame() will
silently drop or mis-type the new column.  These tests detect that drift.
"""

from __future__ import annotations

import dataclasses

import pytest

from loom.etl.lineage._records import (
    _RECORD_SCHEMA_MAP,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)

# 'event' is excluded from to_row() — it must not appear in the schema map.
_EXCLUDED_FIELDS = frozenset({"event"})


class TestRecordSchemaCoverage:
    @pytest.mark.parametrize(
        "record_type",
        [PipelineRunRecord, ProcessRunRecord, StepRunRecord],
        ids=["PipelineRunRecord", "ProcessRunRecord", "StepRunRecord"],
    )
    def test_schema_columns_match_dataclass_fields(
        self,
        record_type: type,
    ) -> None:
        """Schema column names must exactly match the to_row() keys for each record type."""
        dataclass_fields = {
            f.name for f in dataclasses.fields(record_type) if f.name not in _EXCLUDED_FIELDS
        }
        schema_cols = {c.name for c in _RECORD_SCHEMA_MAP[record_type]}
        assert schema_cols == dataclass_fields, (
            f"{record_type.__name__}: schema map has {sorted(schema_cols)} "
            f"but dataclass fields (excluding 'event') are {sorted(dataclass_fields)}. "
            "Update _RECORD_SCHEMA_MAP in lineage/_records.py."
        )

    @pytest.mark.parametrize(
        "record_type",
        [PipelineRunRecord, ProcessRunRecord, StepRunRecord],
        ids=["PipelineRunRecord", "ProcessRunRecord", "StepRunRecord"],
    )
    def test_schema_columns_preserve_dataclass_order(
        self,
        record_type: type,
    ) -> None:
        """Schema columns must follow the same declaration order as the dataclass fields.

        Preserving order matters for backends like Spark that assign column
        positions by index when reading from dicts.
        """
        dataclass_order = [
            f.name for f in dataclasses.fields(record_type) if f.name not in _EXCLUDED_FIELDS
        ]
        schema_order = [c.name for c in _RECORD_SCHEMA_MAP[record_type]]
        assert schema_order == dataclass_order, (
            f"{record_type.__name__}: schema column order {schema_order} "
            f"does not match dataclass field order {dataclass_order}."
        )
