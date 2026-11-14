"""Tests for ETL lineage records and lookup helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest

from loom.etl.lineage._records import (
    EventName,
    LineageRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    RunStatus,
    StepRunRecord,
    get_lineage_schema,
    get_lineage_table_name,
)


class TestLineageRecords:
    def test_record_to_row_excludes_event_and_stringifies_status(self) -> None:
        started_at = datetime(2024, 1, 1, tzinfo=UTC)

        pipeline = PipelineRunRecord(
            event=EventName.PIPELINE_END,
            run_id="run-1",
            correlation_id="corr-1",
            attempt=2,
            pipeline="DailyPipeline",
            started_at=started_at,
            status=RunStatus.SUCCESS,
            duration_ms=12,
            error=None,
        )
        process = ProcessRunRecord(
            event=EventName.PROCESS_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            process_run_id="proc-1",
            process="Build",
            started_at=started_at,
            status=RunStatus.FAILED,
            duration_ms=7,
            error="boom",
            error_type="RuntimeError",
            error_message="boom",
            failed_step_run_id="step-1",
            failed_step="Transform",
        )
        step = StepRunRecord(
            event=EventName.STEP_END,
            run_id="run-1",
            correlation_id="corr-1",
            attempt=1,
            step_run_id="step-1",
            step="Transform",
            started_at=started_at,
            status=RunStatus.SUCCESS,
            duration_ms=5,
            error=None,
            process_run_id="proc-1",
        )

        assert pipeline.to_row()["status"] == "success"
        assert process.to_row()["status"] == "failed"
        assert step.to_row()["status"] == "success"
        assert "event" not in pipeline.to_row()
        assert "event" not in process.to_row()
        assert "event" not in step.to_row()

    @pytest.mark.parametrize(
        "record_type,expected",
        [
            (PipelineRunRecord, "pipeline_runs"),
            (ProcessRunRecord, "process_runs"),
            (StepRunRecord, "step_runs"),
        ],
    )
    def test_lineage_lookups_resolve_known_types(
        self,
        record_type: type[LineageRecord],
        expected: str,
    ) -> None:
        schema = get_lineage_schema(record_type)
        table_name = get_lineage_table_name(record_type)

        assert schema
        assert table_name == expected

    def test_lineage_lookups_reject_unknown_types(self) -> None:
        unknown_type = type("UnknownRecord", (), {})
        unknown_record_type = cast(type[LineageRecord], unknown_type)

        with pytest.raises(KeyError):
            get_lineage_schema(unknown_record_type)

        with pytest.raises(KeyError):
            get_lineage_table_name(unknown_record_type)
