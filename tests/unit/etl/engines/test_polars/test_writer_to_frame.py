"""Tests for PolarsTargetWriter.to_frame execution-record conversion."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from loom.etl.engines.polars import PolarsTargetWriter
from loom.etl.lineage._records import (
    EventName,
    PipelineRunRecord,
    RunStatus,
    StepRunRecord,
)


class TestWriterToFrame:
    def test_to_frame_builds_lazyframe_with_explicit_types(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        record = StepRunRecord(
            event=EventName.STEP_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            step_run_id="step-1",
            step="MyStep",
            started_at=datetime.now(tz=UTC),
            status=RunStatus.SUCCESS,
            duration_ms=17,
        )

        frame = writer.to_frame([record]).collect()

        assert frame.shape == (1, 11)
        assert frame.schema["status"] == pl.String
        assert frame.schema["error_code"] == pl.String
        assert frame.select("status").item() == "success"

    def test_to_frame_rejects_empty_batch(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        with pytest.raises(ValueError, match="at least one record"):
            writer.to_frame([])

    def test_to_frame_rejects_heterogeneous_batch(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        step = StepRunRecord(
            event=EventName.STEP_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            step_run_id="step-1",
            step="MyStep",
            started_at=datetime.now(tz=UTC),
            status=RunStatus.SUCCESS,
            duration_ms=17,
        )
        pipeline = PipelineRunRecord(
            event=EventName.PIPELINE_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            pipeline="MyPipeline",
            started_at=datetime.now(tz=UTC),
            status=RunStatus.SUCCESS,
            duration_ms=23,
        )
        with pytest.raises(TypeError, match="homogeneous record types"):
            writer.to_frame([step, pipeline])
