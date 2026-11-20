"""Tests for SparkTargetWriter.to_frame execution-record conversion."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from pyspark.sql import types as T

from loom.etl.engines.spark import SparkTargetWriter
from loom.etl.lineage._records import (
    EventName,
    PipelineRunRecord,
    RunStatus,
    StepRunRecord,
)


class _FakeSpark:
    def __init__(self) -> None:
        self.calls: list[tuple[list[dict[str, Any]], T.StructType]] = []
        self.createDataFrame = self.create_data_frame  # NOSONAR

    def create_data_frame(self, rows: list[dict[str, Any]], schema: T.StructType) -> object:
        self.calls.append((rows, schema))
        return {"rows": rows, "schema": schema}


class TestSparkWriterToFrame:
    def test_to_frame_uses_spark_create_dataframe(self, tmp_path: Path) -> None:
        spark = _FakeSpark()
        writer = SparkTargetWriter(cast(Any, spark), str(tmp_path))
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
            error=None,
        )

        frame = writer.to_frame([record])
        assert isinstance(frame, dict)
        assert "rows" in frame
        assert "schema" in frame
        assert len(spark.calls) == 1
        rows, schema = next(iter(spark.calls))
        assert rows[0]["status"] == "success"
        assert "event" not in rows[0]
        assert [field.name for field in schema.fields] == [
            "run_id",
            "correlation_id",
            "attempt",
            "step_run_id",
            "step",
            "started_at",
            "status",
            "duration_ms",
            "error",
            "process_run_id",
            "error_type",
            "error_message",
        ]

    def test_to_frame_rejects_empty_batch(self, tmp_path: Path) -> None:
        writer = SparkTargetWriter(cast(Any, _FakeSpark()), str(tmp_path))
        with pytest.raises(ValueError, match="at least one record"):
            writer.to_frame([])

    def test_to_frame_rejects_heterogeneous_batch(self, tmp_path: Path) -> None:
        writer = SparkTargetWriter(cast(Any, _FakeSpark()), str(tmp_path))
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
            error=None,
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
            error=None,
        )
        with pytest.raises(TypeError, match="homogeneous record types"):
            writer.to_frame([step, pipeline])
