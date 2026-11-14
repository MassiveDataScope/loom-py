"""Tests for the ETL lineage table store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.lineage._records import EventName, PipelineRunRecord, RunStatus
from loom.etl.lineage.sinks._table import TableLineageStore


@dataclass(frozen=True)
class _UnknownRecord:
    value: str = "x"


class _RecordingWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[object, TableRef]] = []

    def write_record(self, record: object, ref: TableRef) -> None:
        self.calls.append((record, ref))


class TestTableLineageStore:
    def test_table_lineage_store_prefixes_database_and_forwards_records(self) -> None:
        writer = _RecordingWriter()
        store = TableLineageStore(writer, database="ops")
        record = PipelineRunRecord(
            event=EventName.PIPELINE_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            pipeline="DailyPipeline",
            started_at=datetime(2024, 1, 1, tzinfo=UTC),
            status=RunStatus.SUCCESS,
            duration_ms=1,
            error=None,
        )

        store.write_record(record)

        assert writer.calls == [(record, TableRef("ops.pipeline_runs"))]

    def test_table_lineage_store_uses_table_name_without_database(self) -> None:
        writer = _RecordingWriter()
        store = TableLineageStore(writer)
        record = PipelineRunRecord(
            event=EventName.PIPELINE_END,
            run_id="run-1",
            correlation_id=None,
            attempt=1,
            pipeline="DailyPipeline",
            started_at=datetime(2024, 1, 1, tzinfo=UTC),
            status=RunStatus.SUCCESS,
            duration_ms=1,
            error=None,
        )

        store.write_record(record)

        assert writer.calls == [(record, TableRef("pipeline_runs"))]

    def test_table_lineage_store_rejects_unknown_record_types(self) -> None:
        writer = _RecordingWriter()
        store = TableLineageStore(writer)

        record = cast(Any, _UnknownRecord())
        with pytest.raises(TypeError, match="unrecognised record type"):
            store.write_record(record)
