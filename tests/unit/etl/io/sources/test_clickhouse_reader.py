"""Tests for the ClickHouse source reader."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
import pytest

from loom.etl.declarative.expr import col
from loom.etl.declarative.source import FromClickHouse
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.schema._schema import ColumnSchema, LoomDtype

_ARROW_SUFFIX = ".arrow"
_TEMP_MARKER = "loom-clickhouse-"


def _extract_arrow_path(plan: str) -> str | None:
    for raw_token in plan.split():
        token = raw_token.strip("\"'[]")
        if _TEMP_MARKER in token and token.endswith(_ARROW_SUFFIX) and token.startswith("/"):
            return token
    return None


@dataclass(frozen=True)
class _Params:
    updated_at_from: datetime


class _FakeArrowClient:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.queries: list[str] = []

    def query_arrow(self, query: str) -> pa.Table:
        self.queries.append(query)
        return self.table


class TestClickHouseReaderArrowPath:
    def test_builds_query_and_applies_schema(self) -> None:
        table = pa.table({"id": ["1"], "amount": [1]})
        client = _FakeArrowClient(table)
        reader = ClickHouseSourceReader(client=client)

        spec = (
            FromClickHouse("analytics.cdc_events")
            .where(col("year") == 2024)
            .columns("id", "amount")
            .with_schema(
                (
                    ColumnSchema("id", LoomDtype.INT64, nullable=False),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                )
            )
            .distinct()
            ._to_spec("events")
        )

        result = reader.read(spec, object()).collect()

        assert client.queries == [
            "SELECT DISTINCT `id`, `amount` FROM `analytics`.`cdc_events` WHERE (year = 2024)"
        ]
        assert result.schema == {"id": pl.Int64, "amount": pl.Float64}
        assert result["id"].to_list() == [1]
        assert result["amount"].to_list() == [1.0]

    def test_quotes_datetime_predicates_in_query(self) -> None:
        table = pa.table({"id": ["1"]})
        client = _FakeArrowClient(table)
        reader = ClickHouseSourceReader(client=client)

        spec = (
            FromClickHouse("analytics.cdc_events")
            .where(
                col("source_time")
                >= _Params(updated_at_from=datetime(2026, 5, 25, tzinfo=UTC)).updated_at_from
            )
            .unbounded()
            ._to_spec("events")
        )

        reader.read(spec, object()).collect()

        assert client.queries == [
            "SELECT * FROM `analytics`.`cdc_events`"
            " WHERE (source_time >= toDateTime64('2026-05-25 00:00:00', 3, 'UTC'))"
        ]

    def test_select_star_when_no_columns(self) -> None:
        table = pa.table({"x": [1], "y": [2]})
        client = _FakeArrowClient(table)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        result = ClickHouseSourceReader(client=client).read(spec, None).collect()

        assert "SELECT * FROM" in client.queries[0]
        assert result.columns == ["x", "y"]

    def test_no_schema_returns_frame_as_is(self) -> None:
        table = pa.table({"val": [42]})
        client = _FakeArrowClient(table)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        result = ClickHouseSourceReader(client=client).read(spec, None).collect()

        assert result["val"].to_list() == [42]
        assert result["val"].dtype == pl.Int64


class TestClickHouseReaderClientInit:
    def test_raises_without_url_or_client(self) -> None:
        reader = ClickHouseSourceReader()
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        with pytest.raises(ValueError, match="storage.clickhouse"):
            reader.read(spec, None)

    def test_unknown_client_interface_raises(self) -> None:
        class _BadClient:
            pass

        reader = ClickHouseSourceReader(client=_BadClient())
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        with pytest.raises(TypeError, match="query_arrow"):
            reader.read(spec, None)


# ---------------------------------------------------------------------------
# Streaming path: spool query_arrow_stream() to IPC and scan it lazily
# ---------------------------------------------------------------------------


class _StreamContext:
    """Minimal ``with``-able / iterable stand-in for clickhouse-connect's StreamContext."""

    def __init__(self, batches: list[pa.Table]) -> None:
        self._batches = batches
        self._iter: object | None = None

    def __enter__(self) -> _StreamContext:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def __iter__(self) -> _StreamContext:
        if self._iter is None:
            self._iter = iter(self._batches)
        return self

    def __next__(self) -> pa.Table:
        if self._iter is None:
            self._iter = iter(self._batches)
        return next(self._iter)  # type: ignore[arg-type]


class _FakeStreamingClient:
    def __init__(self, batches: list[pa.Table]) -> None:
        self._batches = batches
        self.queries: list[str] = []

    def query_arrow_stream(self, query: str) -> _StreamContext:
        self.queries.append(query)
        return _StreamContext(self._batches)

    # Provide query_arrow as well so the empty-stream fallback is testable.
    def query_arrow(self, query: str) -> pa.Table:
        self.queries.append(query)
        if not self._batches:
            return pa.table({"id": pa.array([], type=pa.int64())})
        return self._batches[0]


class TestClickHouseReaderStreamingPath:
    def test_read_streaming_spools_batches_to_ipc(self) -> None:
        batches = [
            pa.table({"id": [1, 2]}),
            pa.table({"id": [3, 4]}),
            pa.table({"id": [5]}),
        ]
        client = _FakeStreamingClient(batches)
        reader = ClickHouseSourceReader(client=client)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        lazy = reader.read_streaming(spec, None)
        result = lazy.collect(engine="streaming")

        assert client.queries == ["SELECT * FROM `t`"]
        assert result["id"].to_list() == [1, 2, 3, 4, 5]

    def test_read_streaming_empty_stream_falls_back_to_query_arrow(self) -> None:
        client = _FakeStreamingClient(batches=[])
        reader = ClickHouseSourceReader(client=client)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        result = reader.read_streaming(spec, None).collect()

        # First call is query_arrow_stream (empty), second is the query_arrow fallback.
        assert len(client.queries) == 2
        assert result["id"].to_list() == []

    def test_read_streaming_writes_ipc_file_format_not_stream(self) -> None:
        # Regression: pl.scan_ipc expects the Arrow IPC *File* format (with
        # footer), not the streaming format. Using pa.ipc.new_stream produced
        # a footer-less artifact that failed downstream with InvalidFooter.
        batches = [pa.table({"id": [1, 2, 3]})]
        client = _FakeStreamingClient(batches)
        reader = ClickHouseSourceReader(client=client)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        lazy = reader.read_streaming(spec, None)
        # Resolve the backing file path from the LazyFrame plan and assert it
        # is a valid Arrow IPC File (has footer) and not a bare IPC Stream.
        plan = lazy.explain()
        path = _extract_arrow_path(plan)
        assert path is not None, f"Could not find IPC path in plan: {plan}"

        # File format must open successfully.
        with pa.OSFile(path, "rb") as source:
            reader_file = pa.ipc.open_file(source)
            assert reader_file.num_record_batches >= 1

        # Stream format open should fail on a File-format artifact (the
        # leading bytes are 'ARROW1' magic, not a stream schema message).
        with pa.OSFile(path, "rb") as source, pytest.raises(pa.ArrowInvalid):
            pa.ipc.open_stream(source)

    def test_read_streaming_requires_query_arrow_stream(self) -> None:
        class _NoStreamClient:
            def query_arrow(self, query: str) -> pa.Table:
                return pa.table({"x": [1]})

        reader = ClickHouseSourceReader(client=_NoStreamClient())
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        with pytest.raises(TypeError, match="query_arrow_stream"):
            reader.read_streaming(spec, None)
