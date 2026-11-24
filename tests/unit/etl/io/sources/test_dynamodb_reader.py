"""Tests for DynamoDBSourceReader using a hand-rolled fake boto3 client.

No live DynamoDB required. Filter correctness is tested in
test_dynamodb_predicate.py. Here we verify that the reader passes the right
kwargs to ``client.scan()`` and converts items correctly to Polars DataFrames.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import wait
from typing import Any

import polars as pl
import pytest

from loom.etl.declarative.expr import col
from loom.etl.declarative.source._specs import DynamoDBSourceSpec
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.io.sources._dynamodb import DynamoDBSourceReader, _ParallelSegmentScanner
from loom.etl.io.sources._dynamodb_items import normalize_dynamo_item
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from tests.helpers.loggers import CapturedLog

# ---------------------------------------------------------------------------
# Fake boto3 dynamodb client
# ---------------------------------------------------------------------------


class _FakeDynamoDBClient:
    """Fake low-level client — returns canned AttributeValue pages.

    Honors ``Limit``, ``ExclusiveStartKey``, ``Segment``/``TotalSegments`` and
    records every ``scan()`` kwargs.  It does not evaluate filter expressions.
    """

    def __init__(self, items: list[dict[str, Any]]) -> None:
        self._items = items
        self.scan_calls: list[dict[str, Any]] = []

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls.append(dict(kwargs))
        total = kwargs.get("TotalSegments")
        items = self._items if total is None else self._items[kwargs["Segment"] :: total]
        start_key = kwargs.get("ExclusiveStartKey")
        start = start_key["__index__"] if start_key is not None else 0
        limit = kwargs.get("Limit", len(items))
        page = items[start : start + limit]
        response: dict[str, Any] = {"Items": page}
        if start + len(page) < len(items):
            response["LastEvaluatedKey"] = {"__index__": start + len(page)}
        return response


class _FailingSegmentClient:
    """Fake client where segment 0 raises and other segments page slowly."""

    def __init__(self, pages_per_segment: int) -> None:
        self._pages = pages_per_segment
        self.scan_calls: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        with self._lock:
            self.scan_calls.append(dict(kwargs))
        if kwargs.get("Segment") == 0:
            raise RuntimeError("throttled: segment 0")
        time.sleep(0.002)
        start_key = kwargs.get("ExclusiveStartKey")
        page_index = start_key["__page__"] if start_key else 0
        segment = kwargs.get("Segment")
        response: dict[str, Any] = {"Items": [{"pk": {"S": f"s{segment}-p{page_index}"}}]}
        if page_index + 1 < self._pages:
            response["LastEvaluatedKey"] = {"__page__": page_index + 1}
        return response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ORDERS = [
    {"order_id": {"S": "o1"}, "status": {"S": "active"}},
    {"order_id": {"S": "o2"}, "status": {"S": "pending"}},
]

_ORDER_SCHEMA = (
    ColumnSchema("order_id", LoomDtype.UTF8),
    ColumnSchema("status", LoomDtype.UTF8),
)


def _reader(items: list[dict[str, Any]]) -> tuple[DynamoDBSourceReader, _FakeDynamoDBClient]:
    client = _FakeDynamoDBClient(items)
    return DynamoDBSourceReader(client), client


def _spec(*, table: str = "orders", **kwargs: Any) -> DynamoDBSourceSpec:
    return DynamoDBSourceSpec(alias=table, table=table, **kwargs)


def _numbered_items(n: int) -> list[dict[str, Any]]:
    return [{"pk": {"S": f"id{i}"}, "n": {"N": str(i)}} for i in range(n)]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_lazy_frame(self) -> None:
        reader, _ = _reader(_ORDERS)
        result = reader.read(_spec(), None)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.shape == (2, 2)
        assert set(df.columns) == {"order_id", "status"}

    def test_values_preserved(self) -> None:
        reader, _ = _reader(_ORDERS)
        df = reader.read(_spec(), None).collect()
        assert df["order_id"].to_list() == ["o1", "o2"]
        assert df["status"].to_list() == ["active", "pending"]

    def test_lazyframe_can_be_collected_twice(self) -> None:
        items = _numbered_items(5)
        reader, _ = _reader(items)
        lf = reader.read(_spec(batch_size=2), None)

        df1 = lf.collect()
        df2 = lf.collect()

        assert df1.shape == df2.shape
        assert df1.sort("n").equals(df2.sort("n"))


# ---------------------------------------------------------------------------
# No client
# ---------------------------------------------------------------------------


class TestNoClient:
    def test_read_without_client_raises(self) -> None:
        reader = DynamoDBSourceReader()
        spec = _spec()
        with pytest.raises(RuntimeError, match="no client configured"):
            reader.read(spec, None)


# ---------------------------------------------------------------------------
# Laziness
# ---------------------------------------------------------------------------


class TestLaziness:
    def test_declared_schema_issues_no_scan_until_collect(self) -> None:
        reader, client = _reader(_ORDERS)
        lf = reader.read(_spec(schema=_ORDER_SCHEMA), None)
        assert client.scan_calls == []
        lf.collect()
        assert len(client.scan_calls) > 0

    def test_schemaless_samples_eagerly_then_rescans_on_collect(self) -> None:
        reader, client = _reader(_ORDERS)
        lf = reader.read(_spec(), None)
        inference_calls = len(client.scan_calls)
        assert inference_calls > 0
        lf.collect()
        assert len(client.scan_calls) > inference_calls


# ---------------------------------------------------------------------------
# scan() kwargs — pushdowns
# ---------------------------------------------------------------------------


class TestScanKwargs:
    def test_table_and_page_limit_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(batch_size=50), None).collect()
        call = client.scan_calls[-1]
        assert call["TableName"] == "orders"
        assert call["Limit"] == 50

    def test_no_filter_omits_filter_expression(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(), None).collect()
        assert "FilterExpression" not in client.scan_calls[-1]

    def test_filter_pushed_down(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(filter=col("status") == "active", schema=_ORDER_SCHEMA), None).collect()
        call = client.scan_calls[-1]
        assert call["FilterExpression"] == "#n0 = :v0"
        assert call["ExpressionAttributeNames"] == {"#n0": "status"}
        assert call["ExpressionAttributeValues"] == {":v0": {"S": "active"}}

    def test_projection_pushed_down_with_aliases(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(projection=("order_id", "status"), schema=_ORDER_SCHEMA), None).collect()
        call = client.scan_calls[-1]
        assert call["ProjectionExpression"] == "#p0, #p1"
        assert call["ExpressionAttributeNames"] == {"#p0": "order_id", "#p1": "status"}

    def test_filter_and_projection_names_merged(self) -> None:
        reader, client = _reader(_ORDERS)
        spec = _spec(
            filter=col("status") == "active",
            projection=("order_id",),
            schema=_ORDER_SCHEMA,
        )
        reader.read(spec, None).collect()
        call = client.scan_calls[-1]
        assert call["ExpressionAttributeNames"] == {"#n0": "status", "#p0": "order_id"}
        # filter values must survive the projection-alias merge
        assert call["ExpressionAttributeValues"] == {":v0": {"S": "active"}}

    def test_consistent_read_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(consistent_read=True, schema=_ORDER_SCHEMA), None).collect()
        assert client.scan_calls[-1]["ConsistentRead"] is True

    def test_consistent_read_omitted_by_default(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(schema=_ORDER_SCHEMA), None).collect()
        assert "ConsistentRead" not in client.scan_calls[-1]


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class TestPagination:
    def test_all_pages_consumed_across_last_evaluated_key(self) -> None:
        items = _numbered_items(7)
        reader, client = _reader(items)
        df = reader.read(_spec(batch_size=3, schema=_page_schema()), None).collect()
        assert df.height == 7
        assert sorted(df["pk"].to_list()) == sorted(f"id{i}" for i in range(7))
        scan_calls = [c for c in client.scan_calls if "TotalSegments" not in c]
        assert len(scan_calls) == 3  # ceil(7 / 3) pages
        assert scan_calls[1]["ExclusiveStartKey"] == {"__index__": 3}
        assert scan_calls[2]["ExclusiveStartKey"] == {"__index__": 6}

    def test_batch_size_controls_page_size(self) -> None:
        items = _numbered_items(5)
        reader, client = _reader(items)
        reader.read(_spec(batch_size=2, schema=_page_schema()), None).collect()
        assert all(call["Limit"] == 2 for call in client.scan_calls)
        assert len(client.scan_calls) == 3  # ceil(5 / 2) pages


def _page_schema() -> tuple[ColumnSchema, ...]:
    return (ColumnSchema("pk", LoomDtype.UTF8), ColumnSchema("n", LoomDtype.INT64))


# ---------------------------------------------------------------------------
# limit / n_rows
# ---------------------------------------------------------------------------


class TestLimit:
    @pytest.mark.parametrize(
        ("available", "limit", "expected"),
        [
            pytest.param(10, 3, 3, id="limit_applied"),
            pytest.param(5, None, 5, id="no_limit_returns_all"),
        ],
    )
    def test_limit(self, available: int, limit: int | None, expected: int) -> None:
        reader, _ = _reader(_numbered_items(available))
        df = reader.read(_spec(limit=limit), None).collect()
        assert df.height == expected

    def test_n_rows_head_enforced(self) -> None:
        reader, _ = _reader(_numbered_items(10))
        df = reader.read(_spec(), None).head(2).collect()
        assert df.height == 2

    def test_limit_stops_pagination_early(self) -> None:
        reader, client = _reader(_numbered_items(100))
        reader.read(_spec(batch_size=10, limit=10, schema=_page_schema()), None).collect()
        assert len(client.scan_calls) < 10

    def test_limit_pushed_down_to_server_side_page_limit(self) -> None:
        reader, client = _reader(_numbered_items(50))
        df = reader.read(_spec(limit=5, schema=_page_schema()), None).collect()
        assert df.height == 5
        assert client.scan_calls[-1]["Limit"] == 5
        assert len(client.scan_calls) == 1  # budget exhausted after one page

    def test_limit_caps_only_final_page(self) -> None:
        reader, client = _reader(_numbered_items(10))
        df = reader.read(_spec(batch_size=4, limit=6, schema=_page_schema()), None).collect()
        assert df.height == 6
        assert [call["Limit"] for call in client.scan_calls] == [4, 2]


# ---------------------------------------------------------------------------
# Empty table
# ---------------------------------------------------------------------------


class TestEmptyTable:
    def test_empty_returns_empty_dataframe(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(), None).collect()
        assert df.height == 0

    def test_empty_with_schema_returns_correct_columns(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema=_ORDER_SCHEMA), None).collect()
        assert set(df.columns) == {"order_id", "status"}
        assert df.height == 0


# ---------------------------------------------------------------------------
# Schema inference vs declared schema
# ---------------------------------------------------------------------------


class TestSchemaPaths:
    def test_inferred_schema_types(self) -> None:
        reader, _ = _reader(_numbered_items(3))
        df = reader.read(_spec(), None).collect()
        assert df["pk"].dtype == pl.String
        assert df["n"].dtype == pl.Int64

    def test_inference_scan_caps_page_limit_at_inference_size(self) -> None:
        reader, client = _reader(_numbered_items(3))
        reader.read(_spec(batch_size=5_000), None)
        assert client.scan_calls[0]["Limit"] == 1024  # min(batch_size, 1024), not 5000

    def test_declared_schema_registered_without_collect(self) -> None:
        reader, _ = _reader(_ORDERS)
        lf = reader.read(_spec(schema=_ORDER_SCHEMA), None)
        assert lf.collect_schema().names() == ["order_id", "status"]

    def test_capture_mode_adds_extra_column(self) -> None:
        import json

        items = [
            {"order_id": {"S": "o1"}, "status": {"S": "active"}, "extra_col": {"S": "surprise"}}
        ]
        reader, _ = _reader(items)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="capture"), None).collect()
        assert "_extra" in df.columns
        assert "extra_col" not in df.columns
        assert json.loads(df["_extra"][0])["extra_col"] == "surprise"

    def test_error_mode_raises_on_extra_field(self) -> None:
        items = [
            {"order_id": {"S": "o1"}, "status": {"S": "active"}, "extra_col": {"S": "surprise"}}
        ]
        reader, _ = _reader(items)
        frame = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="error"), None)
        with pytest.raises(
            (ValueError, pl.exceptions.ComputeError),
            match=r"orders: documents contain fields not declared in",
        ):
            frame.collect()

    def test_ignore_mode_drops_extra_field(self) -> None:
        items = [
            {"order_id": {"S": "o1"}, "status": {"S": "active"}, "extra_col": {"S": "surprise"}}
        ]
        reader, _ = _reader(items)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="ignore"), None).collect()
        assert set(df.columns) == {"order_id", "status"}


# ---------------------------------------------------------------------------
# Parallel scan
# ---------------------------------------------------------------------------


class TestParallelScan:
    def test_all_segment_items_returned(self) -> None:
        items = _numbered_items(20)
        reader, _ = _reader(items)
        df = reader.read(
            _spec(batch_size=4, total_segments=4, schema=_page_schema()), None
        ).collect()
        assert df.height == 20
        assert sorted(df["pk"].to_list()) == sorted(f"id{i}" for i in range(20))

    def test_segment_and_total_segments_forwarded(self) -> None:
        items = _numbered_items(12)
        reader, client = _reader(items)
        reader.read(_spec(batch_size=4, total_segments=3, schema=_page_schema()), None).collect()
        segment_calls = [c for c in client.scan_calls if "TotalSegments" in c]
        assert segment_calls
        assert all(call["TotalSegments"] == 3 for call in segment_calls)
        assert {call["Segment"] for call in segment_calls} == {0, 1, 2}

    def test_global_limit_enforced_across_segments(self) -> None:
        items = _numbered_items(40)
        reader, _ = _reader(items)
        df = reader.read(
            _spec(batch_size=4, total_segments=4, limit=6, schema=_page_schema()), None
        ).collect()
        assert df.height == 6


# ---------------------------------------------------------------------------
# Parallel scan — segment failure handling
# ---------------------------------------------------------------------------


class TestParallelScanFailures:
    def test_failed_segment_aborts_iteration_promptly(self) -> None:
        pages_per_segment = 300
        client = _FailingSegmentClient(pages_per_segment)
        scanner = _ParallelSegmentScanner(client, {"TableName": "t", "Limit": 1}, 2)
        with pytest.raises(RuntimeError, match="throttled"):
            for _ in scanner:
                pass
        scanner.close()
        # Fail-fast: the healthy segment must be stopped long before it pages
        # the whole table.
        healthy_calls = [c for c in client.scan_calls if c.get("Segment") == 1]
        assert len(healthy_calls) < pages_per_segment / 2

    def test_segment_error_propagates_through_collect(self) -> None:
        client = _FailingSegmentClient(pages_per_segment=3)
        reader = DynamoDBSourceReader(client)
        lf = reader.read(_spec(total_segments=2, schema=_page_schema()), None)
        with pytest.raises((RuntimeError, pl.exceptions.ComputeError), match="throttled"):
            lf.collect()

    def test_close_without_iteration_logs_segment_error_and_does_not_hang(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = _FailingSegmentClient(pages_per_segment=1)
        scanner = _ParallelSegmentScanner(client, {"TableName": "t", "Limit": 1}, 2)
        wait(scanner._futures, timeout=5)

        scanner.close()  # early stop (e.g. limit satisfied) — must not raise nor hang

        log = CapturedLog(caplog)
        [failure_fields] = log.named(EtlEvent.DYNAMODB_SEGMENT_FAILED)
        assert failure_fields[EtlKey.SEGMENT] == 0
        assert log.exception_types(EtlEvent.DYNAMODB_SEGMENT_FAILED) == [RuntimeError]

    def test_error_not_double_logged_when_raised(self, caplog: pytest.LogCaptureFixture) -> None:
        client = _FailingSegmentClient(pages_per_segment=1)
        scanner = _ParallelSegmentScanner(client, {"TableName": "t", "Limit": 1}, 2)
        with pytest.raises(RuntimeError, match="throttled"):
            for _ in scanner:
                pass
        scanner.close()
        assert CapturedLog(caplog).named(EtlEvent.DYNAMODB_SEGMENT_FAILED) == []


# ---------------------------------------------------------------------------
# AttributeValue normalization end-to-end
# ---------------------------------------------------------------------------


class TestNormalization:
    def test_integral_number_becomes_int(self) -> None:
        reader, _ = _reader([{"pk": {"S": "a"}, "count": {"N": "42"}}])
        df = reader.read(_spec(), None).collect()
        assert df["count"].dtype == pl.Int64
        assert df["count"][0] == 42

    def test_fractional_number_becomes_float(self) -> None:
        reader, _ = _reader([{"pk": {"S": "a"}, "amount": {"N": "3.14"}}])
        df = reader.read(_spec(), None).collect()
        assert df["amount"].dtype == pl.Float64
        assert df["amount"][0] == pytest.approx(3.14)

    def test_string_set_becomes_sorted_list(self) -> None:
        reader, _ = _reader([{"pk": {"S": "a"}, "tags": {"SS": ["zeta", "alpha"]}}])
        df = reader.read(_spec(), None).collect()
        assert df["tags"].dtype == pl.List(pl.String)
        assert df["tags"][0].to_list() == ["alpha", "zeta"]

    def test_number_set_becomes_sorted_list(self) -> None:
        reader, _ = _reader([{"pk": {"S": "a"}, "scores": {"NS": ["30", "7"]}}])
        df = reader.read(_spec(), None).collect()
        assert df["scores"][0].to_list() == [7, 30]

    def test_binary_becomes_bytes(self) -> None:
        reader, _ = _reader([{"pk": {"S": "a"}, "blob": {"B": b"\x00\x01"}}])
        df = reader.read(_spec(), None).collect()
        assert df["blob"].dtype == pl.Binary
        assert df["blob"][0] == b"\x00\x01"

    def test_map_and_list_recursed(self) -> None:
        item = {
            "pk": {"S": "a"},
            "meta": {"M": {"depth": {"N": "2"}, "tags": {"L": [{"S": "x"}, {"N": "1.5"}]}}},
        }
        normalized = normalize_dynamo_item(item)
        assert normalized == {"pk": "a", "meta": {"depth": 2, "tags": ["x", 1.5]}}

    def test_bool_and_null(self) -> None:
        item = {"pk": {"S": "a"}, "active": {"BOOL": True}, "deleted": {"NULL": True}}
        normalized = normalize_dynamo_item(item)
        assert normalized == {"pk": "a", "active": True, "deleted": None}
