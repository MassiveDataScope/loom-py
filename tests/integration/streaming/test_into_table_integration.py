"""Integration tests for IntoTable — SQLAlchemy, Delta, and ClickHouse backends.

- SQLAlchemy / Delta tests use real local services (SQLite, Delta on disk).
- ClickHouse tests are fully unit tests: ``clickhouse_connect.get_client`` is
  patched so no server is required. They verify the buffer logic, coercions,
  and the column-oriented insert contract.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import polars as pl
import pytest
from sqlalchemy import create_engine, event, text

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.config import ConfigContext  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.core.repository.sqlalchemy.session_manager import SessionManager  # noqa: E402
from loom.streaming import Backend, FromTopic, IntoTable, Process, StreamFlow  # noqa: E402
from loom.streaming.nodes._table import DeltaSinkConfig, SqlAlchemySinkConfig  # noqa: E402
from loom.streaming.nodes._table import common as _table_common  # noqa: E402
from loom.streaming.nodes._table.common import ClickHouseSinkConfig  # noqa: E402
from loom.streaming.testing import StreamingTestRunner  # noqa: E402


class _OrderRow(LoomStruct):
    order_id: int
    amount: float
    status: str


class _VisitRow(LoomStruct):
    visit_id: UUID
    label: str


_VISIT_ID = UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")


# ── ClickHouse unit test fixtures ─────────────────────────────────────────────


class _CHRow(LoomStruct):
    """Minimal struct covering all coercion cases for ClickHouse unit tests."""

    event_id: str
    count: int
    active: bool  # bool → UInt8 (coercion required)
    tags: list[str]  # list[str] → Array(String) (native, no json.dumps)
    parent_id: int | None  # Optional → Nullable


def _ch_config(table: str = "test_table", **overrides: Any) -> ClickHouseSinkConfig:
    cfg: dict[str, Any] = {"url": "clickhouse://default:@localhost:18123/default", "table": table}
    cfg.update(overrides)
    return ClickHouseSinkConfig.from_config(cfg, default_table=table)


@pytest.fixture
def mock_ch_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch clickhouse_connect.get_client so no server is needed."""
    client = MagicMock()
    monkeypatch.setattr(_table_common, "_cc", MagicMock(get_client=MagicMock(return_value=client)))
    return client


def _make_db(tmp_path: Path, ddl: str) -> Path:
    path = tmp_path / "sink.db"
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(text(ddl))
    engine.dispose()
    return path


def _sync_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _async_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


class TestIntoTableSQLAlchemyIntegration:
    def test_write_batch_inserts_rows(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders", backend=Backend.SQLALCHEMY)
        sink_config = SqlAlchemySinkConfig.from_config(
            {"chunk_size": 500, "table": "orders"},
            default_table="orders",
        )
        partition = node.build_partition(
            sink_config,
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )

        try:
            buffered = partition.write_batch(
                [
                    _OrderRow(order_id=1, amount=99.9, status="shipped"),
                    _OrderRow(order_id=2, amount=49.5, status="pending"),
                ]
            )
            flushed = partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_id, amount, status FROM orders ORDER BY order_id")
            ).fetchall()
        engine.dispose()

        assert len(rows) == 2
        assert rows[0] == (1, 99.9, "shipped")
        assert rows[1] == (2, 49.5, "pending")
        assert (buffered, flushed) == (0, 2), "durable only once the close flush committed"

    def test_a_uuid_field_is_written_as_core_writes_a_uuid_column(self, tmp_path: Path) -> None:
        path = _make_db(tmp_path, "CREATE TABLE visits (visit_id CHAR(32), label TEXT)")
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_VisitRow, table="visits", backend=Backend.SQLALCHEMY)
        sink_config = SqlAlchemySinkConfig.from_config({"table": "visits"}, default_table="visits")
        partition = node.build_partition(
            sink_config,
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )

        try:
            partition.write_batch([_VisitRow(visit_id=_VISIT_ID, label="first")])
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT visit_id, label FROM visits")).fetchall()
        engine.dispose()

        assert rows == [(_VISIT_ID.hex, "first")]

    def test_empty_batch_is_noop(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders")
        sink_config = SqlAlchemySinkConfig.from_config(
            {"chunk_size": 500, "table": "orders"},
            default_table="orders",
        )
        partition = node.build_partition(
            sink_config,
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )
        try:
            partition.write_batch([])
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        engine.dispose()

        assert count == 0

    def test_config_table_overrides_dsl_table(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE sink_orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders")
        sink_config = SqlAlchemySinkConfig.from_config(
            {"table": "sink_orders", "chunk_size": 500},
            default_table="orders",
        )
        partition = node.build_partition(
            sink_config,
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )
        try:
            partition.write_batch([_OrderRow(order_id=10, amount=5.0, status="done")])
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT order_id FROM sink_orders")).fetchall()
        engine.dispose()

        assert list(rows) == [(10,)]

    def test_multiple_kafka_events_flush_once_when_buffer_is_retained(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        insert_calls: list[int] = []

        @event.listens_for(session_manager.engine.sync_engine, "before_cursor_execute")
        def _count_inserts(
            conn: object,
            cursor: object,
            statement: str,
            parameters: object,
            context: object,
            executemany: bool,
        ) -> None:
            del conn, cursor, parameters, context
            if statement.lstrip().upper().startswith("INSERT INTO ORDERS"):
                insert_calls.append(1)

        monkeypatch.setattr(
            SessionManager,
            "from_config",
            classmethod(lambda cls, config, **kwargs: session_manager),
        )

        flow: StreamFlow[_OrderRow, _OrderRow] = StreamFlow(
            name="orders_sink_flow",
            source=FromTopic("orders.raw", payload=_OrderRow),
            process=Process(
                cast(
                    Any,
                    IntoTable(
                        payload=_OrderRow,
                        table="orders",
                        backend=Backend.SQLALCHEMY,
                        name="orders_sink",
                    ),
                ),
            ),
        )
        config = ConfigContext.from_dict(
            {
                "kafka": {
                    "consumer": {
                        "brokers": ["localhost:9092"],
                        "group_id": "test",
                        "delivery": "at_most_once",
                        "topics": ["orders.raw"],
                    }
                },
                "database": {
                    "warehouse": {
                        "url": _async_url(path),
                    },
                },
                "streaming": {
                    "sinks": {
                        "orders_sink": {
                            "database": "warehouse",
                            "table": "orders",
                            "chunk_size": 500,
                            "buffer_max_records": 10_000,
                            "buffer_max_bytes": 10_000_000,
                            "buffer_max_age_s": 9999.0,
                        }
                    }
                },
            }
        )

        runner = StreamingTestRunner.from_flow(flow, config=config)
        runner.with_payloads(
            [
                _OrderRow(order_id=1, amount=10.0, status="new"),
                _OrderRow(order_id=2, amount=20.0, status="paid"),
                _OrderRow(order_id=3, amount=30.0, status="sent"),
            ]
        ).run()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_id, amount, status FROM orders ORDER BY order_id")
            ).fetchall()
        engine.dispose()

        assert len(rows) == 3
        assert len(insert_calls) == 1

    def test_multiple_kafka_events_flush_once_for_delta(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pytest.importorskip("deltalake")

        uri = str(tmp_path / "events_delta")
        write_calls: list[int] = []
        ipc_batch_calls: list[int] = []
        original_write_deltalake = cast(Any, _table_common).write_deltalake
        original_new_stream = cast(Any, _table_common)._pa_ipc.new_stream

        def _counted_write_deltalake(*args: Any, **kwargs: Any) -> Any:
            write_calls.append(1)
            assert kwargs["writer_properties"].compression == "SNAPPY"
            assert kwargs["target_file_size"] == 134_217_728
            return original_write_deltalake(*args, **kwargs)

        class _CountingRecordBatchWriter:
            def __init__(self, wrapped: Any) -> None:
                self._wrapped = wrapped

            def write_batch(self, batch: Any) -> None:
                ipc_batch_calls.append(batch.num_rows)
                self._wrapped.write_batch(batch)

            def close(self) -> None:
                self._wrapped.close()

        def _counted_new_stream(*args: Any, **kwargs: Any) -> _CountingRecordBatchWriter:
            return _CountingRecordBatchWriter(original_new_stream(*args, **kwargs))

        monkeypatch.setattr(cast(Any, _table_common), "write_deltalake", _counted_write_deltalake)
        monkeypatch.setattr(cast(Any, _table_common)._pa_ipc, "new_stream", _counted_new_stream)

        flow: StreamFlow[_OrderRow, _OrderRow] = StreamFlow(
            name="events_delta_sink_flow",
            source=FromTopic("events.raw", payload=_OrderRow),
            process=Process(
                cast(
                    Any,
                    IntoTable(
                        payload=_OrderRow,
                        table="events",
                        backend=Backend.DELTA,
                        name="events_sink",
                    ),
                )
            ),
        )
        config = ConfigContext.from_dict(
            {
                "kafka": {
                    "consumer": {
                        "brokers": ["localhost:9092"],
                        "group_id": "test",
                        "delivery": "at_most_once",
                        "topics": ["events.raw"],
                    }
                },
                "streaming": {
                    "sinks": {
                        "events_sink": {
                            "uri": uri,
                            "buffer_max_records": 10_000,
                            "buffer_max_bytes": 10_000_000,
                            "buffer_max_age_s": 9999.0,
                            "spool_max_bytes": 1024,
                            "staging": {"compression": "zstd"},
                            "writer_properties": {"compression": "SNAPPY"},
                            "target_file_size": 134_217_728,
                        }
                    }
                },
            }
        )

        runner = StreamingTestRunner.from_flow(flow, config=config)
        runner.with_payloads(
            [
                _OrderRow(order_id=1, amount=10.0, status="new"),
                _OrderRow(order_id=2, amount=20.0, status="paid"),
                _OrderRow(order_id=3, amount=30.0, status="sent"),
            ]
        ).run()

        frame = pl.scan_delta(uri).sort("order_id").collect()

        assert frame.to_dicts() == [
            {"order_id": 1, "amount": 10.0, "status": "new"},
            {"order_id": 2, "amount": 20.0, "status": "paid"},
            {"order_id": 3, "amount": 30.0, "status": "sent"},
        ]
        assert len(write_calls) == 1
        assert ipc_batch_calls == [3]

    def test_delta_partition_reports_rows_durable_only_when_it_writes_them(
        self, tmp_path: Path
    ) -> None:
        pytest.importorskip("deltalake")
        node = IntoTable(payload=_OrderRow, table="events", backend=Backend.DELTA)
        config = DeltaSinkConfig.from_config(
            {"uri": str(tmp_path / "events_delta"), "buffer_max_records": 3},
            default_table="events",
        )
        partition = node.build_partition(config, worker_index=0, worker_count=1)
        rows = [_OrderRow(order_id=i, amount=1.0, status="new") for i in range(4)]

        assert partition.write_batch(rows[:2]) == 0
        assert partition.write_batch(rows[2:]) == 3
        assert partition.close() == 1

    def test_a_delta_payload_without_fields_is_refused_at_startup(self, tmp_path: Path) -> None:
        class _NoColumns(LoomStruct):
            pass

        node = IntoTable(payload=_NoColumns, table="events", backend=Backend.DELTA)
        config = DeltaSinkConfig.from_config(
            {"uri": str(tmp_path / "events_delta")}, default_table="events"
        )

        with pytest.raises(ValueError, match="_NoColumns has none"):
            node.build_partition(config, worker_index=0, worker_count=1)

    def test_delta_sink_config_parses_writer_properties_and_staging(
        self,
    ) -> None:
        config = DeltaSinkConfig.from_config(
            {
                "uri": "s3://data/events",
                "partition_by": ["year", "month"],
                "target_file_size": 134_217_728,
                "spool_max_bytes": 4096,
                "part_max_records": 256,
                "staging": {"compression": "zstd"},
                "writer_properties": {"compression": "SNAPPY"},
            },
            default_table="events",
        )

        assert config.staging_compression == "zstd"
        assert config.writer_properties is not None
        assert config.writer_properties.compression == "SNAPPY"
        assert config.target_file_size == 134_217_728


class TestIntoTableClickHouseUnit:
    """Unit tests for _ClickHouseTablePartition — no real ClickHouse server."""

    def test_write_batch_inserts_column_oriented(self, mock_ch_client: MagicMock) -> None:
        """close() flushes all rows in column-oriented format."""
        node = IntoTable(payload=_CHRow, table="t", backend=Backend.CLICKHOUSE)
        partition = node.build_partition(_ch_config(), worker_index=0, worker_count=1)

        partition.write_batch(
            [
                _CHRow(event_id="e1", count=1, active=True, tags=["a", "b"], parent_id=None),
                _CHRow(event_id="e2", count=2, active=False, tags=["c"], parent_id=10),
            ]
        )
        partition.close()

        mock_ch_client.insert.assert_called_once()
        _, kwargs = mock_ch_client.insert.call_args
        assert kwargs["column_oriented"] is True
        assert kwargs["column_names"] == ["event_id", "count", "active", "tags", "parent_id"]
        data = mock_ch_client.insert.call_args.args[1]
        # data[col_idx] = list of values for that column
        assert data[0] == ["e1", "e2"]  # event_id
        assert data[1] == [1, 2]  # count
        assert data[2] == [1, 0]  # active: bool → int
        assert data[3] == [["a", "b"], ["c"]]  # tags: list[str] native, no json.dumps
        assert data[4] == [None, 10]  # parent_id: Nullable

    def test_list_field_not_json_serialised(self, mock_ch_client: MagicMock) -> None:
        """list[str] arrives at insert() as a Python list, never as a JSON string."""
        node = IntoTable(payload=_CHRow, table="t", backend=Backend.CLICKHOUSE)
        partition = node.build_partition(_ch_config(), worker_index=0, worker_count=1)

        partition.write_batch(
            [
                _CHRow(
                    event_id="e1",
                    count=0,
                    active=False,
                    tags=["/invoice/url", "/updatedAt"],
                    parent_id=None,
                ),
            ]
        )
        partition.close()

        data = mock_ch_client.insert.call_args.args[1]
        tags_column = data[3]
        assert tags_column == [["/invoice/url", "/updatedAt"]]
        assert isinstance(tags_column[0], list), "tags must be list, not json string"

    def test_bool_coercion_to_int(self, mock_ch_client: MagicMock) -> None:
        """bool fields are coerced to 0/1 for ClickHouse UInt8 columns."""
        node = IntoTable(payload=_CHRow, table="t", backend=Backend.CLICKHOUSE)
        partition = node.build_partition(_ch_config(), worker_index=0, worker_count=1)

        partition.write_batch(
            [
                _CHRow(event_id="t", count=0, active=True, tags=[], parent_id=None),
                _CHRow(event_id="f", count=0, active=False, tags=[], parent_id=None),
            ]
        )
        partition.close()

        data = mock_ch_client.insert.call_args.args[1]
        active_column = data[2]  # active is index 2
        assert active_column == [1, 0]
        assert all(isinstance(v, int) for v in active_column)

    def test_age_flush_fires_on_empty_epoch(self, mock_ch_client: MagicMock) -> None:
        """Empty write_batch triggers flush when buffer_max_age_s is exceeded."""
        node = IntoTable(payload=_CHRow, table="t", backend=Backend.CLICKHOUSE)
        partition = node.build_partition(
            _ch_config(buffer_max_records=10_000, buffer_max_age_s=0.01),
            worker_index=0,
            worker_count=1,
        )

        row = _CHRow(event_id="e1", count=1, active=True, tags=["x"], parent_id=None)
        assert partition.write_batch([row]) == 0
        assert mock_ch_client.insert.call_count == 0  # buffer retained

        time.sleep(0.015)  # exceed max_age_s
        assert partition.write_batch([]) == 1  # empty epoch — age check fires flush

        assert mock_ch_client.insert.call_count == 1
        assert partition.close() == 0
        assert mock_ch_client.insert.call_count == 1  # no double-flush

    def test_buffer_retained_below_threshold(self, mock_ch_client: MagicMock) -> None:
        """Items stay in buffer when neither count nor age threshold is reached."""
        node = IntoTable(payload=_CHRow, table="t", backend=Backend.CLICKHOUSE)
        partition = node.build_partition(
            _ch_config(buffer_max_records=10_000, buffer_max_age_s=9999.0),
            worker_index=0,
            worker_count=1,
        )

        partition.write_batch(
            [
                _CHRow(event_id="e1", count=1, active=True, tags=[], parent_id=None),
            ]
        )
        partition.write_batch([])

        assert mock_ch_client.insert.call_count == 0  # still buffered
