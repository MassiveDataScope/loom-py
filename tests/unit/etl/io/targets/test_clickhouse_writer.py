"""Unit tests for ClickHouseTargetWriter — clickhouse-connect is mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from loom.core.plugins.optional import MissingExtraError
from loom.etl.io.targets._clickhouse import ClickHouseTableSpec, ClickHouseTargetWriter
from tests.helpers.extras import CLICKHOUSE_ISLAND, without_extra

_URL = "clickhouse://default:@localhost:8123/default"
_PARAMS = object()


def _make_writer() -> tuple[ClickHouseTargetWriter, MagicMock]:
    """Return (writer, mock_client) with clickhouse_connect patched."""
    mock_client = MagicMock()
    with patch("clickhouse_connect.get_client", return_value=mock_client) as _:
        writer = ClickHouseTargetWriter(_URL)
    writer._client = mock_client
    return writer, mock_client


class TestAppendMode:
    def test_append_calls_insert_without_truncate(self) -> None:
        """append mode: insert is called, TRUNCATE is not."""
        writer, client = _make_writer()
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        spec = ClickHouseTableSpec(table="my_table", write_mode="append")

        writer.write(df, spec, _PARAMS)

        client.command.assert_not_called()
        client.insert.assert_called_once()
        call_args = client.insert.call_args
        assert call_args.args[0] == "my_table"
        assert call_args.kwargs["column_names"] == ["a", "b"]
        assert call_args.kwargs["column_oriented"] is True


class TestReplaceMode:
    def test_replace_truncates_then_inserts(self) -> None:
        """replace mode: TRUNCATE IF EXISTS is called before insert."""
        writer, client = _make_writer()
        df = pl.DataFrame({"x": [10, 20]})
        spec = ClickHouseTableSpec(table="events", write_mode="replace")

        writer.write(df, spec, _PARAMS)

        client.command.assert_called_once()
        truncate_sql: str = client.command.call_args.args[0]
        assert "TRUNCATE" in truncate_sql.upper()
        assert "events" in truncate_sql
        client.insert.assert_called_once()


class TestDatabaseOverride:
    def test_qualified_table_name_when_database_set(self) -> None:
        """When database is set the insert uses 'database.table' form."""
        writer, client = _make_writer()
        df = pl.DataFrame({"v": [1]})
        spec = ClickHouseTableSpec(table="features", write_mode="append", database="analytics")

        writer.write(df, spec, _PARAMS)

        insert_table: str = client.insert.call_args.args[0]
        assert insert_table == "analytics.features"


class TestEmptyFrame:
    def test_empty_frame_skips_insert(self) -> None:
        """An empty DataFrame must not call insert or command."""
        writer, client = _make_writer()
        df = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
        spec = ClickHouseTableSpec(table="t", write_mode="replace")

        writer.write(df, spec, _PARAMS)

        client.command.assert_not_called()
        client.insert.assert_not_called()


class TestLazyFrameInput:
    def test_lazy_frame_is_collected_before_insert(self) -> None:
        """A LazyFrame is collected and inserted correctly."""
        writer, client = _make_writer()
        lf = pl.DataFrame({"n": [1, 2, 3]}).lazy()
        spec = ClickHouseTableSpec(table="nums", write_mode="append")

        writer.write(lf, spec, _PARAMS)

        client.insert.assert_called_once()
        assert client.insert.call_args.kwargs["column_names"] == ["n"]


class TestMissingDependency:
    def test_import_error_when_clickhouse_connect_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MissingExtraError names the extra at construction if clickhouse-connect is absent."""
        without_extra(monkeypatch, ["clickhouse_connect"], [CLICKHOUSE_ISLAND])

        with pytest.raises(MissingExtraError, match="needs the 'clickhouse' extra") as raised:
            ClickHouseTargetWriter(_URL)

        assert raised.value.extra == "clickhouse"


class _DeltaSpec:
    kind = "delta"


class TestSpecGuards:
    def test_a_spec_of_another_kind_is_refused(self) -> None:
        writer, client = _make_writer()

        with pytest.raises(TypeError, match="does not handle _DeltaSpec"):
            writer.write(pl.DataFrame({"a": [1]}), _DeltaSpec(), _PARAMS)  # type: ignore[arg-type]
        client.insert_df.assert_not_called()
