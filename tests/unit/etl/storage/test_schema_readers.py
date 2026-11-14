"""Unit tests for physical schema readers."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from loom.etl.backends.polars._schema import (
    PolarsPhysicalSchema,
    read_delta_physical_schema,
)


class TestDeltaPhysicalSchema:
    def test_read_delta_physical_schema_returns_none_for_missing_table(
        self, tmp_path: Path
    ) -> None:
        schema = read_delta_physical_schema(str(tmp_path / "missing"))

        assert schema is None

    def test_read_delta_physical_schema_reads_columns_and_partitions(self, tmp_path: Path) -> None:
        table_path = tmp_path / "staging" / "orders"
        table_path.mkdir(parents=True, exist_ok=True)
        write_deltalake(
            str(table_path),
            pl.DataFrame({"id": [1, 2], "year": [2024, 2024], "amount": [10.0, 20.0]}),
            mode="overwrite",
            partition_by=["year"],
        )

        schema = read_delta_physical_schema(str(table_path))

        assert schema is not None
        assert isinstance(schema, PolarsPhysicalSchema)
        assert tuple(schema.schema.names()) == ("id", "year", "amount")
        assert schema.schema["id"] == pl.Int64
        assert schema.schema["year"] == pl.Int64
        assert schema.schema["amount"] == pl.Float64
        assert schema.partition_columns == ("year",)

    def test_read_delta_physical_schema_passes_storage_options(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[str, dict[str, str] | None]] = []

        class _FakeSchema:
            def json(self) -> dict[str, object]:
                return {
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    ],
                }

        class _FakeMetadata:
            partition_columns = ["year"]

        class _FakeDeltaTable:
            def __init__(self, uri: str, storage_options: dict[str, str] | None = None) -> None:
                calls.append((uri, storage_options))

            def schema(self) -> _FakeSchema:
                return _FakeSchema()

            def metadata(self) -> _FakeMetadata:
                return _FakeMetadata()

        monkeypatch.setattr("loom.etl.backends.polars._schema.DeltaTable", _FakeDeltaTable)

        storage_options = {
            "databricks_workspace_url": "https://dbc.example",
            "databricks_access_token": "token-123",
        }
        schema = read_delta_physical_schema("uc://main.raw.orders", storage_options)

        assert schema is not None
        assert isinstance(schema, PolarsPhysicalSchema)
        assert tuple(schema.schema.names()) == ("id",)
        assert schema.partition_columns == ("year",)
        assert calls == [
            (
                "uc://main.raw.orders",
                {
                    "databricks_workspace_url": "https://dbc.example",
                    "databricks_access_token": "token-123",
                },
            )
        ]
