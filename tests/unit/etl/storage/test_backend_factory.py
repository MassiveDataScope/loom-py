"""Unit tests for backend factories and observability observer wiring."""

from __future__ import annotations

import importlib
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.helpers.version_limited_extras import require_pyspark

require_pyspark()

from loom.core.connections import ClickHouseConnection  # noqa: E402
from loom.core.model.redaction import Dsn  # noqa: E402
from loom.etl.backends.spark.provider import SparkProvider  # noqa: E402
from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend  # noqa: E402
from loom.etl.lineage._config import LineageConfig  # noqa: E402
from loom.etl.lineage._records import EventName, PipelineRunRecord, RunStatus  # noqa: E402
from loom.etl.lineage.sinks import TableLineageStore  # noqa: E402
from loom.etl.runner._providers import load_backend_provider  # noqa: E402
from loom.etl.runner._wiring import (  # noqa: E402
    make_backends,
    make_checkpoint_store,
    make_client_executor,
    make_lineage_store,
    make_lineage_writer,
)
from loom.etl.storage._config import (  # noqa: E402
    CatalogConnection,
    MissingTablePolicy,
    StorageConfig,
    StorageDefaults,
    TablePathConfig,
    TableRoute,
    TempConfig,
)


def _path_defaults(root: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=root))


class _DummyTargetWriter:
    def __init__(self) -> None:
        self.to_frame_calls: list[list[object]] = []
        self.append_calls: list[tuple[object, object, object]] = []

    def to_frame(self, records: list[object], /) -> object:
        self.to_frame_calls.append(records)
        return {"rows": records}

    def append(self, frame: object, table_ref: object, params_instance: object, /) -> None:
        self.append_calls.append((frame, table_ref, params_instance))


# ---------------------------------------------------------------------------
# make_backends
# ---------------------------------------------------------------------------


def test_make_backends_polars_path_defaults_returns_polars_types(tmp_path: Path) -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(defaults=_path_defaults(str(tmp_path / "test-lake")))
    reader, writer = make_backends(config)

    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_spark_without_session_raises() -> None:
    config = StorageConfig(engine="spark")
    with pytest.raises(ValueError, match="SparkSession"):
        make_backends(config, spark=None)


def test_make_backends_prefers_spark_when_session_is_provided() -> None:
    from unittest.mock import MagicMock

    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

    spark = MagicMock()
    config = StorageConfig(engine="polars")
    reader, writer = make_backends(config, spark=spark)

    assert isinstance(reader, SparkSourceReader)
    assert isinstance(writer, SparkTargetWriter)


def test_make_backends_polars_catalog_route_builds_backends() -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
    )
    reader, writer = make_backends(config)

    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_polars_mixed_routes_builds_backends(tmp_path: Path) -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(
        defaults=_path_defaults(str(tmp_path / "lake")),
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(
            TableRoute(
                name="raw.path_orders",
                path=TablePathConfig(uri=str(tmp_path / "explicit" / "orders")),
            ),
            TableRoute(name="raw.uc_orders", ref="raw.orders", catalog="unity"),
        ),
    )

    reader, writer = make_backends(config)
    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_polars_two_part_uc_ref_requires_catalog_key() -> None:
    config = StorageConfig(
        tables=(TableRoute(name="raw.orders", ref="raw.orders"),),
    )
    with pytest.raises(ValueError, match="2-part refs require route.catalog"):
        make_backends(config)


def test_make_backends_spark_catalog_route_builds_backends() -> None:
    from unittest.mock import MagicMock

    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

    spark = MagicMock()
    config = StorageConfig(
        engine="spark",
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
    )
    reader, writer = make_backends(config, spark=spark)
    assert isinstance(reader, SparkSourceReader)
    assert isinstance(writer, SparkTargetWriter)


def test_load_backend_provider_resolves_registered_providers() -> None:
    assert type(load_backend_provider("polars")).__name__ == "PolarsProvider"
    assert type(load_backend_provider("spark")).__name__ == "SparkProvider"


def test_load_backend_provider_rejects_unknown_engine() -> None:
    with pytest.raises(ValueError, match="No backend provider registered"):
        load_backend_provider("duckdb")


def test_load_backend_provider_names_the_registered_engines_when_rejecting() -> None:
    """The failure must tell the operator which engines it could have asked for."""
    with pytest.raises(ValueError) as excinfo:
        load_backend_provider("duckdb")

    message = str(excinfo.value)
    assert "polars" in message
    assert "spark" in message


# ---------------------------------------------------------------------------
# lineage wiring
# ---------------------------------------------------------------------------


def test_make_lineage_writer_rejects_database_destination_for_polars_engine() -> None:
    lineage = LineageConfig(enabled=True, database="ops")
    config = StorageConfig(engine="polars")
    with pytest.raises(ValueError, match="observability.lineage.database"):
        make_lineage_writer(config, lineage)


def test_make_lineage_writer_polars_root_returns_writer() -> None:
    lineage = LineageConfig(enabled=True, root="s3://bucket/runs")
    writer = make_lineage_writer(StorageConfig(engine="polars"), lineage)
    assert writer is not None
    assert type(writer).__name__ == "TargetLineageWriter"


def test_make_lineage_writer_spark_database_requires_session() -> None:
    lineage = LineageConfig(enabled=True, database="ops")
    config = StorageConfig(engine="spark")
    with pytest.raises(ValueError, match="SparkSession"):
        make_lineage_writer(config, lineage, spark=None)


def test_spark_provider_lineage_writer_root_returns_target_writer_wrapper() -> None:
    from unittest.mock import MagicMock

    spark = MagicMock()
    provider = SparkProvider()
    store = LineageConfig(enabled=True, root="s3://bucket/runs")

    writer = provider.create_lineage_writer(
        StorageConfig(engine="spark"),
        store,
        spark=spark,
    )
    assert type(writer).__name__ == "TargetLineageWriter"


def test_spark_provider_lineage_writer_honors_missing_table_policy() -> None:
    from unittest.mock import MagicMock

    captured: dict[str, MissingTablePolicy] = {}

    class _FakeSparkTargetWriter:
        def __init__(
            self,
            spark: object,
            locator: object | None,
            *,
            route_resolver: object | None = None,
            missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
            file_locator: object | None = None,
        ) -> None:
            _ = (spark, locator, route_resolver, file_locator)
            captured["policy"] = missing_table_policy

    spark = MagicMock()
    provider = SparkProvider()
    store = LineageConfig(enabled=True, root="s3://bucket/runs")
    config = StorageConfig(engine="spark", missing_table_policy=MissingTablePolicy.CREATE)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "loom.etl.backends.spark.provider.SparkTargetWriter", _FakeSparkTargetWriter
    )
    try:
        writer = provider.create_lineage_writer(config, store, spark=spark)
    finally:
        monkeypatch.undo()

    assert type(writer).__name__ == "TargetLineageWriter"
    assert captured["policy"] is MissingTablePolicy.CREATE


def test_polars_provider_lineage_writer_honors_missing_table_policy() -> None:
    from loom.etl.backends.polars.provider import PolarsProvider

    captured: dict[str, MissingTablePolicy] = {}

    class _FakePolarsTargetWriter:
        def __init__(
            self,
            locator: object,
            *,
            missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
            file_locator: object | None = None,
        ) -> None:
            _ = (locator, file_locator)
            captured["policy"] = missing_table_policy

    provider = PolarsProvider()
    store = LineageConfig(enabled=True, root="s3://bucket/runs")
    config = StorageConfig(engine="polars", missing_table_policy=MissingTablePolicy.CREATE)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "loom.etl.backends.polars.provider.PolarsTargetWriter", _FakePolarsTargetWriter
    )
    try:
        writer = provider.create_lineage_writer(config, store)
    finally:
        monkeypatch.undo()

    assert type(writer).__name__ == "TargetLineageWriter"
    assert captured["policy"] is MissingTablePolicy.CREATE


def test_target_lineage_writer_direct_module_import_executes_frame_and_append() -> None:
    from loom.etl.declarative.expr._refs import TableRef

    writer_module = importlib.reload(importlib.import_module("loom.etl.lineage.sinks._writer"))
    target_writer = _DummyTargetWriter()
    writer = writer_module.TargetLineageWriter(target_writer)

    record = PipelineRunRecord(
        event=EventName.PIPELINE_END,
        run_id="run-1",
        correlation_id=None,
        attempt=1,
        pipeline="DailyPipeline",
        started_at=datetime.now(UTC),
        status=RunStatus.SUCCESS,
        duration_ms=1,
        error=None,
    )

    writer.write_record(record, TableRef("ops.pipeline_runs"))
    assert len(target_writer.to_frame_calls) == 1
    assert len(target_writer.append_calls) == 1


def test_make_lineage_store_returns_table_store_when_enabled() -> None:
    lineage = LineageConfig(enabled=True, root="s3://bucket/runs")
    store = make_lineage_store(StorageConfig(engine="polars"), lineage)
    assert isinstance(store, TableLineageStore)


# ---------------------------------------------------------------------------
# make_client_executor
# ---------------------------------------------------------------------------


def test_make_client_executor_polars_with_clickhouse_url_returns_executor() -> None:
    from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor

    config = StorageConfig(
        engine="polars",
        clickhouse=ClickHouseConnection(url=Dsn("clickhouse://user:pass@host:8123/db")),
    )
    executor = make_client_executor(config)

    assert isinstance(executor, ClickHouseClientExecutor)


def test_make_client_executor_polars_without_clickhouse_url_returns_none() -> None:
    config = StorageConfig(engine="polars")
    assert make_client_executor(config) is None


def test_make_client_executor_spark_returns_none() -> None:
    from unittest.mock import MagicMock

    spark = MagicMock()
    config = StorageConfig(
        engine="spark",
        clickhouse=ClickHouseConnection(url=Dsn("clickhouse://user:pass@host:8123/db")),
    )
    assert make_client_executor(config, spark=spark) is None


# ---------------------------------------------------------------------------
# make_checkpoint_store
# ---------------------------------------------------------------------------


def test_make_checkpoint_store_no_root_returns_none() -> None:
    config = StorageConfig()
    result = make_checkpoint_store(config)

    assert result is None


def test_make_checkpoint_store_with_root_returns_store() -> None:
    from loom.etl.checkpoint import CheckpointStore

    config = StorageConfig(temp=TempConfig(root="s3://bucket/checkpoints"))
    result = make_checkpoint_store(config)

    assert isinstance(result, CheckpointStore)


def test_make_checkpoint_store_uses_the_polars_backend_for_temp_storage_options() -> None:
    config = StorageConfig(
        temp=TempConfig(
            root="s3://bucket/checkpoints",
            storage_options={
                "aws_server_side_encryption": "aws:kms",
                "aws_sse_kms_key_id": "alias/loom-temp",
            },
        )
    )

    result = make_checkpoint_store(config)

    assert result is not None
    assert isinstance(result._backend, _PolarsCheckpointBackend)


def test_make_checkpoint_store_warns_encryption_is_lost_on_spark(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from unittest.mock import MagicMock

    config = StorageConfig(
        engine="spark",
        temp=TempConfig(
            root="s3://bucket/checkpoints",
            storage_options={"aws_server_side_encryption": "aws:kms"},
        ),
    )

    with caplog.at_level(logging.WARNING, logger="loom.etl.runner._wiring"):
        make_checkpoint_store(config, spark=MagicMock())

    assert any("ignored by the Spark backend" in r.message for r in caplog.records)
