"""Tests for lineage config structs and storage I/O protocols."""

from __future__ import annotations

from typing import Any

from loom.core.config.convert import convert_config
from loom.core.model.redaction import Dsn
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import SourceSpec, TableSourceSpec
from loom.etl.declarative.target._table import ReplaceSpec
from loom.etl.lineage._config import ETLObservabilityConfig, LineageConfig
from loom.etl.runtime.contracts import SourceReader, SQLExecutor, TableDiscovery, TargetWriter
from loom.etl.schema._schema import ColumnSchema, LoomDtype


def test_etl_observability_config_defaults_and_conversion() -> None:
    default = ETLObservabilityConfig()
    assert default.log.enabled is False
    assert default.otel.enabled is False
    assert default.prometheus.enabled is False
    assert default.lineage.enabled is False

    cfg = convert_config(
        {
            "log": {"enabled": True},
            "otel": {
                "enabled": True,
                "config": {
                    "service_name": "loom-etl",
                    "protocol": "grpc",
                    "endpoint": "https://collector:4317",
                    "headers": {"x-api-key": "token"},
                    "resource_attributes": {"env": "prod"},
                    "span_attributes": {"team": "data-platform"},
                    "exporter_kwargs": {"timeout": 10},
                    "span_processor_kwargs": {"max_export_batch_size": 256},
                },
            },
            "lineage": {"enabled": True, "root": "s3://lake/runs"},
        },
        ETLObservabilityConfig,
        strict=True,
    )

    assert cfg.log.enabled is True
    assert cfg.otel.enabled is True
    assert cfg.lineage.enabled is True
    assert cfg.lineage.root == "s3://lake/runs"


def test_lineage_config_validate_requires_exactly_one_destination() -> None:
    LineageConfig().validate()
    LineageConfig(enabled=True, root=Dsn("s3://lake/runs")).validate()
    LineageConfig(enabled=True, database="ops").validate()

    try:
        LineageConfig(enabled=True).validate()
    except ValueError as exc:
        assert "exactly one destination" in str(exc)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected ValueError for empty destination")

    try:
        LineageConfig(enabled=True, root=Dsn("s3://lake/runs"), database="ops").validate()
    except ValueError as exc:
        assert "exactly one destination" in str(exc)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected ValueError for conflicting destinations")


def test_protocol_method_bodies_are_callable() -> None:
    src_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
    target_spec = ReplaceSpec(table_ref=TableRef("staging.out"))
    schema = (ColumnSchema("id", LoomDtype.INT64),)

    assert _CatalogImpl().exists(TableRef("raw.orders")) is True
    assert _CatalogImpl().columns(TableRef("raw.orders")) == ("id",)
    assert _CatalogImpl().schema(TableRef("raw.orders")) is not None
    _CatalogImpl().update_schema(TableRef("raw.orders"), schema)
    assert _ReaderImpl().read(src_spec, None) == {"alias": "orders", "params": None}
    assert _ReaderImpl().execute_sql({}, "SELECT 1") == {"frames": {}, "query": "SELECT 1"}
    _WriterImpl().write(object(), target_spec, None)


class _CatalogImpl:
    def exists(self, ref: TableRef) -> bool:
        return ref.ref == "raw.orders"

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        return ("id",)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        return (ColumnSchema("id", LoomDtype.INT64),)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        _ = (ref, schema)


class _ReaderImpl:
    def read(self, spec: SourceSpec, params_instance: Any, /) -> Any:
        return {"alias": spec.alias, "params": params_instance}

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        return {"frames": frames, "query": query}


class _WriterImpl:
    def write(
        self, frame: Any, spec: object, params_instance: Any, /, *, streaming: bool = False
    ) -> None:
        _ = (frame, spec, params_instance, streaming)


def test_runtime_protocol_checks_for_concrete_impls() -> None:
    assert isinstance(_CatalogImpl(), TableDiscovery)
    assert isinstance(_ReaderImpl(), SourceReader)
    assert isinstance(_ReaderImpl(), SQLExecutor)
    assert isinstance(_WriterImpl(), TargetWriter)
