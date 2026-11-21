"""Tests for TableDiscovery, SourceReader, TargetWriter protocols and stubs."""

from __future__ import annotations

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._table import ReplaceSpec
from loom.etl.ports.contracts import SourceReader, SQLExecutor, TableDiscovery, TargetWriter
from loom.etl.testing import InMemoryCatalog, InMemorySourceReader, InMemoryTargetWriter

_SENTINEL = object()


def _make_source_spec(alias: str) -> TableSourceSpec:
    return TableSourceSpec(alias=alias, table_ref=TableRef(f"raw.{alias}"))


def _make_target_spec() -> TargetSpec:
    return ReplaceSpec(table_ref=TableRef("staging.out"))


class TestInMemoryCatalog:
    @pytest.mark.parametrize(
        "tables,ref,expected",
        [
            ({"raw.orders": ("id", "amount")}, "raw.orders", True),
            ({"raw.orders": ("id",)}, "raw.missing", False),
            ({}, "raw.orders", False),
        ],
    )
    def test_stub_catalog_exists(self, tables: dict, ref: str, expected: bool) -> None:
        assert InMemoryCatalog(tables).exists(TableRef(ref)) is expected

    def test_stub_catalog_table_no_columns(self) -> None:
        catalog = InMemoryCatalog({"staging.out": ()})
        assert catalog.exists(TableRef("staging.out")) is True
        assert catalog.columns(TableRef("staging.out")) == ()

    @pytest.mark.parametrize(
        "tables,ref,expected",
        [
            ({"raw.orders": ("id", "amount", "year")}, "raw.orders", ("id", "amount", "year")),
            ({"raw.orders": ("id",)}, "raw.missing", ()),
        ],
    )
    def test_stub_catalog_columns(self, tables: dict, ref: str, expected: tuple) -> None:
        assert InMemoryCatalog(tables).columns(TableRef(ref)) == expected

    def test_stub_catalog_satisfies_protocol(self) -> None:
        assert isinstance(InMemoryCatalog(), TableDiscovery)


class TestInMemorySourceReader:
    @pytest.mark.parametrize(
        "seeds,alias,expected",
        [
            ({"orders": _SENTINEL}, "orders", _SENTINEL),
            ({"orders": object()}, "customers", None),
            ({}, "orders", None),
        ],
    )
    def test_stub_source_reader_read(self, seeds: dict, alias: str, expected: object) -> None:
        reader = InMemorySourceReader(seeds)
        result = reader.read(_make_source_spec(alias), None)
        assert result is expected

    def test_stub_source_reader_satisfies_protocol(self) -> None:
        assert isinstance(InMemorySourceReader(), SourceReader)


class TestInMemoryTargetWriter:
    @pytest.mark.parametrize("n_writes", [0, 1, 3])
    def test_stub_target_writer_captures_writes_in_order(self, n_writes: int) -> None:
        writer = InMemoryTargetWriter()
        frames = [object() for _ in range(n_writes)]
        spec = _make_target_spec()
        for f in frames:
            writer.write(f, spec, _params_instance=None)
        assert [w[0] for w in writer.written] == frames

    def test_stub_target_writer_satisfies_protocol(self) -> None:
        assert isinstance(InMemoryTargetWriter(), TargetWriter)


class TestProtocolStubBodies:
    def test_protocol_stub_bodies_execute_without_errors(self) -> None:
        spec = _make_source_spec("orders")
        target = _make_target_spec()
        ref = TableRef("raw.orders")

        assert TableDiscovery.exists(object(), ref) is None
        assert TableDiscovery.columns(object(), ref) is None
        assert TableDiscovery.schema(object(), ref) is None
        assert TableDiscovery.update_schema(object(), ref, ()) is None
        assert SourceReader.read(object(), spec, None) is None
        assert SQLExecutor.execute_sql(object(), {}, "SELECT 1") is None
        assert TargetWriter.write(object(), object(), target, None) is None
