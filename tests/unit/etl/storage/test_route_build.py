"""Unit tests for building table route resolvers from StorageConfig."""

from __future__ import annotations

import pytest

from loom.core.model.redaction import Dsn
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.schema import ColumnSchema, LoomDtype
from loom.etl.storage._config import (
    CatalogConnection,
    StorageConfig,
    StorageDefaults,
    TablePathConfig,
    TableRoute,
)
from loom.etl.storage.routing import CatalogTarget, PathTarget, RoutedCatalog, build_table_resolver


def test_build_table_resolver_qualifies_two_part_override_with_route_catalog() -> None:
    resolver = build_table_resolver(
        StorageConfig(
            engine="spark",
            catalogs={"unity": CatalogConnection()},
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
        )
    )
    target = resolver.resolve(TableRef("raw.orders"))

    assert isinstance(target, CatalogTarget)
    assert target.catalog_ref.ref == "unity.raw.orders"


def test_build_table_resolver_uses_default_catalog_for_two_part_refs() -> None:
    resolver = build_table_resolver(
        StorageConfig(engine="spark", catalogs={"default": CatalogConnection()})
    )
    target = resolver.resolve(TableRef("staging.out"))

    assert isinstance(target, CatalogTarget)
    assert target.catalog_ref.ref == "default.staging.out"


def test_path_override_resolves_to_physical_location() -> None:
    config = StorageConfig(
        engine="spark",
        tables=(TableRoute(name="raw.orders", path=TablePathConfig(uri=Dsn("s3://raw/orders"))),),
    )
    target = build_table_resolver(config).resolve(TableRef("raw.orders"))

    assert isinstance(target, PathTarget)
    assert target.logical_ref == TableRef("raw.orders")
    assert target.location.uri == "s3://raw/orders"


def test_missing_polars_route_raises_actionable_key_error() -> None:
    resolver = build_table_resolver(StorageConfig())

    table_ref = TableRef("raw.missing")
    with pytest.raises(KeyError, match="storage.defaults.table_path"):
        resolver.resolve(table_ref)


def test_composite_resolver_prefers_override_over_default_path() -> None:
    config = StorageConfig(
        defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn("s3://lake"))),
        tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),),
    )
    resolver = build_table_resolver(config)

    override = resolver.resolve(TableRef("raw.orders"))
    fallback = resolver.resolve(TableRef("raw.customers"))

    assert isinstance(override, CatalogTarget)
    assert override.catalog_ref.ref == "main.raw.orders"
    assert isinstance(fallback, PathTarget)
    assert fallback.location.uri == "s3://lake/raw/customers"


class _SpyCatalog:
    def __init__(self) -> None:
        self.refs: list[TableRef] = []
        self.updated: tuple[TableRef, tuple[ColumnSchema, ...]] | None = None

    def exists(self, ref: TableRef) -> bool:
        self.refs.append(ref)
        return ref.ref.endswith("orders")

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        self.refs.append(ref)
        return ("id", "amount")

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        self.refs.append(ref)
        return (ColumnSchema("id", LoomDtype.INT64),)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        self.updated = (ref, schema)


def test_routed_catalog_dispatches_catalog_targets_to_catalog_ref() -> None:
    routed = RoutedCatalog(
        build_table_resolver(
            StorageConfig(engine="spark", catalogs={"default": CatalogConnection()})
        ),
        catalog=_SpyCatalog(),
    )
    schema = (ColumnSchema("id", LoomDtype.INT64),)

    assert routed.exists(TableRef("raw.orders")) is True
    assert routed.columns(TableRef("raw.orders")) == ("id", "amount")
    assert routed.schema(TableRef("raw.orders")) == schema
    routed.update_schema(TableRef("raw.orders"), schema)

    catalog = routed._catalog
    assert isinstance(catalog, _SpyCatalog)
    assert catalog.refs == [
        TableRef("default.raw.orders"),
        TableRef("default.raw.orders"),
        TableRef("default.raw.orders"),
    ]
    assert catalog.updated == (TableRef("default.raw.orders"), schema)


def test_routed_catalog_requires_path_catalog_for_path_targets() -> None:
    routed = RoutedCatalog(
        build_table_resolver(
            StorageConfig(
                defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn("s3://lake")))
            )
        ),
        catalog=_SpyCatalog(),
    )

    table_ref = TableRef("raw.orders")
    with pytest.raises(RuntimeError, match="path mode"):
        routed.exists(table_ref)


def test_routed_catalog_dispatches_path_targets_to_path_catalog() -> None:
    path_catalog = _SpyCatalog()
    routed = RoutedCatalog(
        build_table_resolver(
            StorageConfig(
                defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn("s3://lake")))
            )
        ),
        catalog=_SpyCatalog(),
        path=path_catalog,
    )

    assert routed.exists(TableRef("raw.orders")) is True
    assert path_catalog.refs == [TableRef("raw.orders")]
