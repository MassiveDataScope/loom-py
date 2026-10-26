"""Routing primitives for logical table references.

This module centralizes:

1. resolved target models (catalog/path),
2. route resolver implementations,
3. resolver factory from ``StorageConfig``,
4. route-aware catalog adapter used at compile-time validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from loom.core.model.redaction import reveal_for_driver
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.runtime.contracts import TableDiscovery
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage._config import StorageConfig
from loom.etl.storage._locator import PrefixLocator, TableLocation, TableLocator


@dataclass(frozen=True)
class CatalogTarget:
    """Resolved catalog destination for one logical table."""

    logical_ref: TableRef
    catalog_ref: TableRef


@dataclass(frozen=True)
class PathTarget:
    """Resolved physical destination for one logical table."""

    logical_ref: TableRef
    location: TableLocation


ResolvedTarget = CatalogTarget | PathTarget


@runtime_checkable
class TableRouteResolver(Protocol):
    """Resolve one logical table reference to a runtime target."""

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        """Resolve *logical_ref* to catalog or path destination."""
        ...


class CatalogRouteResolver:
    """Resolve all tables as catalog references."""

    def __init__(self, default_catalog: str = "") -> None:
        self._default_catalog = default_catalog.strip()

    def resolve(self, logical_ref: TableRef) -> CatalogTarget:
        return CatalogTarget(
            logical_ref=logical_ref,
            catalog_ref=logical_ref.qualify(self._default_catalog),
        )


class PathRouteResolver:
    """Resolve all tables through one table locator."""

    def __init__(self, locator: TableLocator) -> None:
        self._locator = locator

    def resolve(self, logical_ref: TableRef) -> PathTarget:
        return PathTarget(logical_ref=logical_ref, location=self._locator.locate(logical_ref))


class FixedCatalogRouteResolver:
    """Resolve one logical table to one explicit catalog table."""

    def __init__(self, catalog_ref: TableRef) -> None:
        self._catalog_ref = catalog_ref

    def resolve(self, logical_ref: TableRef) -> CatalogTarget:
        return CatalogTarget(logical_ref=logical_ref, catalog_ref=self._catalog_ref)


class FixedPathRouteResolver:
    """Resolve one logical table to one explicit physical location."""

    def __init__(self, location: TableLocation) -> None:
        self._location = location

    def resolve(self, logical_ref: TableRef) -> PathTarget:
        return PathTarget(logical_ref=logical_ref, location=self._location)


class CompositeRouteResolver:
    """Resolve using table-specific overrides over one default resolver."""

    def __init__(
        self,
        *,
        default: TableRouteResolver,
        overrides: dict[str, TableRouteResolver] | None = None,
    ) -> None:
        self._default = default
        self._overrides = overrides or {}

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        resolver = self._overrides.get(logical_ref.ref, self._default)
        return resolver.resolve(logical_ref)


@dataclass(frozen=True)
class _MissingRouteResolver:
    """Resolver that always fails for unconfigured logical refs."""

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        raise KeyError(
            f"No storage route configured for logical table {logical_ref.ref!r}. "
            "Define storage.defaults.table_path or add an explicit storage.tables entry."
        )


def build_table_resolver(config: StorageConfig) -> TableRouteResolver:
    """Build route resolver from ``StorageConfig`` defaults + table overrides."""
    default_resolver = _default_resolver(config)
    overrides: dict[str, TableRouteResolver] = {}
    for route in config.tables:
        if route.path is not None:
            overrides[route.name] = FixedPathRouteResolver(route.path.to_location())
        elif route.ref.strip():
            catalog_ref = _qualify_route_ref(route.ref, route.catalog)
            overrides[route.name] = FixedCatalogRouteResolver(TableRef(catalog_ref))

    if not overrides:
        return default_resolver
    return CompositeRouteResolver(default=default_resolver, overrides=overrides)


class RoutedCatalog:
    """Dispatch ``TableDiscovery`` calls by table route (catalog vs path)."""

    def __init__(
        self,
        resolver: TableRouteResolver,
        *,
        catalog: TableDiscovery,
        path: TableDiscovery | None = None,
    ) -> None:
        self._resolver = resolver
        self._catalog = catalog
        self._path = path

    def exists(self, ref: TableRef) -> bool:
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.exists(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.exists(ref)  # type: ignore[union-attr]

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.columns(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.columns(ref)  # type: ignore[union-attr]

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.schema(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.schema(ref)  # type: ignore[union-attr]

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            self._catalog.update_schema(target.catalog_ref, schema)
            return
        self._require_path_catalog(ref)
        self._path.update_schema(ref, schema)  # type: ignore[union-attr]

    def _require_path_catalog(self, ref: TableRef) -> None:
        if self._path is None:
            raise RuntimeError(
                f"Table {ref.ref!r} resolved to path mode but no path catalog is configured."
            )


def _default_resolver(config: StorageConfig) -> TableRouteResolver:
    default_path = config.defaults.table_path
    if default_path is not None:
        return PathRouteResolver(
            PrefixLocator(
                root="" if default_path.uri is None else default_path.uri.reveal(),
                storage_options=reveal_for_driver(default_path.storage_options) or None,
                writer=default_path.writer or None,
                delta_config=default_path.delta_config or None,
                commit=default_path.commit or None,
            )
        )
    if config.engine == "spark":
        return CatalogRouteResolver(default_catalog=_default_catalog_key(config))
    return _MissingRouteResolver()


def _default_catalog_key(config: StorageConfig) -> str:
    return "default" if "default" in config.catalogs else ""


def _qualify_route_ref(ref: str, catalog_key: str) -> str:
    if not catalog_key:
        return ref
    parts = ref.split(".")
    if len(parts) == 2:
        return f"{catalog_key}.{ref}"
    return ref


__all__ = [
    "CatalogTarget",
    "PathTarget",
    "ResolvedTarget",
    "TableRouteResolver",
    "CatalogRouteResolver",
    "PathRouteResolver",
    "FixedCatalogRouteResolver",
    "FixedPathRouteResolver",
    "CompositeRouteResolver",
    "build_table_resolver",
    "RoutedCatalog",
]
