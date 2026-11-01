"""Config resolution helpers for the streaming table sink."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from loom.core.config import ConfigContext, reject_retired_keys
from loom.core.connections.clickhouse import ClickHouseConnection
from loom.core.persistence.database import DatabaseConfig, decode_database_config
from loom.core.persistence.stores import (
    CLICKHOUSE_BACKEND,
    SQLALCHEMY_BACKEND,
    StoreCatalog,
    StoreSpec,
)
from loom.streaming.nodes._table.common import (
    SQLALCHEMY_INLINE_DATABASE_KEYS,
    ClickHouseSinkConfig,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemySinkConfig,
)

_ERR_EMPTY_NAME = "IntoTable requires a non-empty name so it can resolve streaming.sinks.<name>."
_RETIRED_STORE_KEY = "database"
"""The sink key that named a ``database.<name>`` section before ``store``."""


@dataclass(frozen=True, slots=True)
class ResolvedSqlAlchemyTableConfig:
    """Resolved SQLAlchemy table sink configuration."""

    sink: SqlAlchemySinkConfig
    database: DatabaseConfig


@dataclass(frozen=True, slots=True)
class ResolvedDeltaTableConfig:
    """Resolved Delta table sink configuration."""

    sink: DeltaSinkConfig


@dataclass(frozen=True, slots=True)
class ResolvedClickHouseTableConfig:
    """Resolved ClickHouse table sink configuration."""

    sink: ClickHouseSinkConfig


def resolve_sqlalchemy_table_config(
    node: IntoTable[Any],
    ctx: ConfigContext,
    catalog: StoreCatalog,
) -> ResolvedSqlAlchemyTableConfig:
    """Resolve one SQLAlchemy IntoTable node: a ``sqlalchemy`` store of *catalog*, or ``url``.

    Raises:
        ValueError: When the sink section is invalid or names no connection.
        ConfigError: When the store is missing or not ``sqlalchemy``, or the
            connection fails validation.
    """
    sink_key, section = _store_sink_section(
        node, ctx, remedy="set 'store' for a stores.<name> section or 'url' inline, not both"
    )
    reject_retired_keys(section, sink_key, DatabaseConfig)
    sink = SqlAlchemySinkConfig.from_config(section, default_table=node.table)
    if sink.store:
        spec = catalog.require(
            sink.store, backend=SQLALCHEMY_BACKEND, purpose=f"IntoTable {node.name!r}"
        )
        database = spec.decode(DatabaseConfig)
    elif sink.url:
        database = decode_database_config(_inline_database(section), path=sink_key)
    else:
        raise ValueError(
            f"IntoTable '{node.name}': SQLAlchemy sink requires either a 'store' "
            "reference or an inline 'url'."
        )
    return ResolvedSqlAlchemyTableConfig(sink=sink, database=database)


def _inline_database(sink_cfg: Mapping[str, Any]) -> dict[str, Any]:
    """The relational store keys of a sink that declares its connection inline."""
    return {key: sink_cfg[key] for key in SQLALCHEMY_INLINE_DATABASE_KEYS if key in sink_cfg}


def resolve_delta_table_config(
    node: IntoTable[Any],
    ctx: ConfigContext,
) -> ResolvedDeltaTableConfig:
    """Resolve one Delta IntoTable node against ``ConfigContext``.

    Raises:
        ValueError: When the sink section is missing or invalid.
    """
    _, section = _sink_section(node, ctx)
    return ResolvedDeltaTableConfig(
        sink=DeltaSinkConfig.from_config(section, default_table=node.table)
    )


def resolve_clickhouse_table_config(
    node: IntoTable[Any],
    ctx: ConfigContext,
    catalog: StoreCatalog,
) -> ResolvedClickHouseTableConfig:
    """Resolve one ClickHouse IntoTable node: the ``clickhouse`` store of *catalog* it names.

    Raises:
        ValueError: When the sink section is invalid or names no store, or
            the store's connection is not a ClickHouse one.
        ConfigError: When the store is missing or not ``clickhouse``, or the
            connection fails validation.
    """
    _, section = _store_sink_section(
        node, ctx, remedy="the url comes from stores.<name>, so set 'store' only"
    )
    store = str(section.pop("store", None) or "").strip()
    if not store:
        raise ValueError(f"IntoTable '{node.name}': ClickHouse sink requires a 'store' reference.")
    spec = catalog.require(
        store, backend=CLICKHOUSE_BACKEND, purpose=f"ClickHouse sink {node.name!r}"
    )
    sink = ClickHouseSinkConfig.from_config(
        section, default_table=node.table, connection=_clickhouse_connection(spec)
    )
    return ResolvedClickHouseTableConfig(sink=sink)


_CLICKHOUSE_SCHEMES = frozenset({"clickhouse", "clickhouses", "http", "https"})


def _clickhouse_connection(spec: StoreSpec) -> ClickHouseConnection:
    """Decode the ``clickhouse`` store *spec* as the ClickHouse connection every context shares.

    Raises:
        ValueError: When the section uses the retired ``http_url`` key or holds
            a database of another engine, naming the section.
        ConfigError: When the section holds another retired key or is not a
            valid ClickHouse connection.
    """
    raw = spec.settings()
    if "http_url" in raw:
        raise ValueError(f"{spec.path}.http_url was renamed to url; rename the key")
    url = str(raw.get("url") or "")
    scheme = url.split("://", 1)[0].lower() if "://" in url else ""
    if scheme and scheme.split("+", 1)[0] not in _CLICKHOUSE_SCHEMES:
        raise ValueError(
            f"{spec.path} holds a {scheme!r} database, but a ClickHouse sink needs a "
            "ClickHouse connection (a clickhouse:// or http(s):// url)"
        )
    return spec.decode(ClickHouseConnection)


def _sink_section(node: IntoTable[Any], ctx: ConfigContext) -> tuple[str, dict[str, Any]]:
    """Return the path of *node*'s ``streaming.sinks.<name>`` section and a copy of it.

    The node's ``table``, when set, replaces the section's.

    Raises:
        ValueError: When the node has no name or the section is missing.
    """
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    section = dict(ctx.section_or_default(sink_key, dict, {}))
    if node.table:
        section["table"] = node.table
    return sink_key, section


def _store_sink_section(
    node: IntoTable[Any], ctx: ConfigContext, *, remedy: str
) -> tuple[str, dict[str, Any]]:
    """Return :func:`_sink_section` for a sink that may name a store of the catalog.

    Raises:
        ValueError: As :func:`_sink_section`, when the section uses the
            retired ``database`` key, or sets both ``url`` and ``store``,
            naming *remedy*.
    """
    sink_key, section = _sink_section(node, ctx)
    if _RETIRED_STORE_KEY in section:
        raise ValueError(
            f"IntoTable '{node.name}': sink config key {_RETIRED_STORE_KEY!r} was renamed to "
            "'store'; "
            "name a stores.<name> section (ADR 0008)."
        )
    if "url" in section and "store" in section:
        raise ValueError(
            f"IntoTable '{node.name}': the sink section sets both 'url' and 'store'; {remedy}."
        )
    return sink_key, section


__all__ = [
    "ResolvedClickHouseTableConfig",
    "ResolvedDeltaTableConfig",
    "ResolvedSqlAlchemyTableConfig",
    "resolve_clickhouse_table_config",
    "resolve_delta_table_config",
    "resolve_sqlalchemy_table_config",
]
