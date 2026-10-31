"""Config resolution helpers for the streaming table sink."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from loom.core.config import ConfigContext, ConfigError, ConfigKey, reject_retired_keys
from loom.core.connections.clickhouse import ClickHouseConnection
from loom.core.persistence.database import (
    DatabaseConfig,
    database_config_of,
    decode_database_config,
    reject_retired_pool_keys,
)
from loom.core.persistence.stores import StoreKey, StoreSpec, load_catalog
from loom.streaming.nodes._table.common import (
    RETIRED_STORE_REFERENCE,
    SQLALCHEMY_INLINE_DATABASE_KEYS,
    ClickHouseSinkConfig,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemySinkConfig,
)

_CLICKHOUSE_BACKEND = "clickhouse"
_ERR_EMPTY_NAME = "IntoTable requires a non-empty name so it can resolve streaming.sinks.<name>."


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
) -> ResolvedSqlAlchemyTableConfig:
    """Resolve one SQLAlchemy IntoTable node against ``ConfigContext``.

    The sink names a store of the catalog (``store: <name>``), whose backend
    must be ``sqlalchemy``, or declares its connection inline (``url``).

    Raises:
        ValueError: When the node has no name, the sink section is missing,
            names its connection twice or not at all, or names a store the
            catalog does not declare.
        ConfigError: When the catalog does not load, the store's backend is
            not ``sqlalchemy``, or the connection fails validation.
    """
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    sink_cfg = ctx.section_or_default(sink_key, dict, {})
    _refuse_url_and_store(
        node, sink_cfg, remedy="set 'store' for a stores.<name> section or 'url' inline, not both"
    )
    effective_cfg = dict(sink_cfg)
    if node.table:
        effective_cfg["table"] = node.table
    reject_retired_pool_keys(effective_cfg, sink_key)
    sink = SqlAlchemySinkConfig.from_config(
        effective_cfg,
        default_table=node.table,
    )
    if not sink.store and not sink.url:
        raise ValueError(
            f"IntoTable '{node.name}': SQLAlchemy sink requires either a 'store' "
            "reference or an inline 'url'."
        )
    if sink.store:
        spec = load_catalog(ctx).specs.get(StoreKey(sink.store))
        if spec is None:
            raise ValueError(
                f"IntoTable '{node.name}': store {sink.store!r} is not declared under "
                f"{ConfigKey.STORES}.{sink.store}"
            )
        database = database_config_of(spec)
    else:
        database = decode_database_config(_inline_database(effective_cfg), path=sink_key)
    return ResolvedSqlAlchemyTableConfig(sink=sink, database=database)


def _inline_database(sink_cfg: Mapping[str, Any]) -> dict[str, Any]:
    """The relational store keys of a sink that declares its connection inline."""
    return {key: sink_cfg[key] for key in SQLALCHEMY_INLINE_DATABASE_KEYS if key in sink_cfg}


def resolve_delta_table_config(
    node: IntoTable[Any],
    ctx: ConfigContext,
) -> ResolvedDeltaTableConfig:
    """Resolve one Delta IntoTable node against ``ConfigContext``."""
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    sink_cfg = ctx.section_or_default(sink_key, dict, {})
    effective_cfg = dict(sink_cfg)
    if node.table:
        effective_cfg["table"] = node.table
    sink = DeltaSinkConfig.from_config(
        effective_cfg,
        default_table=node.table,
    )
    return ResolvedDeltaTableConfig(sink=sink)


def resolve_clickhouse_table_config(
    node: IntoTable[Any],
    ctx: ConfigContext,
) -> ResolvedClickHouseTableConfig:
    """Resolve one ClickHouse IntoTable node against ``ConfigContext``.

    The sink names a ``clickhouse`` store of the catalog (``store: <name>``),
    whose section is the connection.

    Raises:
        ValueError: When the node has no name, the sink section is missing,
            uses the retired ``database`` key, sets ``url`` too, names no
            store or a store the catalog does not declare, or the store uses
            the retired ``http_url`` or holds a database of another engine.
        ConfigError: When the catalog does not load, the store's backend is
            not ``clickhouse``, or the connection fails validation.
    """
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    sink_cfg = ctx.section_or_default(sink_key, dict, {})
    if RETIRED_STORE_REFERENCE in sink_cfg:
        raise ValueError(
            f"IntoTable '{node.name}': ClickHouse sink config key 'database' was renamed "
            "to 'store'; name a stores.<name> section with backend: clickhouse (ADR 0008)."
        )
    _refuse_url_and_store(
        node, sink_cfg, remedy="the url comes from stores.<name>, so set 'store' only"
    )
    effective_cfg = dict(sink_cfg)
    if node.table:
        effective_cfg["table"] = node.table
    store = str(effective_cfg.pop("store", None) or "").strip()
    if not store:
        raise ValueError(f"IntoTable '{node.name}': ClickHouse sink requires a 'store' reference.")
    spec = load_catalog(ctx).specs.get(StoreKey(store))
    if spec is None:
        raise ValueError(
            f"IntoTable '{node.name}': store {store!r} is not declared under "
            f"{ConfigKey.STORES}.{store}"
        )
    sink = ClickHouseSinkConfig.from_config(
        effective_cfg, default_table=node.table, connection=_clickhouse_connection(spec)
    )
    return ResolvedClickHouseTableConfig(sink=sink)


_CLICKHOUSE_SCHEMES = frozenset({"clickhouse", "clickhouses", "http", "https"})


def _clickhouse_connection(spec: StoreSpec) -> ClickHouseConnection:
    """Decode the ``clickhouse`` store *spec* as the ClickHouse connection every context shares.

    Raises:
        ValueError: When the section uses the retired ``http_url`` key or holds
            a database of another engine, naming the section.
        ConfigError: When the store's backend is not ``clickhouse``, the
            section holds another retired key or is not a valid ClickHouse
            connection.
    """
    if spec.backend != _CLICKHOUSE_BACKEND:
        raise ConfigError(
            f"{spec.path}.backend is {spec.backend!r}, but a ClickHouse sink needs "
            f"backend: {_CLICKHOUSE_BACKEND}"
        )
    raw = spec.settings()
    reject_retired_keys(raw, spec.path, ClickHouseConnection)
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


def _refuse_url_and_store(
    node: IntoTable[Any], sink_cfg: Mapping[str, Any], *, remedy: str
) -> None:
    """Refuse a sink section that names its connection twice.

    Raises:
        ValueError: If the section sets both ``url`` and ``store``.
    """
    if "url" in sink_cfg and "store" in sink_cfg:
        raise ValueError(
            f"IntoTable '{node.name}': the sink section sets both 'url' and 'store'; {remedy}."
        )


__all__ = [
    "ResolvedClickHouseTableConfig",
    "ResolvedDeltaTableConfig",
    "ResolvedSqlAlchemyTableConfig",
    "resolve_clickhouse_table_config",
    "resolve_delta_table_config",
    "resolve_sqlalchemy_table_config",
]
