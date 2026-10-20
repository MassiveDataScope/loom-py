"""Config resolution helpers for the streaming table sink."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from loom.core.config import ConfigContext
from loom.streaming.nodes._table.common import (
    ClickHouseSinkConfig,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemyDatabaseConfig,
    SqlAlchemySinkConfig,
)

_ERR_EMPTY_NAME = "IntoTable requires a non-empty name so it can resolve streaming.sinks.<name>."


@dataclass(frozen=True, slots=True)
class ResolvedSqlAlchemyTableConfig:
    """Resolved SQLAlchemy table sink configuration."""

    sink: SqlAlchemySinkConfig
    database: SqlAlchemyDatabaseConfig


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
    """Resolve one SQLAlchemy IntoTable node against ``ConfigContext``."""
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    sink_cfg = ctx.section_or_default(sink_key, dict, {})
    _refuse_url_and_database(
        node, sink_cfg, remedy="set 'database' for a shared section or 'url' inline, not both"
    )
    effective_cfg = dict(sink_cfg)
    if node.table:
        effective_cfg["table"] = node.table
    sink = SqlAlchemySinkConfig.from_config(
        effective_cfg,
        default_table=node.table,
    )
    if not sink.database and not sink.url:
        raise ValueError(
            f"IntoTable '{node.name}': SQLAlchemy sink requires either a shared "
            "'database' reference or an inline 'url'."
        )
    if sink.database:
        database_key = f"database.{sink.database}"
        if not ctx.has(database_key):
            raise ValueError(
                f"IntoTable '{node.name}': missing shared config section at {database_key}"
            )
        database = SqlAlchemyDatabaseConfig.from_config(
            ctx.section_or_default(database_key, dict, {})
        )
    else:
        database = SqlAlchemyDatabaseConfig.from_config(effective_cfg)
    return ResolvedSqlAlchemyTableConfig(sink=sink, database=database)


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
    """Resolve one ClickHouse IntoTable node against ``ConfigContext``."""
    if not node.name:
        raise ValueError(_ERR_EMPTY_NAME)
    sink_key = f"streaming.sinks.{node.name}"
    if not ctx.has(sink_key):
        raise ValueError(f"IntoTable '{node.name}': no config section found at {sink_key}")
    sink_cfg = ctx.section_or_default(sink_key, dict, {})
    _refuse_url_and_database(
        node, sink_cfg, remedy="the url comes from database.<name>, so set 'database' only"
    )
    effective_cfg = dict(sink_cfg)
    if node.table:
        effective_cfg["table"] = node.table
    database_name = str(effective_cfg.get("database") or "").strip()
    if not database_name:
        raise ValueError(
            f"IntoTable '{node.name}': ClickHouse sink requires a 'database' reference."
        )
    database_key = f"database.{database_name}"
    if not ctx.has(database_key):
        raise ValueError(
            f"IntoTable '{node.name}': missing shared config section at {database_key}"
        )
    db_cfg = ctx.section_or_default(database_key, dict, {})
    effective_cfg["url"] = str(db_cfg.get("http_url") or db_cfg.get("url") or "").strip()
    sink = ClickHouseSinkConfig.from_config(effective_cfg, default_table=node.table)
    return ResolvedClickHouseTableConfig(sink=sink)


def _refuse_url_and_database(
    node: IntoTable[Any], sink_cfg: Mapping[str, Any], *, remedy: str
) -> None:
    """Refuse a sink section that names its connection twice.

    Raises:
        ValueError: If the section sets both ``url`` and ``database``.
    """
    if "url" in sink_cfg and "database" in sink_cfg:
        raise ValueError(
            f"IntoTable '{node.name}': the sink section sets both 'url' and 'database'; {remedy}."
        )


__all__ = [
    "ResolvedClickHouseTableConfig",
    "ResolvedDeltaTableConfig",
    "ResolvedSqlAlchemyTableConfig",
    "resolve_clickhouse_table_config",
    "resolve_delta_table_config",
    "resolve_sqlalchemy_table_config",
]
